package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/config"
	"github.com/nguyenbatam/arena_game_server/internal/metrics"
	"github.com/nguyenbatam/arena_game_server/internal/net/httputil"
	"github.com/nguyenbatam/arena_game_server/internal/platform"
	"github.com/nguyenbatam/arena_game_server/internal/safe"
	"github.com/nguyenbatam/arena_game_server/internal/turn"
)

// The platform tier's HTTP surface.
//
// It lives in the gateway process and on the same mux as /ws, which is a demo's
// convenience and not the shape a product would ship: accounts, a store and a
// leaderboard scale on request rate and a game server scales on tick budget,
// and pinning them to one deployment means scaling whichever is cheaper by
// whichever is dearer. The seam that makes splitting them a small job is
// already drawn — platform.Service knows nothing about HTTP, sessions or rooms,
// and everything below is a handler calling one of its methods.
//
// What must not move is the pair of calls the realtime tier makes directly:
// the rating read on a queue join and the match write on a room ending. Those
// are not requests a player made and they must not become network hops that a
// player's match waits on.

// mountPlatform registers the platform routes. Nothing is mounted when the
// tier is off, so a request to /platform/... is a 404 rather than a handler
// explaining that a feature is disabled — the endpoints genuinely do not exist
// in that build of the process.
func (a *App) mountPlatform(mux *http.ServeMux) {
	if a.platform == nil {
		return
	}
	mux.HandleFunc("/auth/register", a.handleRegister)
	mux.HandleFunc("/platform/profile", a.handleProfile)
	mux.HandleFunc("/platform/inventory", a.handleInventory)
	mux.HandleFunc("/platform/catalog", a.handleCatalog)
	mux.HandleFunc("/platform/store/buy", a.handleBuy)
	mux.HandleFunc("/platform/history", a.handleHistory)
	mux.HandleFunc("/platform/leaderboard", a.handleLeaderboard)
}

// platformOp bounds one database call.
//
// Derived from the request context, so a client that gives up frees the
// connection it was holding in the pool rather than leaving it to finish a
// query nobody will read. That matters more here than on the Redis paths: a
// pool is a fixed number of connections, and requests that outlive their
// callers are how one slow query becomes every request queueing behind it.
func (a *App) platformOp(r *http.Request) (context.Context, context.CancelFunc) {
	return bounded(r.Context(), a.cfg.PlatformTimeout)
}

// account identifies the caller from its bearer token.
//
// The same JWT the WebSocket handshake accepts, deliberately: a player who has
// logged in holds exactly one credential, and issuing a second one for the HTTP
// API would be a second thing to expire, revoke and get wrong.
func (a *App) account(r *http.Request) (string, bool) {
	if a.jwt == nil {
		return "", false
	}
	// The scheme is case-insensitive — RFC 7235 says so, and clients take it at
	// its word. Matching "Bearer " exactly answers a correctly-formed request
	// with a 401, which a caller reads as an expired token and debugs for a
	// while before noticing the capital B.
	h := r.Header.Get("Authorization")
	scheme, tok, ok := strings.Cut(h, " ")
	if !ok || !strings.EqualFold(scheme, "bearer") {
		return "", false
	}
	claims, err := a.jwt.Verify(strings.TrimSpace(tok))
	if err != nil {
		return "", false
	}
	return claims.PlayerID, true
}

// platformGate is the preamble every handler shares: method, rate limit, and
// (when required) an identified caller.
//
// It carries the op name so that what it refuses is counted too. A request
// turned away here does no work and so has no latency worth recording, but it
// is not nothing: a run of result="unauthorized" on these endpoints is somebody
// working through a list of tokens, and without this it is invisible — the
// handler it was aimed at never runs, so nothing else counts it.
func (a *App) platformGate(w http.ResponseWriter, r *http.Request, op, method string, needAuth bool) (string, bool) {
	refuse := func(result, msg string, code int) {
		metrics.PlatformOps.WithLabelValues(op, result).Inc()
		http.Error(w, msg, code)
	}
	if r.Method != method {
		refuse("bad_method", "method not allowed", http.StatusMethodNotAllowed)
		return "", false
	}
	if !a.allowRate(a.platformRL, httputil.RealIP(r, a.cfg.TrustProxy)) {
		refuse("rate_limited", "rate limited", http.StatusTooManyRequests)
		return "", false
	}
	if !needAuth {
		return "", true
	}
	id, ok := a.account(r)
	if !ok {
		refuse("unauthorized", "unauthorized", http.StatusUnauthorized)
		return "", false
	}
	return id, true
}

// platformErr maps a domain error onto a status code and a sentence a person
// can read.
//
// The wording is written here rather than taken from err.Error(), and the two
// audiences are why. A domain error is named for a log — "platform: already at
// the limit for this item" says which package refused and why — and that same
// string in a dialog is a bug report waiting to be filed. So the errors keep
// their names, this decides what a client is told, and neither has to
// compromise for the other.
//
// Anything not listed is a 500 with a generic body and the detail in the log.
// Handing a client the text of a database error is how a schema, a table name
// and sometimes a value end up in somebody's browser console.
func (a *App) platformErr(w http.ResponseWriter, op string, started time.Time, err error) {
	result := "declined"
	var code int
	var msg string
	switch {
	case errors.Is(err, platform.ErrInvalid):
		// The detail is the useful half — "username must be 3-32 characters" —
		// and it was written to be read by whoever typed it.
		code = http.StatusBadRequest
		msg = strings.TrimPrefix(err.Error(), platform.ErrInvalid.Error()+": ")
	case errors.Is(err, platform.ErrBadCredentials):
		// Deliberately the same answer for an unknown username and a wrong
		// password: telling them apart tells an attacker which accounts exist.
		code, msg = http.StatusUnauthorized, "incorrect username or password"
	case errors.Is(err, platform.ErrUsernameTaken):
		code, msg = http.StatusConflict, "that username is taken"
	case errors.Is(err, platform.ErrItemLimit):
		code, msg = http.StatusConflict, "you already hold as many of that item as you can"
	case errors.Is(err, platform.ErrNoItem):
		code, msg = http.StatusNotFound, "no such item"
	case errors.Is(err, platform.ErrNoAccount):
		code, msg = http.StatusNotFound, "no such account"
	case errors.Is(err, platform.ErrInsufficientFunds):
		// 402 is the one status code that says exactly this and is almost never
		// used. A wallet that cannot cover a purchase is not a malformed
		// request and it is not a server fault.
		code, msg = http.StatusPaymentRequired, "not enough currency"
	default:
		result = "error"
		code, msg = http.StatusInternalServerError, "internal error"
		log.Printf("platform: %s: %v", op, err)
	}
	metrics.PlatformOps.WithLabelValues(op, result).Inc()
	// Observed on the way out whatever the outcome. A call that failed still
	// spent the time, and a database answering slowly enough to hit
	// PLATFORM_TIMEOUT is exactly the case an operator needs in this histogram
	// — recording only successes hides the two-second calls and leaves the
	// latency graph looking healthy right through an outage.
	metrics.PlatformLatency.WithLabelValues(op).Observe(time.Since(started).Seconds())
	http.Error(w, msg, code)
}

// platformOK writes a successful response and counts it.
func (a *App) platformOK(w http.ResponseWriter, op string, started time.Time, v any) {
	metrics.PlatformOps.WithLabelValues(op, "ok").Inc()
	metrics.PlatformLatency.WithLabelValues(op).Observe(time.Since(started).Seconds())
	w.Header().Set("Content-Type", "application/json")
	writeJSONErr("platform."+op, json.NewEncoder(w).Encode(v))
}

// readJSON decodes a bounded request body. The cap is small on purpose: these
// bodies are a username and a password, and an unbounded one is an anonymous
// caller choosing how much memory this process spends.
func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return false
	}
	return true
}

func (a *App) handleRegister(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	if _, ok := a.platformGate(w, r, "register", http.MethodPost, false); !ok {
		return
	}
	// Registration shares the login limiter as well as the platform one: it is
	// the more expensive of the two — a PBKDF2 hash plus a transaction — and it
	// is the one that creates rows.
	if !a.allowRate(a.loginRL, httputil.RealIP(r, a.cfg.TrustProxy)) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	var req struct {
		Username    string `json:"username"`
		Password    string `json:"password"`
		DisplayName string `json:"display_name"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	ctx, cancel := a.platformOp(r)
	defer cancel()
	acc, err := a.platform.Register(ctx, req.Username, req.Password, req.DisplayName)
	if err != nil {
		a.platformErr(w, "register", started, err)
		return
	}
	// Registered and logged in, in one round trip. The alternative is a client
	// that has to call login immediately afterwards with credentials it just
	// sent, which is a second chance to get it wrong and nothing gained.
	a.issueSession(w, "register", started, acc)
}

// issueSession mints the token for an authenticated account and writes the
// response both /auth/register and /auth/login return.
func (a *App) issueSession(w http.ResponseWriter, op string, started time.Time, acc platform.Account) {
	if a.jwt == nil {
		// Reachable only with the platform on and JWT_SECRET unset, which
		// production refuses to start with. In development it means accounts
		// exist but sessions are anonymous, and saying so beats a token that
		// authenticates nobody.
		metrics.PlatformOps.WithLabelValues(op, "declined").Inc()
		http.Error(w, "auth disabled: set JWT_SECRET to issue sessions", http.StatusNotFound)
		return
	}
	tok, _, exp, err := a.jwt.IssueFor(acc.ID, acc.DisplayName)
	if err != nil {
		a.platformErr(w, op, started, err)
		return
	}
	a.platformOK(w, op, started, map[string]any{
		"token": tok, "player_id": acc.ID, "expires_at": exp.Unix(),
		"account": acc,
	})
}

func (a *App) handleProfile(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	id, ok := a.platformGate(w, r, "profile", http.MethodGet, true)
	if !ok {
		return
	}
	ctx, cancel := a.platformOp(r)
	defer cancel()
	acc, err := a.platform.Account(ctx, id)
	if err != nil {
		a.platformErr(w, "profile", started, err)
		return
	}
	prof, err := a.platform.Profile(ctx, id)
	if err != nil {
		a.platformErr(w, "profile", started, err)
		return
	}
	a.platformOK(w, "profile", started, map[string]any{"account": acc, "profile": prof})
}

func (a *App) handleInventory(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	id, ok := a.platformGate(w, r, "inventory", http.MethodGet, true)
	if !ok {
		return
	}
	ctx, cancel := a.platformOp(r)
	defer cancel()
	items, err := a.platform.Inventory(ctx, id)
	if err != nil {
		a.platformErr(w, "inventory", started, err)
		return
	}
	if items == nil {
		items = []platform.Stack{}
	}
	a.platformOK(w, "inventory", started, map[string]any{"items": items})
}

// handleCatalog needs no token: a price list is the same for everybody, and
// requiring a session to read one means a player cannot see what the game sells
// before deciding to sign up for it.
func (a *App) handleCatalog(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	if _, ok := a.platformGate(w, r, "catalog", http.MethodGet, false); !ok {
		return
	}
	a.platformOK(w, "catalog", started, map[string]any{"items": a.platform.Catalog().List()})
}

func (a *App) handleBuy(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	id, ok := a.platformGate(w, r, "buy", http.MethodPost, true)
	if !ok {
		return
	}
	var req struct {
		ItemID string `json:"item_id"`
		Qty    int    `json:"qty"`
		Key    string `json:"idempotency_key"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	ctx, cancel := a.platformOp(r)
	defer cancel()
	rec, err := a.platform.Buy(ctx, id, req.ItemID, req.Qty, req.Key)
	if err != nil {
		a.platformErr(w, "buy", started, err)
		return
	}
	a.platformOK(w, "buy", started, rec)
}

func (a *App) handleHistory(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	id, ok := a.platformGate(w, r, "history", http.MethodGet, true)
	if !ok {
		return
	}
	ctx, cancel := a.platformOp(r)
	defer cancel()
	rows, err := a.platform.History(ctx, id, queryLimit(r, 20, 100))
	if err != nil {
		a.platformErr(w, "history", started, err)
		return
	}
	if rows == nil {
		rows = []platform.MatchRow{}
	}
	a.platformOK(w, "history", started, map[string]any{"matches": rows})
}

func (a *App) handleLeaderboard(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	if _, ok := a.platformGate(w, r, "leaderboard", http.MethodGet, false); !ok {
		return
	}
	ctx, cancel := a.platformOp(r)
	defer cancel()
	board, err := a.platform.Leaderboard(ctx, queryLimit(r, 50, 200))
	if err != nil {
		a.platformErr(w, "leaderboard", started, err)
		return
	}
	if board == nil {
		board = []platform.Rank{}
	}
	a.platformOK(w, "leaderboard", started, map[string]any{"ranks": board})
}

// queryLimit reads ?limit= within a ceiling the caller does not get to raise.
// An unbounded limit on a leaderboard is a full table scan any anonymous
// request can ask for.
func queryLimit(r *http.Request, def, max int) int {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

// connectAttempts and connectBackoff are the startup budget for a database
// that is not answering yet: six seconds, the same shape the Redis connection
// uses. `docker compose up` starts this process and its database at once, and a
// server that exits because Postgres was four seconds from accepting
// connections is a server nobody can start.
const (
	connectAttempts = 30
	connectBackoff  = 200 * time.Millisecond
)

// retryConnect waits out a database that is still starting, and does not wait
// out one that will never start.
//
// Split from startPlatform so the loop can be tested without a database,
// because the loop is where the bugs live: retrying zero times, retrying
// forever, or — the one that actually costs something — spending the whole
// budget on a malformed connection string and then reporting a timeout instead
// of the typo. platform.ErrBadDSN is the difference, and it is checked here
// rather than guessed from the error text.
//
// The last error is what comes back, not the first: the first is usually
// "connection refused" from before the database had opened its socket, and the
// last is whatever it is really unhappy about.
func retryConnect(ctx context.Context, attempts int, backoff time.Duration, dial func() (platform.Store, error)) (platform.Store, error) {
	var err error
	for i := 0; i < attempts; i++ {
		var store platform.Store
		store, err = dial()
		if err == nil {
			return store, nil
		}
		if errors.Is(err, platform.ErrBadDSN) {
			// No amount of waiting fixes a string. Fail now, while the message
			// still says what is wrong with it.
			return nil, err
		}
		if i == attempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}
	return nil, err
}

// startPlatform builds the platform tier and points the matchmaker's rating
// lookups at it.
func (a *App) startPlatform(cfg config.Static) error {
	var store platform.Store
	if cfg.PlatformMemory() {
		store = platform.NewMemory()
		log.Printf("platform enabled (in-memory: accounts, wallets and history die with this process)")
	} else {
		var err error
		store, err = retryConnect(context.Background(), connectAttempts, connectBackoff, func() (platform.Store, error) {
			ctx, cancel := bounded(context.Background(), cfg.PlatformTimeout)
			defer cancel()
			return platform.NewPostgres(ctx, cfg.PlatformDSN, platform.PostgresOptions{
				MaxConns: cfg.PlatformMaxConns,
				Migrate:  true,
			})
		})
		if err != nil {
			// The DSN is deliberately not in the message: it carries a
			// password. platform.ErrBadDSN's own text is redacted by pgconn.
			return fmt.Errorf("platform: postgres: %w", err)
		}
		log.Printf("platform enabled @ postgres pool=%d timeout=%s", cfg.PlatformMaxConns, cfg.PlatformTimeout)
	}
	a.platform = platform.NewService(platform.Options{Store: store})
	// The rating store the matchmaker reads through becomes the database. This
	// is the third implementation of rating.Store the README said would be one
	// constructor, and the Redis mm:rating hash goes unused from here: a 30-day
	// cache and a record of the same number are two sources of truth, and the
	// cache would win every time they disagreed.
	a.rating = a.platform.Ratings()
	return nil
}

// recordToPlatform writes one finished match.
//
// It takes its own deadline from context.Background() rather than inheriting
// the caller's. A room's OnEnd runs under the Redis budget — one second, sized
// for an HGET — and this is a transaction across four tables for every player
// in the match. Inheriting that budget would cancel the write on a busy
// database and lose the result of a match that was played in full.
func (a *App) recordToPlatform(matchID, mode string, ids []string, before, after, scores []int) {
	rep := platform.Report{
		MatchID: matchID, Mode: mode, EndedAt: time.Now(),
		Results: make([]platform.Result, 0, len(ids)),
	}
	ranks := placements(scores)
	for i, id := range ids {
		rep.Results = append(rep.Results, platform.Result{
			AccountID: id, Score: scores[i], Placement: ranks[i],
			RatingBefore: before[i], RatingAfter: after[i],
		})
	}
	ctx, cancel := bounded(context.Background(), a.cfg.PlatformTimeout)
	defer cancel()

	started := time.Now()
	n, err := a.platform.RecordMatch(ctx, rep)
	if err != nil {
		// Logged and counted, not retried. A retry loop here would hold the
		// room's end-of-match goroutine open against a database that is already
		// struggling, and the write is idempotent rather than recoverable: the
		// honest fix is an outbox, which is the same thing the Kafka publishes
		// in this file want and for the same reason.
		metrics.PlatformOps.WithLabelValues("record_match", "error").Inc()
		log.Printf("platform: record %s: %v", matchID, err)
		return
	}
	metrics.PlatformOps.WithLabelValues("record_match", "ok").Inc()
	metrics.PlatformLatency.WithLabelValues("record_match").Observe(time.Since(started).Seconds())
	metrics.MatchesRecorded.Add(float64(n))
}

// placements turns scores into standard competition ranking — 1, 2, 2, 4 —
// which is what a reward table and a history line both want. Ties genuinely
// happen: a deathmatch where nobody scored ends with everyone on zero, and
// calling one of them the winner because they sorted first would pay a win
// bonus to whoever happened to be listed first.
func placements(scores []int) []int {
	out := make([]int, len(scores))
	for i, s := range scores {
		rank := 1
		for _, other := range scores {
			if other > s {
				rank++
			}
		}
		out[i] = rank
	}
	return out
}

// onTurnEnd records a finished turn-based match.
//
// It runs on whichever node applied the last move, which is routinely not a
// node either player is attached to — a match abandoned by both sides is ended
// by the matchmaker's deadline sweeper. That is fine here: the platform store
// is reached the same way from every node, and the write is keyed on the match
// id, so a duplicate from a redelivered anything is a no-op.
//
// Handed to a goroutine because turn.Service calls this on the write path of a
// player's move. A database transaction there would put the slowest dependency
// in the process between a player pressing a card and the other player seeing
// it. The cost of doing it off the path is the honest one: a process that dies
// in the next few milliseconds loses the record of a match that was played —
// the same trade the Kafka lifecycle publishes make, and the same outbox fixes
// both the day it matters.
func (a *App) onTurnEnd(st *turn.State) {
	if a.platform == nil || st == nil {
		return
	}
	safe.Go("turn.record", func() { a.recordTurnResult(st) })
}

func (a *App) recordTurnResult(st *turn.State) {
	ctx, cancel := bounded(context.Background(), a.cfg.PlatformTimeout)
	defer cancel()

	ids := []string{st.Players[0], st.Players[1]}
	scores := []int{int(st.Scores[0]), int(st.Scores[1])}
	// A turn-based match is recorded and paid, and moves no rating: before and
	// after are the same number, so the delta the store applies is zero.
	//
	// One rating cannot rank two different games. The arena's ladder is what
	// skill-based matchmaking widens its search around, and feeding it results
	// from a card game with random pairing would make that search a search
	// around noise. A real product gives each mode its own ladder, which here
	// is a column and a key and nothing more interesting; the turn-based mode
	// does not have skill matchmaking to spend one on yet.
	ratings := a.ratingsOf(ctx, ids)
	a.recordToPlatform(st.MatchID, "turn", ids, ratings, ratings, scores)
}
