package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/config"
	"github.com/nguyenbatam/arena_game_server/internal/metrics"
	"github.com/nguyenbatam/arena_game_server/internal/placement"
	"github.com/nguyenbatam/arena_game_server/internal/platform"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
	"github.com/nguyenbatam/arena_game_server/internal/turn"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// A gateway with the platform tier on and no database behind it — the mode the
// demo comes up in, and the one that exercises every rule without a container.
func platformApp(t *testing.T) (*App, *httptest.Server) {
	t.Helper()
	a := appWithStatic(t, config.Static{
		PlatformDSN:     "memory",
		PlatformTimeout: 5 * time.Second,
		JWTSecret:       "test-secret",
		JWTTTL:          time.Hour,
	}, defaultView())
	if a.platform == nil {
		t.Fatal("PLATFORM_DSN=memory did not switch the platform tier on")
	}
	srv := httptest.NewServer(a.routes())
	t.Cleanup(srv.Close)
	return a, srv
}

func postJSON(t *testing.T, srv *httptest.Server, path, token string, body any) (*http.Response, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return do(t, req)
}

func getJSON(t *testing.T, srv *httptest.Server, path, token string) (*http.Response, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return do(t, req)
}

func do(t *testing.T, req *http.Request) (*http.Response, map[string]any) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

// register returns a token and the account id behind it.
func register(t *testing.T, srv *httptest.Server, username string) (token, id string) {
	t.Helper()
	resp, body := postJSON(t, srv, "/auth/register", "", map[string]string{
		"username": username, "password": "correct-horse", "display_name": username,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register %s: status %d (%v)", username, resp.StatusCode, body)
	}
	tok, _ := body["token"].(string)
	pid, _ := body["player_id"].(string)
	if tok == "" || pid == "" {
		t.Fatalf("register returned no session: %v", body)
	}
	return tok, pid
}

func TestRegisterThenLoginCarriesTheSameDurableIdentity(t *testing.T) {
	a, srv := platformApp(t)

	tok, id := register(t, srv, "pilot")
	if id[:2] != "a-" {
		t.Fatalf("player id %q is not an account id", id)
	}

	// The whole point of an account: log in again tomorrow and be the same
	// player. The pre-platform /auth/login minted a fresh random id every time.
	resp, body := postJSON(t, srv, "/auth/login", "", map[string]string{
		"username": "pilot", "password": "correct-horse",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login: status %d (%v)", resp.StatusCode, body)
	}
	if body["player_id"] != id {
		t.Fatalf("second login handed out %v, want the same id %s", body["player_id"], id)
	}
	// Both tokens authenticate the same account. They may well be the same
	// string: the claims are (account, name, issued-at, expires-at) with
	// one-second granularity and no per-token id, so two logins inside one
	// second sign identical bytes. That is the price of a stateless token, and
	// the thing that would change it — a jti — is only worth adding alongside a
	// revocation list to consume it.
	if claims, err := a.jwt.Verify(body["token"].(string)); err != nil || claims.PlayerID != id {
		t.Fatalf("the login token does not authenticate the account: %v (%v)", claims, err)
	}
	_ = tok

	if resp, _ := postJSON(t, srv, "/auth/login", "", map[string]string{
		"username": "pilot", "password": "wrong",
	}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong password: status %d, want 401", resp.StatusCode)
	}
	// With the platform on there is no anonymous door left: a name alone is not
	// a credential, and every platform read is authorised by this token.
	if resp, _ := postJSON(t, srv, "/auth/login", "", map[string]string{
		"name": "pilot",
	}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("nameless login: status %d, want 401", resp.StatusCode)
	}
	if resp, _ := postJSON(t, srv, "/auth/register", "", map[string]string{
		"username": "pilot", "password": "correct-horse",
	}); resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate register: status %d, want 409", resp.StatusCode)
	}
}

func TestPlatformReadsNeedAToken(t *testing.T) {
	_, srv := platformApp(t)
	tok, _ := register(t, srv, "pilot")

	for _, path := range []string{"/platform/profile", "/platform/inventory", "/platform/history"} {
		if resp, _ := getJSON(t, srv, path, ""); resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s without a token: status %d, want 401", path, resp.StatusCode)
		}
		if resp, _ := getJSON(t, srv, path, "not-a-token"); resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s with a bad token: status %d, want 401", path, resp.StatusCode)
		}
		if resp, _ := getJSON(t, srv, path, tok); resp.StatusCode != http.StatusOK {
			t.Fatalf("%s with a valid token: status %d", path, resp.StatusCode)
		}
	}
	// A price list and a ladder are the same for everybody, and a player who
	// has not signed up yet is exactly who should be able to read them.
	for _, path := range []string{"/platform/catalog", "/platform/leaderboard"} {
		if resp, _ := getJSON(t, srv, path, ""); resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d, want 200 without a token", path, resp.StatusCode)
		}
	}
}

func TestBuyingChargesOnceAndShowsUpInTheInventory(t *testing.T) {
	_, srv := platformApp(t)
	tok, _ := register(t, srv, "shopper")

	buy := func(key string) (*http.Response, map[string]any) {
		return postJSON(t, srv, "/platform/store/buy", tok, map[string]any{
			"item_id": "emote.gg", "qty": 1, "idempotency_key": key,
		})
	}
	resp, body := buy("buy-1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("buy: status %d (%v)", resp.StatusCode, body)
	}
	prof := body["profile"].(map[string]any)
	want := float64(platform.StartingCurrency - 60)
	if prof["currency"] != want {
		t.Fatalf("balance = %v, want %v", prof["currency"], want)
	}

	// The response was lost and the client sent it again.
	resp, body = buy("buy-1")
	if resp.StatusCode != http.StatusOK || body["replay"] != true {
		t.Fatalf("retry: status %d replay=%v", resp.StatusCode, body["replay"])
	}
	if body["profile"].(map[string]any)["currency"] != want {
		t.Fatalf("the retry charged again: %v", body["profile"])
	}

	_, inv := getJSON(t, srv, "/platform/inventory", tok)
	items := inv["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["item_id"] != "emote.gg" {
		t.Fatalf("inventory = %v", items)
	}

	// 402 is the whole point of having a wallet: too expensive is neither a
	// malformed request nor a server fault.
	resp, _ = postJSON(t, srv, "/platform/store/buy", tok, map[string]any{
		"item_id": "skin.void", "qty": 1, "idempotency_key": "buy-2",
	})
	if resp.StatusCode != http.StatusPaymentRequired {
		t.Fatalf("unaffordable purchase: status %d, want 402", resp.StatusCode)
	}
	resp, _ = postJSON(t, srv, "/platform/store/buy", tok, map[string]any{
		"item_id": "nope", "qty": 1, "idempotency_key": "buy-3",
	})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown item: status %d, want 404", resp.StatusCode)
	}
}

// The seam the whole package exists for: a match played in the realtime tier
// lands in the durable one — rating, record, reward and history — and a player
// reads it back over HTTP.
func TestAFinishedArenaMatchReachesTheProfileAndTheLadder(t *testing.T) {
	a, srv := platformApp(t)
	ctx := context.Background()
	winTok, winID := register(t, srv, "winner")
	_, loseID := register(t, srv, "loser")

	req := &pb.RoomRequest{
		RoomId: "room-1",
		Seats: []*pb.Seat{
			{ConnId: winID, PlayerId: 1, Name: "winner"},
			{ConnId: loseID, PlayerId: 2, Name: "loser"},
		},
	}
	snap := sim.Snapshot{
		Tick: 100, Ended: true, Winner: 1,
		Players: []sim.Player{{ID: 1, Score: 9}, {ID: 2, Score: 2}},
	}
	a.recordResult(ctx, req, snap)

	_, body := getJSON(t, srv, "/platform/profile", winTok)
	prof := body["profile"].(map[string]any)
	if prof["matches"] != float64(1) || prof["wins"] != float64(1) || prof["kills"] != float64(9) {
		t.Fatalf("winner profile = %v", prof)
	}
	if prof["rating"].(float64) <= platform.StartingRating {
		t.Fatalf("the winner's rating did not move: %v", prof["rating"])
	}
	if prof["currency"].(float64) <= float64(platform.StartingCurrency) {
		t.Fatalf("the win paid nothing: %v", prof["currency"])
	}

	_, hist := getJSON(t, srv, "/platform/history", winTok)
	rows := hist["matches"].([]any)
	if len(rows) != 1 {
		t.Fatalf("history = %v", rows)
	}
	row := rows[0].(map[string]any)
	if row["match_id"] != "room-1" || row["mode"] != "arena" || row["placement"] != float64(1) {
		t.Fatalf("history row = %v", row)
	}

	// The room job was redelivered — the reaper requeues anything taken but not
	// acked — and the second write must move nothing.
	a.recordResult(ctx, req, snap)
	_, body = getJSON(t, srv, "/platform/profile", winTok)
	if again := body["profile"].(map[string]any); again["matches"] != float64(1) {
		t.Fatalf("a redelivered match was counted twice: %v", again)
	}

	_, board := getJSON(t, srv, "/platform/leaderboard", "")
	ranks := board["ranks"].([]any)
	if len(ranks) != 2 {
		t.Fatalf("leaderboard = %v", ranks)
	}
	if top := ranks[0].(map[string]any); top["account_id"] != winID || top["rank"] != float64(1) {
		t.Fatalf("top of the ladder = %v, want the winner", top)
	}
}

// A room the matchmaker filled with bots must not be a currency faucet.
func TestAMatchAgainstBotsPaysNothing(t *testing.T) {
	a, srv := platformApp(t)
	tok, id := register(t, srv, "farmer")

	a.recordResult(context.Background(), &pb.RoomRequest{
		RoomId: "room-bots",
		Seats:  []*pb.Seat{{ConnId: id, PlayerId: 1, Name: "farmer"}},
	}, sim.Snapshot{
		Tick: 100, Ended: true, Winner: 1,
		Players: []sim.Player{{ID: 1, Score: 20}, {ID: 1000, Bot: true, Score: 0}},
	})

	_, body := getJSON(t, srv, "/platform/profile", tok)
	prof := body["profile"].(map[string]any)
	if prof["matches"] != float64(0) || prof["currency"] != float64(platform.StartingCurrency) {
		t.Fatalf("a bot match was recorded and paid: %v", prof)
	}
}

// Turn-based matches are recorded and paid, and leave the arena's ladder alone:
// one number cannot rank two different games.
func TestAFinishedTurnMatchIsRecordedWithoutMovingTheLadder(t *testing.T) {
	a, srv := platformApp(t)
	tok, oneID := register(t, srv, "cardone")
	_, twoID := register(t, srv, "cardtwo")

	a.recordTurnResult(&turn.State{
		MatchID: "turn-1",
		Players: [2]string{oneID, twoID},
		Scores:  [2]uint32{5, 3},
		Ended:   true,
		Winner:  oneID,
	})

	_, body := getJSON(t, srv, "/platform/profile", tok)
	prof := body["profile"].(map[string]any)
	if prof["matches"] != float64(1) || prof["wins"] != float64(1) {
		t.Fatalf("turn match not recorded: %v", prof)
	}
	if prof["rating"] != float64(platform.StartingRating) {
		t.Fatalf("a turn match moved the arena ladder to %v", prof["rating"])
	}
	if prof["currency"].(float64) <= float64(platform.StartingCurrency) {
		t.Fatalf("the turn match paid nothing: %v", prof["currency"])
	}

	_, hist := getJSON(t, srv, "/platform/history", tok)
	row := hist["matches"].([]any)[0].(map[string]any)
	if row["mode"] != "turn" || row["match_id"] != "turn-1" {
		t.Fatalf("history row = %v", row)
	}
}

// With PLATFORM_DSN unset nothing changes: the endpoints are not mounted and
// /auth/login hands a token to whoever asks, exactly as it did before there was
// a platform tier. The demo has to come up from a clone with no database.
func TestWithoutTheTierNothingIsMountedAndLoginStaysAnonymous(t *testing.T) {
	a := appWithStatic(t, config.Static{JWTSecret: "test-secret", JWTTTL: time.Hour}, defaultView())
	if a.platform != nil {
		t.Fatal("the platform tier came up with no DSN")
	}
	srv := httptest.NewServer(a.routes())
	defer srv.Close()

	for _, path := range []string{"/platform/profile", "/platform/leaderboard", "/auth/register"} {
		resp, _ := getJSON(t, srv, path, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: status %d, want 404 when the tier is off", path, resp.StatusCode)
		}
	}
	resp, body := postJSON(t, srv, "/auth/login", "", map[string]string{"name": "pilot"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("anonymous login: status %d", resp.StatusCode)
	}
	if pid, _ := body["player_id"].(string); pid == "" || pid[:2] != "p-" {
		t.Fatalf("anonymous login handed out %q, want a session id", pid)
	}
}

// The wiring, end to end inside one process: a real room runs to its last tick,
// its OnEnd fires, and the result is in the database a player reads over HTTP.
//
// The tests above call recordResult directly, which is the right unit but skips
// the one line that connects the two tiers — the room callback. That line is
// easy to lose in a refactor and nothing else would notice.
func TestARoomRunningToItsEndWritesThroughToThePlatform(t *testing.T) {
	a, srv := platformApp(t)
	tok, oneID := register(t, srv, "roomone")
	_, twoID := register(t, srv, "roomtwo")

	a.startRoom(placement.NewJob(&pb.RoomRequest{
		RoomId: "room-live", Seed: 7, TickRate: 100, MatchTicks: 3,
		Seats: []*pb.Seat{
			{ConnId: oneID, PlayerId: 1, Name: "roomone"},
			{ConnId: twoID, PlayerId: 2, Name: "roomtwo"},
		},
	}))

	deadline := time.After(5 * time.Second)
	for {
		_, hist := getJSON(t, srv, "/platform/history", tok)
		if rows, _ := hist["matches"].([]any); len(rows) == 1 {
			row := rows[0].(map[string]any)
			if row["match_id"] != "room-live" || row["mode"] != "arena" {
				t.Fatalf("history row = %v", row)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("the match ended and nothing reached the platform: room.OnEnd is not wired to recordResult")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// The turn-based half of the same wiring check, driven the way a client drives
// it: create the match, sync to see whose turn it is, play a card, repeat.
//
// TestAFinishedTurnMatchIsRecordedWithoutMovingTheLadder above calls
// recordTurnResult directly, which is the right unit and skips two lines that
// are easy to lose — `OnEnd: a.onTurnEnd` in New, and the goroutine onTurnEnd
// hands the write to. Nothing else would notice if either went.
func TestPlayingATurnMatchOutReachesThePlatform(t *testing.T) {
	a, srv := platformApp(t)
	ctx := context.Background()
	tok, oneID := register(t, srv, "playone")
	_, twoID := register(t, srv, "playtwo")

	if _, err := a.turn.Create(ctx, "turn-live", 42, [2]string{oneID, twoID}); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 16; i++ {
		up, err := a.turn.Sync(ctx, "turn-live", oneID, 0)
		if err != nil {
			t.Fatalf("sync: %v", err)
		}
		if up.State.Ended {
			break
		}
		mover := up.State.Turn
		// Sync as whoever is to move, so the hand in hand is theirs.
		if mover != oneID {
			if up, err = a.turn.Sync(ctx, "turn-live", mover, 0); err != nil {
				t.Fatalf("sync %s: %v", mover, err)
			}
		}
		if len(up.State.YourHand) == 0 {
			t.Fatalf("player %s is to move with an empty hand", mover)
		}
		if err := a.turn.Play(ctx, "turn-live", turn.Move{
			PlayerID: mover, Card: up.State.YourHand[0], TurnNumber: up.State.TurnNumber,
		}); err != nil {
			t.Fatalf("play %d: %v", i, err)
		}
	}

	// onTurnEnd hands the write to a goroutine on purpose — a database
	// transaction on a player's move path would put the slowest dependency in
	// the process between one player pressing a card and the other seeing it —
	// so the result arrives shortly after the match does.
	deadline := time.After(5 * time.Second)
	for {
		_, hist := getJSON(t, srv, "/platform/history", tok)
		if rows, _ := hist["matches"].([]any); len(rows) == 1 {
			row := rows[0].(map[string]any)
			if row["match_id"] != "turn-live" || row["mode"] != "turn" {
				t.Fatalf("history row = %v", row)
			}
			if row["rating_before"] != row["rating_after"] {
				t.Fatalf("a turn match moved the ladder: %v", row)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("the turn match ended and nothing reached the platform: turn.Options.OnEnd is not wired to onTurnEnd")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestQueryLimitIsBoundedAtBothEnds(t *testing.T) {
	// An unbounded limit on a leaderboard is a full table scan any anonymous
	// request can ask for, and a zero or negative one is a page nobody wanted.
	for _, tc := range []struct {
		query string
		want  int
	}{
		{"", 20},
		{"?limit=5", 5},
		{"?limit=9999", 100},
		{"?limit=0", 20},
		{"?limit=-3", 20},
		{"?limit=abc", 20},
	} {
		r, err := http.NewRequest(http.MethodGet, "http://x/platform/history"+tc.query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := queryLimit(r, 20, 100); got != tc.want {
			t.Fatalf("limit%q = %d, want %d", tc.query, got, tc.want)
		}
	}
}

func TestPlacementsRankTiesTogether(t *testing.T) {
	// Standard competition ranking: 1, 2, 2, 4. A deathmatch where nobody
	// scored ends with everyone on zero, and calling whoever sorted first the
	// winner would pay a win bonus for arriving in the right slice position.
	for _, tc := range []struct {
		scores []int
		want   []int
	}{
		{[]int{9, 2}, []int{1, 2}},
		{[]int{0, 0}, []int{1, 1}},
		{[]int{5, 7, 5, 1}, []int{2, 1, 2, 4}},
		{nil, nil},
	} {
		got := placements(tc.scores)
		if len(got) != len(tc.want) {
			t.Fatalf("placements(%v) = %v", tc.scores, got)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("placements(%v) = %v, want %v", tc.scores, got, tc.want)
			}
		}
	}
}

func TestPlatformEndpointsRefuseTheWrongMethodAndTooManyRequests(t *testing.T) {
	a := appWithStatic(t, config.Static{
		PlatformDSN: "memory", PlatformTimeout: 5 * time.Second,
		JWTSecret: "test-secret", JWTTTL: time.Hour,
		// Two requests a minute, so the third is refused rather than served.
		PlatformRateLimit: 2,
	}, defaultView())
	srv := httptest.NewServer(a.routes())
	defer srv.Close()

	// GET on a POST endpoint, and vice versa.
	if resp, _ := getJSON(t, srv, "/platform/store/buy", ""); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET on buy: status %d, want 405", resp.StatusCode)
	}
	if resp, _ := postJSON(t, srv, "/platform/leaderboard", "", nil); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST on leaderboard: status %d, want 405", resp.StatusCode)
	}

	// The method check runs before the limiter, so those two cost nothing.
	// These are the only endpoints in the process that put an anonymous request
	// in front of a database.
	codes := []int{}
	for i := 0; i < 4; i++ {
		resp, _ := getJSON(t, srv, "/platform/leaderboard", "")
		codes = append(codes, resp.StatusCode)
	}
	if codes[0] != 200 || codes[1] != 200 {
		t.Fatalf("the first two requests were not served: %v", codes)
	}
	if codes[2] != http.StatusTooManyRequests || codes[3] != http.StatusTooManyRequests {
		t.Fatalf("the limiter did not refuse past its window: %v", codes)
	}
}

func TestMalformedBodiesAreRejectedWithoutReachingTheStore(t *testing.T) {
	_, srv := platformApp(t)
	tok, _ := register(t, srv, "sloppy")

	post := func(path, body string) int {
		req, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, _ := do(t, req)
		return resp.StatusCode
	}
	if got := post("/platform/store/buy", "{not json"); got != http.StatusBadRequest {
		t.Fatalf("malformed json: status %d, want 400", got)
	}
	// The body cap is 4 KiB: these bodies are a username and a password, and an
	// unbounded one is an anonymous caller choosing how much memory to spend.
	if got := post("/auth/register", `{"username":"`+strings.Repeat("a", 8192)+`"}`); got != http.StatusBadRequest {
		t.Fatalf("oversized body: status %d, want 400", got)
	}
	if got := post("/platform/store/buy", `{"item_id":"emote.gg","qty":1}`); got != http.StatusBadRequest {
		t.Fatalf("missing idempotency key: status %d, want 400", got)
	}
}

// failingStore is a platform.Store where every call fails. It exists for one
// assertion: that a broken database is a 500 with a generic body, and never the
// text of a driver error. Handing a client that text is how a schema, a table
// name and sometimes a value end up in somebody's browser console.
type failingStore struct{ err error }

func (s failingStore) CreateAccount(context.Context, platform.Account, platform.Credential) error {
	return s.err
}

func (s failingStore) AccountByUsername(context.Context, string) (platform.Account, platform.Credential, error) {
	return platform.Account{}, platform.Credential{}, s.err
}
func (s failingStore) AccountByID(context.Context, string) (platform.Account, error) {
	return platform.Account{}, s.err
}
func (s failingStore) NoteLogin(context.Context, string, time.Time) error { return s.err }
func (s failingStore) Profile(context.Context, string) (platform.Profile, error) {
	return platform.Profile{}, s.err
}
func (s failingStore) Rating(context.Context, string) (int, error) { return 0, s.err }
func (s failingStore) RatingsFor(context.Context, []string) (map[string]int, error) {
	return nil, s.err
}
func (s failingStore) Inventory(context.Context, string) ([]platform.Stack, error) {
	return nil, s.err
}
func (s failingStore) Apply(context.Context, platform.Entry) (platform.Receipt, error) {
	return platform.Receipt{}, s.err
}
func (s failingStore) RecordMatch(context.Context, []platform.MatchRow) (int, error) {
	return 0, s.err
}
func (s failingStore) History(context.Context, string, int) ([]platform.MatchRow, error) {
	return nil, s.err
}
func (s failingStore) Leaderboard(context.Context, int) ([]platform.Rank, error) { return nil, s.err }
func (s failingStore) Close() error                                              { return nil }

func TestABrokenStoreIs500AndLeaksNothing(t *testing.T) {
	a, srv := platformApp(t)
	tok, _ := register(t, srv, "unlucky")

	// Swap the store out from under the service: everything below now fails the
	// way a database with a dropped connection does.
	secret := `pq: relation "profiles" does not exist for user arena@10.0.0.7`
	a.platform = platform.NewService(platform.Options{
		Store: failingStore{err: errors.New(secret)}, Iters: 1,
	})

	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodGet, "/platform/profile", ""},
		{http.MethodGet, "/platform/inventory", ""},
		{http.MethodGet, "/platform/history", ""},
		{http.MethodGet, "/platform/leaderboard", ""},
		{http.MethodPost, "/platform/store/buy", `{"item_id":"emote.gg","qty":1,"idempotency_key":"k"}`},
	} {
		req, err := http.NewRequest(tc.method, srv.URL+tc.path, strings.NewReader(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("%s: status %d, want 500", tc.path, resp.StatusCode)
		}
		if strings.Contains(string(body), "profiles") || strings.Contains(string(body), "10.0.0.7") {
			t.Fatalf("%s leaked the store error to the client: %q", tc.path, body)
		}
	}
}

// With the platform tier on and JWT_SECRET unset — a development-only
// combination, since production refuses to boot without the secret — accounts
// exist but there is nothing to issue a session with. Saying so beats handing
// out a token that authenticates nobody.
func TestRegisteringWithoutAJWTSecretRefusesRatherThanIssuingNothing(t *testing.T) {
	a := appWithStatic(t, config.Static{
		PlatformDSN: "memory", PlatformTimeout: 5 * time.Second,
	}, defaultView())
	if a.jwt != nil {
		t.Fatal("jwt came up with no secret")
	}
	srv := httptest.NewServer(a.routes())
	defer srv.Close()

	resp, _ := postJSON(t, srv, "/auth/register", "", map[string]string{
		"username": "pilot", "password": "correct-horse",
	})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("register with no JWT secret: status %d, want 404", resp.StatusCode)
	}
}

// The whole gateway against a real database rather than the memory twin.
//
// Every other app-level test here runs on PLATFORM_DSN=memory, which is the
// right default — CI cannot depend on a database being installed. But the
// Postgres branch of startPlatform, the pool, the schema migration and the
// rating store swap are only exercised here. `make test-platform` sets the DSN.
func TestTheGatewayComesUpAgainstARealDatabase(t *testing.T) {
	dsn := os.Getenv("PLATFORM_TEST_DSN")
	if dsn == "" {
		t.Skip("PLATFORM_TEST_DSN unset; run `make test-platform`")
	}
	a := appWithStatic(t, config.Static{
		PlatformDSN: dsn, PlatformTimeout: 5 * time.Second, PlatformMaxConns: 4,
		JWTSecret: "test-secret", JWTTTL: time.Hour,
	}, defaultView())
	if a.platform == nil {
		t.Fatal("a postgres DSN did not switch the platform tier on")
	}
	t.Cleanup(func() { _ = a.platform.Close() })
	srv := httptest.NewServer(a.routes())
	defer srv.Close()

	// Unique per run: this database is shared with the store conformance suite,
	// which truncates between its own cases and not around this one.
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	winTok, winID := register(t, srv, "gw"+suffix+"a")
	_, loseID := register(t, srv, "gw"+suffix+"b")

	// The matchmaker reads ratings through rating.Store, which startPlatform
	// repointed at the database. A new account has to read back the default.
	ctx := context.Background()
	if v := a.ratingOf(ctx, winID); v != platform.StartingRating {
		t.Fatalf("rating read through the database = %d, want %d", v, platform.StartingRating)
	}

	a.recordResult(ctx, &pb.RoomRequest{
		RoomId: "room-" + suffix,
		Seats: []*pb.Seat{
			{ConnId: winID, PlayerId: 1, Name: "winner"},
			{ConnId: loseID, PlayerId: 2, Name: "loser"},
		},
	}, sim.Snapshot{
		Tick: 100, Ended: true, Winner: 1,
		Players: []sim.Player{{ID: 1, Score: 9}, {ID: 2, Score: 2}},
	})

	_, body := getJSON(t, srv, "/platform/profile", winTok)
	prof := body["profile"].(map[string]any)
	if prof["matches"] != float64(1) || prof["wins"] != float64(1) {
		t.Fatalf("the match did not reach Postgres: %v", prof)
	}
	if prof["rating"].(float64) <= platform.StartingRating {
		t.Fatalf("rating did not move in the database: %v", prof["rating"])
	}
	// And the swapped-in store reads the new number back.
	if v := a.ratingOf(ctx, winID); float64(v) != prof["rating"] {
		t.Fatalf("rating.Store read %d, profile says %v", v, prof["rating"])
	}
}

// Every client-facing error is written for the person reading it, not for the
// log. A domain error is named so an operator can see which package refused —
// "platform: already at the limit for this item" — and that same string in a
// dialog is a bug report waiting to be filed.
//
// This is a blanket assertion rather than a list of expected sentences, so a
// sentinel added later cannot quietly start echoing its package name at users.
func TestClientErrorsAreWrittenForPeopleNotForLogs(t *testing.T) {
	_, srv := platformApp(t)
	tok, _ := register(t, srv, "reader")
	// Own it once so a second purchase is refused with ErrItemLimit.
	if resp, _ := postJSON(t, srv, "/platform/store/buy", tok, map[string]any{
		"item_id": "emote.gg", "qty": 1, "idempotency_key": "own-it",
	}); resp.StatusCode != http.StatusOK {
		t.Fatalf("setup purchase: %d", resp.StatusCode)
	}

	body := func(method, path, token, payload string) (int, string) {
		req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, strings.TrimSpace(string(b))
	}

	for _, tc := range []struct {
		name, method, path, payload string
		want                        int
	}{
		{"username taken", http.MethodPost, "/auth/register",
			`{"username":"reader","password":"correct-horse"}`, http.StatusConflict},
		{"username too short", http.MethodPost, "/auth/register",
			`{"username":"ab","password":"correct-horse"}`, http.StatusBadRequest},
		{"password too short", http.MethodPost, "/auth/register",
			`{"username":"newcomer","password":"short"}`, http.StatusBadRequest},
		{"wrong password", http.MethodPost, "/auth/login",
			`{"username":"reader","password":"nope"}`, http.StatusUnauthorized},
		{"unknown username", http.MethodPost, "/auth/login",
			`{"username":"ghost","password":"correct-horse"}`, http.StatusUnauthorized},
		{"unknown item", http.MethodPost, "/platform/store/buy",
			`{"item_id":"nope","qty":1,"idempotency_key":"k1"}`, http.StatusNotFound},
		{"too expensive", http.MethodPost, "/platform/store/buy",
			`{"item_id":"skin.void","qty":1,"idempotency_key":"k2"}`, http.StatusPaymentRequired},
		{"already owned", http.MethodPost, "/platform/store/buy",
			`{"item_id":"emote.gg","qty":1,"idempotency_key":"k3"}`, http.StatusConflict},
		{"no idempotency key", http.MethodPost, "/platform/store/buy",
			`{"item_id":"emote.gg","qty":1}`, http.StatusBadRequest},
	} {
		token := tok
		if strings.HasPrefix(tc.path, "/auth/") {
			token = ""
		}
		code, msg := body(tc.method, tc.path, token, tc.payload)
		if code != tc.want {
			t.Errorf("%s: status %d, want %d (%q)", tc.name, code, tc.want, msg)
		}
		if msg == "" {
			t.Errorf("%s: empty body — the client has nothing to show", tc.name)
			continue
		}
		if strings.Contains(msg, "platform:") || strings.Contains(msg, "invalid request:") {
			t.Errorf("%s: %q reads like a log line, not a message for a person", tc.name, msg)
		}
	}

	// An unknown username and a wrong password must be indistinguishable:
	// telling them apart tells an attacker which accounts exist.
	_, wrong := body(http.MethodPost, "/auth/login", "", `{"username":"reader","password":"nope"}`)
	_, ghost := body(http.MethodPost, "/auth/login", "", `{"username":"ghost","password":"correct-horse"}`)
	if wrong != ghost {
		t.Fatalf("login answers differ: %q vs %q — that is an account-enumeration oracle", wrong, ghost)
	}
}

// The startup retry loop, without a database.
//
// `docker compose up` starts this process and its database at once, so waiting
// is the normal path and not an error path — and the loop is where the bugs
// live: not retrying, retrying forever, losing the last error, or spending the
// whole budget on a string that was never going to parse.
func TestRetryConnectWaitsOutAStartingDatabase(t *testing.T) {
	ctx := context.Background()
	mem := platform.NewMemory()

	t.Run("succeeds on the first try", func(t *testing.T) {
		calls := 0
		got, err := retryConnect(ctx, 5, time.Millisecond, func() (platform.Store, error) {
			calls++
			return mem, nil
		})
		if err != nil || got != platform.Store(mem) {
			t.Fatalf("got %v, %v", got, err)
		}
		if calls != 1 {
			t.Fatalf("dialled %d times for a database that answered at once", calls)
		}
	})

	t.Run("waits out a database that is still starting", func(t *testing.T) {
		calls := 0
		got, err := retryConnect(ctx, 10, time.Millisecond, func() (platform.Store, error) {
			calls++
			if calls < 4 {
				return nil, errors.New("connection refused")
			}
			return mem, nil
		})
		if err != nil {
			t.Fatalf("gave up on a database that came up: %v", err)
		}
		if got == nil || calls != 4 {
			t.Fatalf("store=%v after %d attempts", got, calls)
		}
	})

	t.Run("gives up after the budget and reports the last error", func(t *testing.T) {
		calls := 0
		_, err := retryConnect(ctx, 4, time.Millisecond, func() (platform.Store, error) {
			calls++
			// The first error is usually "connection refused" from before the
			// socket was open; the last is what it is really unhappy about,
			// and that is the one worth printing.
			return nil, fmt.Errorf("attempt %d", calls)
		})
		if calls != 4 {
			t.Fatalf("dialled %d times, want the full budget of 4", calls)
		}
		if err == nil || err.Error() != "attempt 4" {
			t.Fatalf("err = %v, want the last one", err)
		}
	})

	t.Run("does not wait out a malformed DSN", func(t *testing.T) {
		calls := 0
		_, err := retryConnect(ctx, 30, time.Hour, func() (platform.Store, error) {
			calls++
			return nil, fmt.Errorf("%w: invalid port", platform.ErrBadDSN)
		})
		// A one-hour backoff: if this retried at all the test would hang, which
		// is the point. No amount of waiting fixes a typo, and the budget spent
		// waiting is a budget that then reports a timeout instead of the typo.
		if calls != 1 {
			t.Fatalf("a malformed DSN was retried %d times", calls)
		}
		if !errors.Is(err, platform.ErrBadDSN) {
			t.Fatalf("err = %v, want ErrBadDSN", err)
		}
	})

	t.Run("stops when the process is shutting down", func(t *testing.T) {
		dead, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		_, err := retryConnect(dead, 30, time.Hour, func() (platform.Store, error) {
			calls++
			return nil, errors.New("connection refused")
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if calls != 1 {
			t.Fatalf("kept dialling %d times after the context was done", calls)
		}
	})
}

// A typo in PLATFORM_DSN must fail the boot at once, with a message about the
// string — not six seconds later with a message about a timeout.
func TestAMalformedDSNFailsStartupImmediately(t *testing.T) {
	started := time.Now()
	_, err := New(config.Static{
		NodeID: "gs-test", PublicAddr: "ws://localhost:8080/ws", Role: pb.Role_ROLE_ALL,
		PlatformDSN: "postgres://user@host:notaport/db", PlatformTimeout: time.Second,
	}, config.NewLive(defaultView(), nil))
	if err == nil {
		t.Fatal("a malformed DSN started the server")
	}
	if !errors.Is(err, platform.ErrBadDSN) {
		t.Fatalf("err = %v, want ErrBadDSN", err)
	}
	// The full retry budget is six seconds. Anything near it means the loop
	// waited out a string that was never going to parse.
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("took %s to reject a malformed DSN", elapsed)
	}
	// And the password is not in the message: pgconn redacts it, and this error
	// goes straight into a log.
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("the error carries the connection string: %v", err)
	}
}

// A room id has to be unique, and the clock is not.
//
// It used to be `r-<node>-<UnixNano>`. UnixNano reports nanoseconds but the
// clock behind it is coarser: 200k calls in a tight loop measured 16k distinct
// values here, up to 18 of them sharing one. matchPass forms matches in a loop,
// so two matches inside one tick is ordinary rather than exotic.
//
// It was always a bug — two matches sharing a directory entry route a player to
// the wrong one — and recording results made it a silent one, because
// match_results is keyed on the match id. The second match's result is
// swallowed as a replay of the first, and nothing anywhere says so.
func TestRoomIDsAreUniqueUnderAClockThatIsNot(t *testing.T) {
	a := appWithStatic(t, config.Static{}, defaultView())

	const n = 50_000
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		id := a.newRoomID()
		if _, dup := seen[id]; dup {
			t.Fatalf("room id %q handed out twice in %d draws", id, i+1)
		}
		seen[id] = struct{}{}
	}

	// The node stays in front so a room can still be traced to the process that
	// opened it — the only thing the timestamp was really buying.
	if !strings.HasPrefix(a.newRoomID(), "r-"+a.cfg.NodeID+"-") {
		t.Fatalf("room id %q no longer names its node", a.newRoomID())
	}

	// And the shape of the thing this replaced, so the reason is not lost: a
	// clock-derived id collides in exactly this loop.
	clockIDs := make(map[int64]struct{}, n)
	for i := 0; i < n; i++ {
		clockIDs[time.Now().UnixNano()] = struct{}{}
	}
	if len(clockIDs) == n {
		t.Logf("note: this machine's clock did not repeat in %d calls; it does on others, "+
			"which is why the id does not depend on it", n)
	}
}

func TestTheBearerSchemeIsMatchedCaseInsensitively(t *testing.T) {
	_, srv := platformApp(t)
	tok, _ := register(t, srv, "casing")

	// RFC 7235: the scheme name is case-insensitive, and clients take it at its
	// word. Rejecting a correctly-formed header reads to a caller as an expired
	// token, which is a while spent debugging the wrong thing.
	for _, header := range []string{
		"Bearer " + tok,
		"bearer " + tok,
		"BEARER " + tok,
		"Bearer  " + tok, // extra space; the token is trimmed
	} {
		req, err := http.NewRequest(http.MethodGet, srv.URL+"/platform/profile", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", header)
		resp, _ := do(t, req)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Authorization %q: status %d, want 200", header, resp.StatusCode)
		}
	}
	for _, header := range []string{
		"",
		tok,            // no scheme at all
		"Basic " + tok, // the wrong scheme
		"Bearer",       // scheme with nothing after it
		"Bearer not.a.token",
	} {
		req, err := http.NewRequest(http.MethodGet, srv.URL+"/platform/profile", nil)
		if err != nil {
			t.Fatal(err)
		}
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		resp, _ := do(t, req)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("Authorization %q: status %d, want 401", header, resp.StatusCode)
		}
	}
}

// ---------------------------------------------------------------------------
// What the metrics say happened.
//
// Observability wiring is the part that rots without being noticed: nothing
// fails when a counter stops being incremented, and the first time anybody
// looks is during the incident it was meant to explain. These read the actual
// collectors rather than the /metrics text, so they pin the call sites.

func counterFor(t *testing.T, op, result string) float64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.PlatformOps.WithLabelValues(op, result).Write(&m); err != nil {
		t.Fatalf("read arena_platform_ops_total{%s,%s}: %v", op, result, err)
	}
	return m.GetCounter().GetValue()
}

func latencyCountFor(t *testing.T, op string) uint64 {
	t.Helper()
	obs, err := metrics.PlatformLatency.GetMetricWithLabelValues(op)
	if err != nil {
		t.Fatalf("latency for %s: %v", op, err)
	}
	var m dto.Metric
	if err := obs.(prometheus.Histogram).Write(&m); err != nil {
		t.Fatalf("read histogram: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

// A call that failed still spent the time. Recording only successes leaves the
// latency graph flat and healthy right through the outage it exists to show —
// and a database slow enough to hit PLATFORM_TIMEOUT is precisely the case an
// operator goes looking for.
func TestFailedCallsAreTimedTooNotJustSuccessfulOnes(t *testing.T) {
	_, srv := platformApp(t)
	tok, _ := register(t, srv, "timed")

	before := latencyCountFor(t, "buy")
	beforeDeclined := counterFor(t, "buy", "declined")

	// Declined: 400 points against a starting wallet that cannot cover it.
	if resp, _ := postJSON(t, srv, "/platform/store/buy", tok, map[string]any{
		"item_id": "skin.void", "qty": 1, "idempotency_key": "k1",
	}); resp.StatusCode != http.StatusPaymentRequired {
		t.Fatalf("setup: status %d, want 402", resp.StatusCode)
	}

	if got := latencyCountFor(t, "buy") - before; got != 1 {
		t.Fatalf("a declined purchase added %d latency samples, want 1", got)
	}
	if got := counterFor(t, "buy", "declined") - beforeDeclined; got != 1 {
		t.Fatalf("declined counter moved by %v, want 1", got)
	}
}

// A request turned away by the gate never reaches a handler, so nothing else in
// the process counts it. A run of result="unauthorized" on these endpoints is
// somebody working through a list of tokens; without this it is invisible.
func TestTheGateCountsWhatItRefuses(t *testing.T) {
	a := appWithStatic(t, config.Static{
		PlatformDSN: "memory", PlatformTimeout: 5 * time.Second,
		JWTSecret: "test-secret", JWTTTL: time.Hour,
		PlatformRateLimit: 2,
	}, defaultView())
	srv := httptest.NewServer(a.routes())
	defer srv.Close()

	type probe struct {
		name, op, result string
		fire             func()
	}
	for _, p := range []probe{
		{"no token", "profile", "unauthorized", func() { getJSON(t, srv, "/platform/profile", "") }},
		{"bad token", "history", "unauthorized", func() { getJSON(t, srv, "/platform/history", "nope") }},
		{"wrong method", "leaderboard", "bad_method", func() { postJSON(t, srv, "/platform/leaderboard", "", nil) }},
	} {
		before := counterFor(t, p.op, p.result)
		p.fire()
		if got := counterFor(t, p.op, p.result) - before; got != 1 {
			t.Errorf("%s: arena_platform_ops_total{op=%q,result=%q} moved by %v, want 1",
				p.name, p.op, p.result, got)
		}
	}

	// The limiter is two a minute and keyed by IP across all of these
	// endpoints, so the probes above have already spent the budget.
	//
	// Measured as a delta, not an absolute: Prometheus collectors are package
	// state and every other test in this file has already moved them. An
	// absolute assertion here passed on its own and failed in the suite, which
	// is the same mistake in a test that this test is about in the code.
	beforeLimited := counterFor(t, "leaderboard", "rate_limited")
	beforeTimed := latencyCountFor(t, "leaderboard")
	served := 0
	for i := 0; i < 4; i++ {
		resp, _ := getJSON(t, srv, "/platform/leaderboard", "")
		if resp.StatusCode == http.StatusOK {
			served++
		}
	}
	limited := 4 - served
	if limited == 0 {
		t.Fatal("the limiter let four requests through a budget of two")
	}
	if got := counterFor(t, "leaderboard", "rate_limited") - beforeLimited; got != float64(limited) {
		t.Fatalf("%d requests were refused and %v were counted", limited, got)
	}

	// A refusal must not be timed: it did no work, and a pile of microsecond
	// samples would drag the latency percentiles away from the calls that
	// actually touched the database. Only the ones that were served count.
	if got := latencyCountFor(t, "leaderboard") - beforeTimed; got != uint64(served) {
		t.Fatalf("%d requests were served and %d latency samples were recorded", served, got)
	}
}

// Shutdown releases the pool. Nothing else does, so a process that is asked to
// stop and does not would hold its connections until the container is killed —
// which on a rolling deploy is every replica at once against a connection limit
// the new ones also need.
func TestShutdownClosesThePlatformStore(t *testing.T) {
	a := appWithStatic(t, config.Static{
		PlatformDSN: "memory", PlatformTimeout: time.Second,
		JWTSecret: "test-secret", JWTTTL: time.Hour,
	}, defaultView())
	if a.platform == nil {
		t.Fatal("platform tier did not come up")
	}
	// Wrapped rather than reached into: Service does not hand its store back,
	// and it should not — nothing in the process has any business writing
	// around it.
	closed := &closeCounter{Store: platform.NewMemory()}
	a.platform = platform.NewService(platform.Options{Store: closed, Iters: 1})

	if err := a.shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if closed.n != 1 {
		t.Fatalf("Close called %d times, want 1", closed.n)
	}
}

type closeCounter struct {
	platform.Store
	n int
}

func (c *closeCounter) Close() error { c.n++; return nil }
