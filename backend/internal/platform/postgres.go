package platform

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"

	"github.com/nguyenbatam/arena_game_server/internal/rating"
)

//go:embed schema.sql
var schema string

// Postgres is the durable Store.
//
// database/sql over pgx's stdlib driver rather than pgx's own interface: the
// queries here are ordinary and the pooling is ordinary, and what is gained by
// dropping to the native API — binary protocol, COPY, listen/notify — is not
// used by anything in this package. The seam is the Store interface either way.
type Postgres struct {
	db *sql.DB
}

// PostgresOptions are the pool's shape.
type PostgresOptions struct {
	// MaxConns caps connections held against the database. It is a ceiling on
	// concurrency, not a target: Postgres does not get faster past roughly a
	// couple of connections per core, and a pool bigger than the server's
	// max_connections is an outage waiting for a traffic spike. The gateway
	// fleet multiplies this — every replica opens its own pool.
	MaxConns int
	// ConnLifetime recycles connections so a failover or a rolling restart on
	// the database side is picked up without a process restart on this one.
	ConnLifetime time.Duration
	// Migrate applies schema.sql. See the comment at the top of that file for
	// why this is not a migration tool.
	Migrate bool
}

func NewPostgres(ctx context.Context, dsn string, o PostgresOptions) (*Postgres, error) {
	// Parsed before anything is opened, for two reasons.
	//
	// sql.Open does not validate: pgx defers parsing to the first connection,
	// so a typo comes back as a connection failure indistinguishable from a
	// database that is still starting — and a caller that retries connection
	// failures then spends its whole startup budget on a string that was never
	// going to work. Separating them is what lets the retry loop give up at
	// once on the one and wait on the other.
	//
	// And pgconn redacts the password when it reports a bad string, which
	// matters for an error that is about to be logged.
	if _, err := pgconn.ParseConfig(dsn); err != nil {
		// Both wrapped: ErrBadDSN is what the retry loop checks, and the
		// parse error underneath it is what tells a person which part of the
		// string is wrong.
		return nil, fmt.Errorf("%w: %w", ErrBadDSN, err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("platform: open: %w", err)
	}
	if o.MaxConns <= 0 {
		o.MaxConns = 16
	}
	if o.ConnLifetime <= 0 {
		o.ConnLifetime = 30 * time.Minute
	}
	db.SetMaxOpenConns(o.MaxConns)
	db.SetMaxIdleConns(o.MaxConns)
	db.SetConnMaxLifetime(o.ConnLifetime)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		// The pool is being abandoned, so a close that fails changes nothing
		// the caller can act on — it is joined onto the error being returned so
		// it is not simply gone, since a pool that will not close is a pool
		// still holding connections this process is about to forget about.
		return nil, errors.Join(fmt.Errorf("platform: ping: %w", err), db.Close())
	}
	s := &Postgres{db: db}
	if o.Migrate {
		if _, err := db.ExecContext(ctx, schema); err != nil {
			return nil, errors.Join(fmt.Errorf("platform: schema: %w", err), db.Close())
		}
	}
	return s, nil
}

func (s *Postgres) DB() *sql.DB  { return s.db }
func (s *Postgres) Close() error { return s.db.Close() }

// pgCode pulls the SQLSTATE out of a driver error so a constraint can be
// reported as the thing it means. Matching on the message text is the usual
// alternative and it breaks on a locale change or a server upgrade.
func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

const (
	codeUniqueViolation     = "23505"
	codeForeignKeyViolation = "23503"
)

func (s *Postgres) CreateAccount(ctx context.Context, a Account, c Credential) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rolled back only if Commit did not run

	_, err = tx.ExecContext(ctx, `
		INSERT INTO accounts (id, username, username_key, display_name, pw_hash, pw_salt, pw_iters, created_at)
		VALUES ($1, $2, lower($2), $3, $4, $5, $6, $7)`,
		a.ID, a.Username, a.DisplayName, c.Hash, c.Salt, c.Iters, a.CreatedAt)
	if err != nil {
		if pgCode(err) == codeUniqueViolation {
			return ErrUsernameTaken
		}
		return err
	}
	// Same transaction as the account. An account without a profile is a player
	// who can log in and then 500s on every read, and it would be created by
	// nothing more exotic than a process dying between two statements.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO profiles (account_id, rating, currency) VALUES ($1, $2, $3)`,
		a.ID, StartingRating, StartingCurrency); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Postgres) AccountByUsername(ctx context.Context, username string) (Account, Credential, error) {
	var a Account
	var c Credential
	var last sql.NullTime
	err := s.db.QueryRowContext(ctx, `
		SELECT id, username, display_name, created_at, last_login_at, pw_hash, pw_salt, pw_iters
		FROM accounts WHERE username_key = lower($1)`, username).
		Scan(&a.ID, &a.Username, &a.DisplayName, &a.CreatedAt, &last, &c.Hash, &c.Salt, &c.Iters)
	if errors.Is(err, sql.ErrNoRows) {
		return Account{}, Credential{}, ErrNoAccount
	}
	if err != nil {
		return Account{}, Credential{}, err
	}
	a.LastLoginAt = last.Time
	return a, c, nil
}

func (s *Postgres) AccountByID(ctx context.Context, id string) (Account, error) {
	var a Account
	var last sql.NullTime
	err := s.db.QueryRowContext(ctx, `
		SELECT id, username, display_name, created_at, last_login_at
		FROM accounts WHERE id = $1`, id).
		Scan(&a.ID, &a.Username, &a.DisplayName, &a.CreatedAt, &last)
	if errors.Is(err, sql.ErrNoRows) {
		return Account{}, ErrNoAccount
	}
	if err != nil {
		return Account{}, err
	}
	a.LastLoginAt = last.Time
	return a, nil
}

func (s *Postgres) NoteLogin(ctx context.Context, id string, at time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE accounts SET last_login_at = $2 WHERE id = $1`, id, at)
	if err != nil {
		return err
	}
	// An UPDATE that matched nothing is not success. SQL reports it as one, and
	// taking that at face value is how this implementation and the memory twin
	// disagreed: the twin returned ErrNoAccount and this returned nil for the
	// same call. Nothing reached the difference — Login only stamps an account
	// it just authenticated — which is exactly why it is worth closing rather
	// than arguing about: an untested divergence between two stores selected by
	// one environment variable only ever surfaces in production.
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNoAccount
	}
	return nil
}

// rowQuerier is the sliver of *sql.DB and *sql.Tx that the single-row reads
// here need. It exists so a read can be run either on the pool or inside a
// transaction already in flight, without a second copy of the query.
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func profileFrom(ctx context.Context, q rowQuerier, accountID string) (Profile, error) {
	p := Profile{AccountID: accountID}
	err := q.QueryRowContext(ctx, `
		SELECT rating, matches, wins, kills, currency FROM profiles WHERE account_id = $1`, accountID).
		Scan(&p.Rating, &p.Matches, &p.Wins, &p.Kills, &p.Currency)
	if errors.Is(err, sql.ErrNoRows) {
		return Profile{}, ErrNoAccount
	}
	return p, err
}

func (s *Postgres) Profile(ctx context.Context, accountID string) (Profile, error) {
	return profileFrom(ctx, s.db, accountID)
}

func (s *Postgres) Rating(ctx context.Context, accountID string) (int, error) {
	var v int
	err := s.db.QueryRowContext(ctx,
		`SELECT rating FROM profiles WHERE account_id = $1`, accountID).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNoAccount
	}
	return v, err
}

// RatingsFor reads a whole match's ratings in one statement.
//
// = ANY($1) with a single array parameter rather than a generated IN list:
// the statement text is the same whatever the room size, so Postgres plans it
// once and the pool's prepared-statement cache holds one entry instead of one
// per distinct player count.
func (s *Postgres) RatingsFor(ctx context.Context, accountIDs []string) (map[string]int, error) {
	out := make(map[string]int, len(accountIDs))
	if len(accountIDs) == 0 {
		return out, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT account_id, rating FROM profiles WHERE account_id = ANY($1)`,
		accountIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var v int
		if err := rows.Scan(&id, &v); err != nil {
			return nil, err
		}
		out[id] = v
	}
	return out, rows.Err()
}

func (s *Postgres) Inventory(ctx context.Context, accountID string) ([]Stack, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT item_id, qty, acquired_at FROM inventory
		WHERE account_id = $1 AND qty > 0 ORDER BY item_id`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Stack
	for rows.Next() {
		var st Stack
		if err := rows.Scan(&st.ItemID, &st.Qty, &st.AcquiredAt); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// Apply is the transaction the whole package is arranged around.
//
// Three statements, in this order, and the order is the design:
//
//  1. Claim the idempotency key. ON CONFLICT DO NOTHING means a redelivered
//     request claims nothing and the transaction ends here — the check and the
//     work are inside one transaction, so there is no window in which two
//     concurrent retries of the same key both see "not applied yet".
//  2. Move the currency with the balance check *in the WHERE clause*. Reading
//     the balance and then writing it is the classic lost-update bug: two
//     requests both read 100, both decide 60 is affordable, both write 40.
//     Guarding in the UPDATE makes the database do it, and zero rows affected
//     is the refusal.
//  3. Move the item, with the per-account ceiling in the DO UPDATE's WHERE so a
//     unique cosmetic bought twice declines instead of stacking.
//
// Anything that declines rolls the whole thing back, key included — the retry
// of a refused purchase must be free to succeed once the wallet can afford it.
func (s *Postgres) Apply(ctx context.Context, e Entry) (Receipt, error) {
	if e.Max > 0 && e.Qty > e.Max {
		return Receipt{}, ErrItemLimit
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Receipt{}, err
	}
	defer tx.Rollback() //nolint:errcheck // rolled back only if Commit did not run

	res, err := tx.ExecContext(ctx, `
		INSERT INTO ledger (account_id, idem_key, kind, item_id, qty, delta, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (account_id, idem_key) DO NOTHING`,
		e.AccountID, e.Key, e.Kind, e.ItemID, e.Qty, e.Delta, e.At)
	if err != nil {
		if pgCode(err) == codeForeignKeyViolation {
			return Receipt{}, ErrNoAccount
		}
		return Receipt{}, err
	}
	if n, err := res.RowsAffected(); err != nil {
		return Receipt{}, err
	} else if n == 0 {
		// Read through tx, not s.db. Going to the pool here would have this one
		// request holding two connections at once — the transaction it is still
		// inside, plus a fresh one for the read — and a pool sized near the
		// number of concurrent requests then deadlocks: every request holds its
		// transaction and waits for a second connection that only another
		// request could release. The transaction is read-committed and has
		// written nothing it needs to not see, so reading through it is also
		// the more accurate answer.
		return replayIn(ctx, tx, e)
	}

	p := Profile{AccountID: e.AccountID}
	err = tx.QueryRowContext(ctx, `
		UPDATE profiles SET currency = currency + $2
		WHERE account_id = $1 AND currency + $2 >= 0
		RETURNING rating, matches, wins, kills, currency`, e.AccountID, e.Delta).
		Scan(&p.Rating, &p.Matches, &p.Wins, &p.Kills, &p.Currency)
	if errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, ErrInsufficientFunds
	}
	if err != nil {
		return Receipt{}, err
	}

	item := Stack{ItemID: e.ItemID}
	switch {
	case e.ItemID != "" && e.Qty == 0:
		// A currency movement that still names an item — a refund, a reward
		// paid against a thing somebody owns. Nothing moves in the inventory,
		// but the receipt still says where that item stands, because the
		// alternative is a Qty of 0 that reads as "you own none" when they own
		// three. The memory twin answered this way already.
		err = tx.QueryRowContext(ctx, `
			SELECT qty, acquired_at FROM inventory WHERE account_id = $1 AND item_id = $2`,
			e.AccountID, e.ItemID).Scan(&item.Qty, &item.AcquiredAt)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return Receipt{}, err
		}
	case e.ItemID != "" && e.Qty != 0:
		err = tx.QueryRowContext(ctx, `
			INSERT INTO inventory (account_id, item_id, qty, acquired_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (account_id, item_id) DO UPDATE SET qty = inventory.qty + EXCLUDED.qty
			WHERE $5 = 0 OR inventory.qty + EXCLUDED.qty <= $5
			RETURNING qty, acquired_at`,
			e.AccountID, e.ItemID, e.Qty, e.At, e.Max).Scan(&item.Qty, &item.AcquiredAt)
		if errors.Is(err, sql.ErrNoRows) {
			return Receipt{}, ErrItemLimit
		}
		if err != nil {
			return Receipt{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Receipt{}, err
	}
	return Receipt{Profile: p, Item: item}, nil
}

// replayIn answers a key that had already been applied: the current state, not
// the original response. See Receipt.Replay.
//
// It reads through whatever q it is given so Apply can call it from inside its
// own transaction rather than reaching back into the pool for a second
// connection.
func replayIn(ctx context.Context, q rowQuerier, e Entry) (Receipt, error) {
	p, err := profileFrom(ctx, q, e.AccountID)
	if err != nil {
		return Receipt{}, err
	}
	item := Stack{ItemID: e.ItemID}
	if e.ItemID != "" {
		err := q.QueryRowContext(ctx, `
			SELECT qty, acquired_at FROM inventory WHERE account_id = $1 AND item_id = $2`,
			e.AccountID, e.ItemID).Scan(&item.Qty, &item.AcquiredAt)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return Receipt{}, err
		}
	}
	return Receipt{Profile: p, Item: item, Replay: true}, nil
}

// RecordMatch writes a finished match: one history line per player, and the
// profile movement each line implies, in one transaction.
//
// Two statements for the whole match rather than two per player. The loop this
// replaces made the cost of recording a match grow with how many people were in
// it — eight players was eighteen round trips inside an open transaction, which
// is eighteen network waits holding row locks that every other write to those
// profiles queues behind. The set-based version is a fixed four regardless of
// room size, and the rules it enforces are unchanged:
//
//   - a seat with no account behind it (a bot, a deleted player) is skipped
//     rather than failing the match for everybody else — that is the EXISTS;
//   - a line already written is not written again and does not move the profile
//     a second time — that is ON CONFLICT DO NOTHING, and RETURNING is how the
//     profile update learns which lines were actually new;
//   - the rating moves by a delta and stops at the ladder floor. See
//     MatchRow.RatingDelta for why relative rather than absolute.
func (s *Postgres) RecordMatch(ctx context.Context, rows []MatchRow) (int, error) {
	// One line per account, first wins. A repeated account inside a single call
	// used to be absorbed by ON CONFLICT on the second insert; a set-based
	// insert would instead let it through the profile update twice, and the
	// memory store keeps the first line too. Nothing produces one today, which
	// is exactly why it should not be left to chance.
	rows = dedupeByAccount(rows)
	if len(rows) == 0 {
		return 0, nil
	}

	matchID := make([]string, len(rows))
	accountID := make([]string, len(rows))
	mode := make([]string, len(rows))
	score := make([]int32, len(rows))
	placement := make([]int32, len(rows))
	before := make([]int32, len(rows))
	after := make([]int32, len(rows))
	reward := make([]int64, len(rows))
	endedAt := make([]time.Time, len(rows))
	for i, r := range rows {
		matchID[i], accountID[i], mode[i] = r.MatchID, r.AccountID, r.Mode
		score[i], placement[i] = int32(r.Score), int32(r.Placement)
		before[i], after[i] = int32(r.RatingBefore), int32(r.RatingAfter)
		reward[i], endedAt[i] = r.Reward, r.EndedAt
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // rolled back only if Commit did not run

	written, err := tx.QueryContext(ctx, `
		INSERT INTO match_results
			(match_id, account_id, mode, score, placement, rating_before, rating_after, reward, ended_at)
		SELECT i.match_id, i.account_id, i.mode, i.score, i.placement,
		       i.rating_before, i.rating_after, i.reward, i.ended_at
		FROM unnest($1::text[], $2::text[], $3::text[], $4::int[], $5::int[],
		            $6::int[], $7::int[], $8::bigint[], $9::timestamptz[])
			AS i(match_id, account_id, mode, score, placement,
			     rating_before, rating_after, reward, ended_at)
		WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.id = i.account_id)
		ON CONFLICT (match_id, account_id) DO NOTHING
		RETURNING account_id`,
		matchID, accountID, mode, score, placement, before, after, reward, endedAt)
	if err != nil {
		return 0, err
	}

	at := make(map[string]int, len(rows))
	for i, r := range rows {
		at[r.AccountID] = i
	}
	var (
		upID    []string
		upDelta []int32
		upWin   []int32
		upKills []int32
		upPay   []int64
	)
	for written.Next() {
		var id string
		if err := written.Scan(&id); err != nil {
			written.Close() //nolint:errcheck // the transaction is being rolled back
			return 0, err
		}
		i, ok := at[id]
		if !ok {
			continue
		}
		r := rows[i]
		win := int32(0)
		if r.Won() {
			win = 1
		}
		upID = append(upID, id)
		upDelta = append(upDelta, int32(r.RatingDelta()))
		upWin = append(upWin, win)
		upKills = append(upKills, int32(r.Score))
		upPay = append(upPay, r.Reward)
	}
	if err := written.Err(); err != nil {
		written.Close() //nolint:errcheck // the transaction is being rolled back
		return 0, err
	}
	// Closed before the next statement: database/sql pins the transaction's
	// connection to an open Rows, and the update below would deadlock behind it.
	if err := written.Close(); err != nil {
		return 0, err
	}
	if len(upID) == 0 {
		// Every line was already recorded, or belonged to nobody. Committing an
		// empty transaction is cheaper than working out whether to.
		return 0, tx.Commit()
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE profiles p
		SET rating   = GREATEST(p.rating + u.rating_delta, $6),
		    matches  = p.matches + 1,
		    wins     = p.wins + u.win,
		    kills    = p.kills + u.kills,
		    currency = p.currency + u.reward
		FROM unnest($1::text[], $2::int[], $3::int[], $4::int[], $5::bigint[])
			AS u(account_id, rating_delta, win, kills, reward)
		WHERE p.account_id = u.account_id`,
		upID, upDelta, upWin, upKills, upPay, rating.Floor); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(upID), nil
}

// dedupeByAccount keeps the first line per account, preserving order.
func dedupeByAccount(rows []MatchRow) []MatchRow {
	seen := make(map[string]struct{}, len(rows))
	out := rows[:0:0]
	for _, r := range rows {
		if _, dup := seen[r.AccountID]; dup {
			continue
		}
		seen[r.AccountID] = struct{}{}
		out = append(out, r)
	}
	return out
}

func (s *Postgres) History(ctx context.Context, accountID string, limit int) ([]MatchRow, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT match_id, mode, score, placement, rating_before, rating_after, reward, ended_at
		FROM match_results WHERE account_id = $1
		ORDER BY ended_at DESC, match_id DESC LIMIT $2`, accountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MatchRow
	for rows.Next() {
		r := MatchRow{AccountID: accountID}
		if err := rows.Scan(&r.MatchID, &r.Mode, &r.Score, &r.Placement,
			&r.RatingBefore, &r.RatingAfter, &r.Reward, &r.EndedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Postgres) Leaderboard(ctx context.Context, limit int) ([]Rank, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT p.account_id, a.display_name, p.rating, p.matches, p.wins
		FROM profiles p JOIN accounts a ON a.id = p.account_id
		WHERE p.matches > 0
		ORDER BY p.rating DESC, p.account_id ASC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Rank
	for rows.Next() {
		var r Rank
		if err := rows.Scan(&r.AccountID, &r.DisplayName, &r.Rating, &r.Matches, &r.Wins); err != nil {
			return nil, err
		}
		r.Rank = len(out) + 1
		out = append(out, r)
	}
	return out, rows.Err()
}
