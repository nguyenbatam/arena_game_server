package platform

import (
	"context"
	"errors"
	"testing"
	"time"
)

// What happens when a write is cut off half-way through.
//
// Everything else in the suite exercises the paths the store takes on purpose —
// a purchase that succeeds, one that is declined, one that is a replay. This
// file is about the one it does not take on purpose: the caller gives up while
// the transaction is open. That is not hypothetical here. PLATFORM_TIMEOUT is a
// deadline on exactly these calls, and a database under load is exactly when it
// fires.
//
// The failure is forced deterministically rather than by racing a short timer:
// another connection holds a row lock, so the guarded UPDATE blocks until the
// deadline fires, every time. Postgres only, because there is nothing to force
// in the memory twin — it holds one mutex for the whole operation, which is
// atomic by construction rather than by transaction.
//
// The property is the one the package rests on: **all of it, or none of it.**
// Half a purchase — a claimed idempotency key with no charge behind it — is the
// worst outcome available, because the retry is then refused as a duplicate and
// the player has paid nothing and owns nothing, with no error anywhere to say
// so.

// lockProfile takes a row lock on one account's profile from a separate
// connection and returns a function that releases it.
func lockProfile(t *testing.T, pg *Postgres, accountID string) func() {
	t.Helper()
	ctx := context.Background()
	tx, err := pg.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin blocker: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		`SELECT 1 FROM profiles WHERE account_id = $1 FOR UPDATE`, accountID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("lock profile: %v", err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_ = tx.Rollback()
	}
	t.Cleanup(release)
	return release
}

func countRows(t *testing.T, pg *Postgres, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pg.DB().QueryRowContext(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestAPurchaseCutOffMidTransactionLeavesNothingBehind(t *testing.T) {
	pg := freshPostgres(t)
	ctx := context.Background()
	svc := NewService(Options{Store: pg, Iters: 1})
	a := mkAccount(t, pg, "interrupted")

	release := lockProfile(t, pg, a.ID)

	// The ledger insert lands first and the currency UPDATE blocks behind the
	// lock, so the deadline fires with the key claimed and nothing paid — the
	// exact half-written state the transaction exists to prevent.
	deadline, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, err := pg.Apply(deadline, Entry{
		AccountID: a.ID, Key: "buy-1", Kind: "purchase",
		ItemID: "emote.gg", Qty: 1, Delta: -60, Max: 1, At: time.Now(),
	})
	cancel()
	if err == nil {
		t.Fatal("the purchase completed while the profile row was locked")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the deadline", err)
	}
	release()

	if n := countRows(t, pg, `SELECT count(*) FROM ledger WHERE account_id = $1`, a.ID); n != 0 {
		t.Fatalf("%d ledger rows survived the rollback — the key is claimed and nothing was paid", n)
	}
	if n := countRows(t, pg, `SELECT count(*) FROM inventory WHERE account_id = $1`, a.ID); n != 0 {
		t.Fatalf("%d inventory rows survived the rollback", n)
	}
	p, err := pg.Profile(ctx, a.ID)
	if err != nil {
		t.Fatalf("profile: %v", err)
	}
	if p.Currency != StartingCurrency {
		t.Fatalf("balance = %d, want the untouched %d", p.Currency, StartingCurrency)
	}

	// And the whole point of rolling the key back: the same request succeeds
	// once the database can serve it. A client retrying with the key it already
	// sent must not be told it already bought something it does not own.
	rec, err := svc.Buy(ctx, a.ID, "emote.gg", 1, "buy-1")
	if err != nil {
		t.Fatalf("retry after the interruption: %v", err)
	}
	if rec.Replay {
		t.Fatal("the retry was refused as a duplicate of a purchase that never happened")
	}
	if rec.Item.Qty != 1 || rec.Profile.Currency != StartingCurrency-60 {
		t.Fatalf("retry = %+v", rec)
	}
}

func TestAMatchResultCutOffMidTransactionLeavesNothingBehind(t *testing.T) {
	pg := freshPostgres(t)
	ctx := context.Background()
	svc := NewService(Options{Store: pg, Iters: 1})
	one := mkAccount(t, pg, "cutone")
	two := mkAccount(t, pg, "cuttwo")

	// The lock is on the second player, so the first player's row is already
	// inserted and their profile already updated when the deadline fires. If
	// RecordMatch were a statement at a time rather than a transaction, that
	// first player would keep a rating and a reward from a match the other half
	// of never recorded — and the retry would skip them as already done.
	release := lockProfile(t, pg, two.ID)

	rep := Report{
		MatchID: "room-cut", Mode: "arena", EndedAt: time.Unix(1_700_000_000, 0),
		Results: []Result{
			{AccountID: one.ID, Score: 9, Placement: 1, RatingBefore: 1000, RatingAfter: 1016},
			{AccountID: two.ID, Score: 2, Placement: 2, RatingBefore: 1000, RatingAfter: 984},
		},
	}
	deadline, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, err := svc.RecordMatch(deadline, rep)
	cancel()
	if err == nil {
		t.Fatal("the result was recorded while a profile row was locked")
	}
	release()

	if n := countRows(t, pg, `SELECT count(*) FROM match_results WHERE match_id = 'room-cut'`); n != 0 {
		t.Fatalf("%d match rows survived the rollback", n)
	}
	for _, a := range []Account{one, two} {
		p, err := pg.Profile(ctx, a.ID)
		if err != nil {
			t.Fatalf("profile %s: %v", a.Username, err)
		}
		if p.Matches != 0 || p.Rating != StartingRating || p.Currency != StartingCurrency {
			t.Fatalf("%s kept part of an unrecorded match: %+v", a.Username, p)
		}
	}

	// Replayed in full once the database is free, which is what the room's
	// OnEnd would do on the next delivery of the same job.
	n, err := svc.RecordMatch(ctx, rep)
	if err != nil || n != 2 {
		t.Fatalf("replay after the interruption = %d, %v; want both players", n, err)
	}
	if p, _ := pg.Profile(ctx, one.ID); p.Matches != 1 || p.Rating != 1016 {
		t.Fatalf("winner = %+v", p)
	}
	if p, _ := pg.Profile(ctx, two.ID); p.Matches != 1 || p.Rating != 984 {
		t.Fatalf("loser = %+v", p)
	}
}

// A store whose pool has been closed is what a process looks like after
// shutdown, and what a caller sees if a dependency is torn down out of order.
// Every method has to report it rather than panic — a nil map, a nil rows, or a
// dereference of a result that was never fetched would all end the goroutine
// instead of the request.
func TestEveryMethodReportsAClosedPoolRatherThanPanicking(t *testing.T) {
	pg := freshPostgres(t)
	ctx := context.Background()
	a := mkAccount(t, pg, "afterclose")
	if err := pg.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	calls := map[string]func() error{
		"CreateAccount": func() error {
			return pg.CreateAccount(ctx, Account{ID: "a-x", Username: "x"}, Credential{Iters: 1})
		},
		"AccountByUsername": func() error { _, _, err := pg.AccountByUsername(ctx, "afterclose"); return err },
		"AccountByID":       func() error { _, err := pg.AccountByID(ctx, a.ID); return err },
		"NoteLogin":         func() error { return pg.NoteLogin(ctx, a.ID, time.Now()) },
		"Profile":           func() error { _, err := pg.Profile(ctx, a.ID); return err },
		"Rating":            func() error { _, err := pg.Rating(ctx, a.ID); return err },
		"Inventory":         func() error { _, err := pg.Inventory(ctx, a.ID); return err },
		"Apply": func() error {
			_, err := pg.Apply(ctx, Entry{AccountID: a.ID, Key: "k", Kind: "grant", Delta: 1})
			return err
		},
		"RecordMatch": func() error {
			_, err := pg.RecordMatch(ctx, []MatchRow{{MatchID: "m", AccountID: a.ID}})
			return err
		},
		"History":     func() error { _, err := pg.History(ctx, a.ID, 10); return err },
		"Leaderboard": func() error { _, err := pg.Leaderboard(ctx, 10); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			if err := call(); err == nil {
				t.Fatalf("%s reported success against a closed pool", name)
			}
		})
	}
}
