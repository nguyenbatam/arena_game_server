package platform

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/rating"
)

// The two implementations must agree.
//
// This is the same rule the Redis stores in this repo follow, for the same
// reason: a difference between the twin the tests run against and the store
// production runs against only ever shows up in production. Memory always runs;
// Postgres runs when PLATFORM_TEST_DSN points at a database, which `make
// test-platform` starts.
//
// Every case below is written against the Store interface and nothing else, so
// adding a third implementation means adding a line to this list.
func eachStore(t *testing.T, f func(t *testing.T, s Store)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { f(t, NewMemory()) })
	t.Run("postgres", func(t *testing.T) { f(t, freshPostgres(t)) })
}

// freshPostgres connects to PLATFORM_TEST_DSN and empties the schema, skipping
// the test when there is no database to talk to.
func freshPostgres(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv("PLATFORM_TEST_DSN")
	if dsn == "" {
		t.Skip("PLATFORM_TEST_DSN unset; run `make test-platform` for the Postgres half")
	}
	ctx := context.Background()
	pg, err := NewPostgres(ctx, dsn, PostgresOptions{Migrate: true, MaxConns: 4})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = pg.Close() })
	// Every case starts from an empty schema. CASCADE reaches profiles,
	// inventory, ledger and match_results through their foreign keys.
	if _, err := pg.DB().ExecContext(ctx, `TRUNCATE accounts CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return pg
}

func mkAccount(t *testing.T, s Store, username string) Account {
	t.Helper()
	svc := NewService(Options{Store: s, Iters: 1})
	a, err := svc.Register(context.Background(), username, "correct-horse", username+" display")
	if err != nil {
		t.Fatalf("register %s: %v", username, err)
	}
	return a
}

func TestAccountsAreUniquePerUsernameIgnoringCase(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})

		a, err := svc.Register(ctx, "Pilot", "correct-horse", "Pilot")
		if err != nil {
			t.Fatalf("register: %v", err)
		}
		if _, err := svc.Register(ctx, "pilot", "correct-horse", "impostor"); !errors.Is(err, ErrUsernameTaken) {
			t.Fatalf("second register: want ErrUsernameTaken, got %v", err)
		}

		// Login must find the account whatever case it is typed in: the
		// username is a key, and a key that depends on shift is a support
		// ticket.
		got, err := svc.Login(ctx, "PILOT", "correct-horse")
		if err != nil {
			t.Fatalf("login: %v", err)
		}
		if got.ID != a.ID {
			t.Fatalf("login returned %s, want %s", got.ID, a.ID)
		}
		if _, err := svc.Login(ctx, "pilot", "wrong"); !errors.Is(err, ErrBadCredentials) {
			t.Fatalf("bad password: want ErrBadCredentials, got %v", err)
		}
		if _, err := svc.Login(ctx, "nobody", "correct-horse"); !errors.Is(err, ErrBadCredentials) {
			t.Fatalf("unknown user: want ErrBadCredentials, got %v", err)
		}
	})
}

func TestNewAccountStartsWithARatingAndAWallet(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		a := mkAccount(t, s, "fresh")

		p, err := s.Profile(ctx, a.ID)
		if err != nil {
			t.Fatalf("profile: %v", err)
		}
		if p.Rating != StartingRating || p.Currency != StartingCurrency {
			t.Fatalf("profile = %+v, want rating %d currency %d", p, StartingRating, StartingCurrency)
		}
		if v, err := s.Rating(ctx, a.ID); err != nil || v != StartingRating {
			t.Fatalf("rating = %d, %v", v, err)
		}
	})
}

func TestUnknownAccount(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		if _, err := s.Profile(ctx, "a-nope"); !errors.Is(err, ErrNoAccount) {
			t.Fatalf("profile: want ErrNoAccount, got %v", err)
		}
		if _, err := s.Rating(ctx, "a-nope"); !errors.Is(err, ErrNoAccount) {
			t.Fatalf("rating: want ErrNoAccount, got %v", err)
		}
		// Lists answer empty rather than erroring — see the Store interface.
		if items, err := s.Inventory(ctx, "a-nope"); err != nil || len(items) != 0 {
			t.Fatalf("inventory = %v, %v", items, err)
		}
		if rows, err := s.History(ctx, "a-nope", 10); err != nil || len(rows) != 0 {
			t.Fatalf("history = %v, %v", rows, err)
		}
		if _, err := s.Apply(ctx, Entry{AccountID: "a-nope", Key: "k", Kind: "grant", Delta: 1}); !errors.Is(err, ErrNoAccount) {
			t.Fatalf("apply: want ErrNoAccount, got %v", err)
		}
	})
}

func TestPurchaseIsChargedOnceHoweverOftenItIsRetried(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		a := mkAccount(t, s, "buyer")
		item := svc.Catalog()["skin.crimson"]

		first, err := svc.Buy(ctx, a.ID, item.ID, 1, "buy-1")
		if err != nil {
			t.Fatalf("buy: %v", err)
		}
		if first.Replay {
			t.Fatal("first purchase reported as a replay")
		}
		want := StartingCurrency - item.Price
		if first.Profile.Currency != want {
			t.Fatalf("balance = %d, want %d", first.Profile.Currency, want)
		}

		// The client never learned the first attempt landed and sent it again.
		second, err := svc.Buy(ctx, a.ID, item.ID, 1, "buy-1")
		if err != nil {
			t.Fatalf("retry: %v", err)
		}
		if !second.Replay {
			t.Fatal("retry was not reported as a replay")
		}
		if second.Profile.Currency != want {
			t.Fatalf("retry moved the balance to %d, want %d", second.Profile.Currency, want)
		}
		if second.Item.Qty != 1 {
			t.Fatalf("retry left qty %d, want 1", second.Item.Qty)
		}

		items, err := s.Inventory(ctx, a.ID)
		if err != nil || len(items) != 1 || items[0].ItemID != item.ID || items[0].Qty != 1 {
			t.Fatalf("inventory = %v, %v", items, err)
		}
	})
}

// An idempotency key is the client's to choose, so two clients choose the same
// one. Stored bare, the second player's purchase is swallowed as a duplicate:
// no charge, no item, no error to look at. internal/turn hit this with move
// keys; the ledger is keyed the same way for the same reason.
func TestIdempotencyKeysAreScopedToTheAccount(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		one := mkAccount(t, s, "playerone")
		two := mkAccount(t, s, "playertwo")

		for _, id := range []string{one.ID, two.ID} {
			r, err := svc.Buy(ctx, id, "emote.gg", 1, "buy-1")
			if err != nil {
				t.Fatalf("buy for %s: %v", id, err)
			}
			if r.Replay {
				t.Fatalf("%s: purchase collided with the other account's key", id)
			}
		}
	})
}

func TestAWalletCannotBeOverdrawnAndTheKeyStaysFree(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		a := mkAccount(t, s, "broke")

		// 400 against a starting 250.
		if _, err := svc.Buy(ctx, a.ID, "skin.void", 1, "buy-void"); !errors.Is(err, ErrInsufficientFunds) {
			t.Fatalf("want ErrInsufficientFunds, got %v", err)
		}
		p, err := s.Profile(ctx, a.ID)
		if err != nil || p.Currency != StartingCurrency {
			t.Fatalf("refused purchase moved the balance: %+v (%v)", p, err)
		}

		// A refusal must not burn the key: the same request has to be able to
		// succeed once the wallet can afford it.
		if _, err := svc.Grant(ctx, a.ID, "", 0, 200, "award-1"); err != nil {
			t.Fatalf("grant: %v", err)
		}
		r, err := svc.Buy(ctx, a.ID, "skin.void", 1, "buy-void")
		if err != nil {
			t.Fatalf("retry after top-up: %v", err)
		}
		if r.Replay || r.Item.Qty != 1 {
			t.Fatalf("retry after top-up = %+v", r)
		}
		if want := StartingCurrency + 200 - 400; r.Profile.Currency != want {
			t.Fatalf("balance = %d, want %d", r.Profile.Currency, want)
		}
	})
}

func TestAUniqueCosmeticCannotBeBoughtTwice(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		a := mkAccount(t, s, "collector")

		if _, err := svc.Buy(ctx, a.ID, "emote.gg", 1, "k1"); err != nil {
			t.Fatalf("buy: %v", err)
		}
		// A different key, so this is a second purchase and not a retry.
		if _, err := svc.Buy(ctx, a.ID, "emote.gg", 1, "k2"); !errors.Is(err, ErrItemLimit) {
			t.Fatalf("want ErrItemLimit, got %v", err)
		}
		p, _ := s.Profile(ctx, a.ID)
		if want := StartingCurrency - 60; p.Currency != want {
			t.Fatalf("declined purchase charged: balance %d, want %d", p.Currency, want)
		}
		// A consumable has no ceiling and stacks.
		if _, err := svc.Buy(ctx, a.ID, "boost.xp", 2, "k3"); err != nil {
			t.Fatalf("consumable: %v", err)
		}
		items, _ := s.Inventory(ctx, a.ID)
		if len(items) != 2 {
			t.Fatalf("inventory = %v", items)
		}
	})
}

func TestRecordMatchIsIdempotentPerPlayer(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		win := mkAccount(t, s, "winner")
		lose := mkAccount(t, s, "loser")

		rep := Report{
			MatchID: "room-1", Mode: "arena", EndedAt: time.Unix(1_700_000_000, 0),
			Results: []Result{
				{AccountID: win.ID, Score: 7, Placement: 1, RatingBefore: 1000, RatingAfter: 1016},
				{AccountID: lose.ID, Score: 2, Placement: 2, RatingBefore: 1000, RatingAfter: 984},
				// A bot, or somebody whose account is gone. One unusable seat
				// must not cost the other players their result.
				{AccountID: "a-ghost", Score: 1, Placement: 3, RatingBefore: 1000, RatingAfter: 1000},
			},
		}
		n, err := svc.RecordMatch(ctx, rep)
		if err != nil || n != 2 {
			t.Fatalf("record = %d, %v; want 2", n, err)
		}

		p, _ := s.Profile(ctx, win.ID)
		reward := DefaultRewards.Payout(rep.Results[0])
		if p.Rating != 1016 || p.Matches != 1 || p.Wins != 1 || p.Kills != 7 {
			t.Fatalf("winner profile = %+v", p)
		}
		if p.Currency != StartingCurrency+reward {
			t.Fatalf("winner balance = %d, want %d", p.Currency, StartingCurrency+reward)
		}
		if lp, _ := s.Profile(ctx, lose.ID); lp.Wins != 0 || lp.Rating != 984 {
			t.Fatalf("loser profile = %+v", lp)
		}

		// The job was redelivered, or the gateway retried after a timeout.
		n, err = svc.RecordMatch(ctx, rep)
		if err != nil || n != 0 {
			t.Fatalf("replay = %d, %v; want 0", n, err)
		}
		again, _ := s.Profile(ctx, win.ID)
		if again != p {
			t.Fatalf("replay moved the profile: %+v -> %+v", p, again)
		}
	})
}

// A writer can die between two players' rows. The retry has to write exactly
// the ones that are missing — which is what keying on (match, account) buys
// over a single "have we seen this match?" flag.
func TestRecordMatchCompletesAPartialWrite(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		one := mkAccount(t, s, "halfone")
		two := mkAccount(t, s, "halftwo")

		half := Report{MatchID: "room-2", Mode: "arena", Results: []Result{
			{AccountID: one.ID, Score: 3, Placement: 1, RatingBefore: 1000, RatingAfter: 1010},
		}}
		if n, err := svc.RecordMatch(ctx, half); err != nil || n != 1 {
			t.Fatalf("first half = %d, %v", n, err)
		}
		full := half
		full.Results = append(append([]Result(nil), half.Results...), Result{
			AccountID: two.ID, Score: 1, Placement: 2, RatingBefore: 1000, RatingAfter: 990,
		})
		if n, err := svc.RecordMatch(ctx, full); err != nil || n != 1 {
			t.Fatalf("completion = %d, %v; want exactly the missing row", n, err)
		}
		if p, _ := s.Profile(ctx, one.ID); p.Matches != 1 {
			t.Fatalf("already-recorded player counted twice: %+v", p)
		}
		if p, _ := s.Profile(ctx, two.ID); p.Matches != 1 || p.Rating != 990 {
			t.Fatalf("missing player not completed: %+v", p)
		}
	})
}

func TestHistoryIsNewestFirstAndBounded(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		a := mkAccount(t, s, "historian")

		base := time.Unix(1_700_000_000, 0)
		for i := 0; i < 5; i++ {
			rep := Report{
				MatchID: fmt.Sprintf("room-%d", i), Mode: "turn",
				EndedAt: base.Add(time.Duration(i) * time.Minute),
				Results: []Result{{AccountID: a.ID, Score: i, Placement: 1, RatingBefore: 1000, RatingAfter: 1000 + i}},
			}
			if _, err := svc.RecordMatch(ctx, rep); err != nil {
				t.Fatalf("record %d: %v", i, err)
			}
		}
		rows, err := s.History(ctx, a.ID, 3)
		if err != nil || len(rows) != 3 {
			t.Fatalf("history = %v, %v", rows, err)
		}
		for i, want := range []string{"room-4", "room-3", "room-2"} {
			if rows[i].MatchID != want {
				t.Fatalf("history[%d] = %s, want %s", i, rows[i].MatchID, want)
			}
		}
		if rows[0].Mode != "turn" || rows[0].RatingAfter != 1004 {
			t.Fatalf("history row lost its detail: %+v", rows[0])
		}
	})
}

func TestLeaderboardRanksOnlyPlayersAndBreaksTiesTotally(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		high := mkAccount(t, s, "high")
		tieA := mkAccount(t, s, "tiea")
		tieB := mkAccount(t, s, "tieb")
		mkAccount(t, s, "neverplayed")

		record := func(a Account, matchID string, after int) {
			t.Helper()
			if _, err := svc.RecordMatch(ctx, Report{
				MatchID: matchID, Mode: "arena", EndedAt: time.Unix(1_700_000_000, 0),
				Results: []Result{{AccountID: a.ID, Score: 1, Placement: 1, RatingBefore: 1000, RatingAfter: after}},
			}); err != nil {
				t.Fatalf("record: %v", err)
			}
		}
		record(high, "m-high", 1200)
		record(tieA, "m-a", 1100)
		record(tieB, "m-b", 1100)

		board, err := svc.Leaderboard(ctx, 10)
		if err != nil {
			t.Fatalf("leaderboard: %v", err)
		}
		if len(board) != 3 {
			t.Fatalf("board has %d rows, want 3 (an unplayed account is not on the ladder): %+v", len(board), board)
		}
		if board[0].AccountID != high.ID || board[0].Rank != 1 || board[0].Rating != 1200 {
			t.Fatalf("top = %+v", board[0])
		}
		if board[1].Rank != 2 || board[2].Rank != 3 {
			t.Fatalf("ranks are not consecutive: %+v", board)
		}
		lo, hi := tieA.ID, tieB.ID
		if lo > hi {
			lo, hi = hi, lo
		}
		if board[1].AccountID != lo || board[2].AccountID != hi {
			t.Fatalf("tie was not broken by account id: %+v", board)
		}
		if board[0].DisplayName != high.DisplayName {
			t.Fatalf("display name = %q, want %q", board[0].DisplayName, high.DisplayName)
		}
	})
}

func TestNoteLoginIsVisibleOnTheAccount(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		a := mkAccount(t, s, "returning")
		at := time.Unix(1_700_000_000, 0).UTC()
		if err := s.NoteLogin(ctx, a.ID, at); err != nil {
			t.Fatalf("note: %v", err)
		}
		got, err := s.AccountByID(ctx, a.ID)
		if err != nil {
			t.Fatalf("by id: %v", err)
		}
		if !got.LastLoginAt.Equal(at) {
			t.Fatalf("last login = %v, want %v", got.LastLoginAt, at)
		}
	})
}

// The balance check lives in the UPDATE's WHERE clause precisely so this case
// works. Read-then-write loses it: every goroutine reads 250, every one decides
// 60 is affordable, and five of them spend the same coin.
//
// Exactly four purchases of 60 fit in 250, whatever order they arrive in.
func TestConcurrentSpendingCannotOverdraw(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		a := mkAccount(t, s, "contended")

		const n = 12
		var wg sync.WaitGroup
		results := make([]error, n)
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				// A distinct key each: these are twelve different purchases
				// racing, not one purchase retried.
				_, results[i] = svc.Buy(ctx, a.ID, "boost.xp", 1, fmt.Sprintf("buy-%d", i))
			}(i)
		}
		close(start)
		wg.Wait()

		ok, refused := 0, 0
		for i, err := range results {
			switch {
			case err == nil:
				ok++
			case errors.Is(err, ErrInsufficientFunds):
				refused++
			default:
				t.Fatalf("purchase %d: %v", i, err)
			}
		}
		if want := int(StartingCurrency / 80); ok != want || refused != n-want {
			t.Fatalf("%d succeeded and %d refused, want %d and %d", ok, refused, want, n-want)
		}
		p, err := s.Profile(ctx, a.ID)
		if err != nil {
			t.Fatalf("profile: %v", err)
		}
		if p.Currency != StartingCurrency-int64(ok)*80 {
			t.Fatalf("balance = %d after %d purchases", p.Currency, ok)
		}
		items, _ := s.Inventory(ctx, a.ID)
		if len(items) != 1 || items[0].Qty != ok {
			t.Fatalf("inventory = %v, want %d of boost.xp", items, ok)
		}
	})
}

// A rating is applied as a delta, not assigned.
//
// The value in a result was read when the match started and is written when it
// ends, minutes later. Assigning it means a match that finished in between is
// silently undone — the player wins one and loses one and ends up where the
// slower match said they were. Adding deltas composes, whatever order the two
// writers arrive in.
func TestRatingsComposeAcrossOverlappingMatches(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		a := mkAccount(t, s, "overlapped")

		// Both matches read 1000 on the way in. The first to finish gains 20;
		// the second, which started earlier and is still holding the old
		// reading, loses 10.
		record := func(id string, before, after int) {
			t.Helper()
			if _, err := svc.RecordMatch(ctx, Report{
				MatchID: id, Mode: "arena", EndedAt: time.Unix(1_700_000_000, 0),
				Results: []Result{{AccountID: a.ID, Score: 1, Placement: 1, RatingBefore: before, RatingAfter: after}},
			}); err != nil {
				t.Fatalf("record %s: %v", id, err)
			}
		}
		record("fast", 1000, 1020)
		record("slow", 1000, 990)

		p, err := s.Profile(ctx, a.ID)
		if err != nil {
			t.Fatalf("profile: %v", err)
		}
		if p.Rating != 1010 {
			t.Fatalf("rating = %d, want 1010 (+20 then -10); an assignment would leave 990", p.Rating)
		}
	})
}

// A losing streak stops at the ladder floor rather than running the number into
// the ground, where it stops meaning anything and the player can never be
// matched with anybody.
func TestRatingStopsAtTheFloor(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		a := mkAccount(t, s, "tumbling")

		if _, err := svc.RecordMatch(ctx, Report{
			MatchID: "rout", Mode: "arena", EndedAt: time.Unix(1_700_000_000, 0),
			Results: []Result{{AccountID: a.ID, Placement: 8, RatingBefore: 1000, RatingAfter: -5000}},
		}); err != nil {
			t.Fatalf("record: %v", err)
		}
		p, _ := s.Profile(ctx, a.ID)
		if p.Rating != rating.Floor {
			t.Fatalf("rating = %d, want the floor %d", p.Rating, rating.Floor)
		}
	})
}

// Two divergences the suite did not reach until they were looked for, both
// found by asking what each implementation does with an input nothing sends
// yet. Neither could have produced a live bug today — Login only stamps an
// account it just authenticated, and no caller applies an entry that names an
// item and moves none of it — and that is the reason to pin them rather than
// shrug: an untested difference between two stores selected by one environment
// variable is a difference that surfaces the day something new calls them, in
// whichever of the two is running in production.
func TestTheTwoStoresAgreeOnCallsNothingMakesYet(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()

		// An UPDATE that matched nothing is not success. SQL reports it as one.
		if err := s.NoteLogin(ctx, "a-nobody", time.Unix(1_700_000_000, 0)); !errors.Is(err, ErrNoAccount) {
			t.Errorf("NoteLogin for an unknown account = %v, want ErrNoAccount", err)
		}

		// A currency movement that still names an item — a refund, or a reward
		// paid against something somebody owns. Nothing moves in the inventory,
		// and the receipt still has to say where that item stands: a Qty of 0
		// reads as "you own none" to a caller who owns three.
		svc := NewService(Options{Store: s, Iters: 1})
		a := mkAccount(t, s, "receipted")
		if _, err := svc.Buy(ctx, a.ID, "boost.xp", 3, "stock-up"); err != nil {
			t.Fatalf("buy: %v", err)
		}
		rec, err := s.Apply(ctx, Entry{
			AccountID: a.ID, Key: "refund", Kind: "grant",
			ItemID: "boost.xp", Qty: 0, Delta: 10, At: time.Unix(1_700_000_000, 0),
		})
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
		if rec.Item.Qty != 3 {
			t.Errorf("Receipt.Item.Qty = %d, want the 3 they hold", rec.Item.Qty)
		}
		if rec.Item.AcquiredAt.IsZero() {
			t.Error("Receipt.Item lost the acquisition time of a stack that still exists")
		}
		if rec.Profile.Currency != StartingCurrency-3*80+10 {
			t.Errorf("balance = %d", rec.Profile.Currency)
		}
		// And an item the account does not hold reports zero rather than
		// failing: there is nothing wrong with a receipt for a stack of none.
		rec, err = s.Apply(ctx, Entry{
			AccountID: a.ID, Key: "refund-2", Kind: "grant",
			ItemID: "emote.gg", Qty: 0, Delta: 5, At: time.Unix(1_700_000_000, 0),
		})
		if err != nil || rec.Item.Qty != 0 || rec.Item.ItemID != "emote.gg" {
			t.Errorf("receipt for an unheld item = %+v (%v)", rec.Item, err)
		}
	})
}

// ---------------------------------------------------------------------------
// RatingsFor
//
// The batched read a finished match uses. Rating answers one player and is what
// a queue join does; this answers a whole room, because recording a result had
// every seat in hand at once and was still spending a query per player — on the
// room's own goroutine, before the room could be counted as finished.

func TestRatingsForAgreesWithRatingOneAccountAtATime(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		a := mkAccount(t, s, "ratings-one")
		b := mkAccount(t, s, "ratings-two")

		got, err := s.RatingsFor(ctx, []string{a.ID, b.ID})
		if err != nil {
			t.Fatalf("RatingsFor: %v", err)
		}
		for _, id := range []string{a.ID, b.ID} {
			want, err := s.Rating(ctx, id)
			if err != nil {
				t.Fatalf("Rating(%s): %v", id, err)
			}
			if got[id] != want {
				t.Errorf("RatingsFor[%s] = %d, Rating says %d", id, got[id], want)
			}
		}
	})
}

// An id with no account is left out of the map rather than reported. Rating
// returns ErrNoAccount because a caller asking about one player wants to know;
// a caller recording a match has bots and unauthenticated sessions among its
// seats by construction, and one of those must not fail the whole read.
func TestRatingsForOmitsIdsWithNoAccount(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		a := mkAccount(t, s, "ratings-real")

		got, err := s.RatingsFor(ctx, []string{a.ID, "p-not-an-account", "bot-1000"})
		if err != nil {
			t.Fatalf("RatingsFor: %v", err)
		}
		if _, ok := got[a.ID]; !ok {
			t.Errorf("the real account is missing from %v", got)
		}
		if len(got) != 1 {
			t.Errorf("RatingsFor returned %v; want only the real account", got)
		}
	})
}

// It has to see what RecordMatch wrote, not a number cached from before it.
func TestRatingsForReflectsARecordedMatch(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		win := mkAccount(t, s, "ratings-winner")
		lose := mkAccount(t, s, "ratings-loser")

		if _, err := s.RecordMatch(ctx, []MatchRow{
			{MatchID: "m-ratings", AccountID: win.ID, Mode: "arena", Score: 5, Placement: 1,
				RatingBefore: StartingRating, RatingAfter: StartingRating + 20, EndedAt: time.Now().UTC()},
			{MatchID: "m-ratings", AccountID: lose.ID, Mode: "arena", Score: 1, Placement: 2,
				RatingBefore: StartingRating, RatingAfter: StartingRating - 20, EndedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatalf("RecordMatch: %v", err)
		}

		got, err := s.RatingsFor(ctx, []string{win.ID, lose.ID})
		if err != nil {
			t.Fatalf("RatingsFor: %v", err)
		}
		if got[win.ID] != StartingRating+20 {
			t.Errorf("winner = %d, want %d", got[win.ID], StartingRating+20)
		}
		if got[lose.ID] != StartingRating-20 {
			t.Errorf("loser = %d, want %d", got[lose.ID], StartingRating-20)
		}
	})
}

func TestRatingsForOfNothingIsAnEmptyMapNotAnError(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		got, err := s.RatingsFor(context.Background(), nil)
		if err != nil {
			t.Fatalf("RatingsFor(nil): %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("RatingsFor(nil) returned %v", got)
		}
	})
}

// The rating.Store view the matchmaker actually holds. Positional where the
// store's own map is not, and rating.Default where the store had nothing —
// which is the contract every other rating.Store implementation follows.
func TestRatingsAdapterAnswersPositionallyWithDefaults(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		a := mkAccount(t, s, "adapter-one")
		b := mkAccount(t, s, "adapter-two")

		var store rating.Store = NewService(Options{Store: s, Iters: 1}).Ratings()
		ids := []string{b.ID, "bot-1000", a.ID, b.ID}
		got, err := store.GetMany(ctx, ids)
		if err != nil {
			t.Fatalf("GetMany: %v", err)
		}
		want := []int{StartingRating, rating.Default, StartingRating, StartingRating}
		if len(got) != len(want) {
			t.Fatalf("GetMany returned %d values for %d ids", len(got), len(ids))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("GetMany%v = %v, want %v", ids, got, want)
			}
		}
	})
}
