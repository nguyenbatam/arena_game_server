package platform

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nguyenbatam/arena_game_server/internal/rating"
)

func TestPasswordsAreSaltedAndNeverStoredInTheClear(t *testing.T) {
	s := NewService(Options{Store: NewMemory(), Iters: 2})
	a, err := s.hash("correct-horse")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	b, err := s.hash("correct-horse")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if string(a.Hash) == string(b.Hash) {
		t.Fatal("the same password hashed to the same digest twice: the salt is not random")
	}
	if strings.Contains(string(a.Hash), "correct-horse") {
		t.Fatal("the password is recoverable from the digest")
	}
	if !verify("correct-horse", a) {
		t.Fatal("the right password did not verify")
	}
	if verify("correct-hors", a) {
		t.Fatal("the wrong password verified")
	}
	// The cost travels with the credential, so a row written at the old work
	// factor still verifies after the constant moves.
	if a.Iters != 2 {
		t.Fatalf("iters = %d, want the service's", a.Iters)
	}
	old := Credential{Hash: a.Hash, Salt: a.Salt, Iters: a.Iters}
	if !NewService(Options{Store: NewMemory(), Iters: 500}).storeVerify("correct-horse", old) {
		t.Fatal("a credential written at an older cost stopped verifying when the constant moved")
	}
}

func TestRegistrationRejectsWhatCannotBeAKey(t *testing.T) {
	s := NewService(Options{Store: NewMemory(), Iters: 1})
	ctx := context.Background()
	for _, tc := range []struct{ name, user, pass string }{
		{"short username", "ab", "correct-horse"},
		{"long username", strings.Repeat("a", 33), "correct-horse"},
		{"space in username", "two words", "correct-horse"},
		{"unicode lookalike", "pilоt", "correct-horse"}, // that "о" is Cyrillic
		{"short password", "pilot", "short"},
	} {
		if _, err := s.Register(ctx, tc.user, tc.pass, ""); err == nil {
			t.Fatalf("%s: accepted", tc.name)
		}
	}
	if _, err := s.Register(ctx, "pilot", "correct-horse", ""); err != nil {
		t.Fatalf("valid registration rejected: %v", err)
	}
}

func TestDisplayNameDefaultsToTheUsername(t *testing.T) {
	s := NewService(Options{Store: NewMemory(), Iters: 1})
	a, err := s.Register(context.Background(), "pilot", "correct-horse", "   ")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if a.DisplayName != "pilot" {
		t.Fatalf("display name = %q", a.DisplayName)
	}
}

func TestPayoutRewardsPlayingWinningAndScoringWithinACap(t *testing.T) {
	r := DefaultRewards
	loss := r.Payout(Result{Placement: 4})
	if loss != r.Participation {
		t.Fatalf("a loss paid %d, want the participation reward %d", loss, r.Participation)
	}
	win := r.Payout(Result{Placement: 1})
	if win <= loss {
		t.Fatalf("a win paid %d, no more than a loss at %d", win, loss)
	}
	// The cap is what bounds a bug, an exploit or a bot farm: a score the game
	// counted wrong should cost a bounded amount of currency.
	if v := r.Payout(Result{Placement: 1, Score: 1_000_000}); v != r.Cap {
		t.Fatalf("an absurd score paid %d, want the cap %d", v, r.Cap)
	}
}

// The matchmaker reads ratings through rating.Store and does not know or care
// that a database is behind it — which is the whole claim the seam makes.
func TestRatingsAdapterServesTheMatchmaker(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	svc := NewService(Options{Store: mem, Iters: 1})
	var store rating.Store = svc.Ratings()

	a, err := svc.Register(ctx, "ranked", "correct-horse", "")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if v, err := store.Get(ctx, a.ID); err != nil || v != rating.Default {
		t.Fatalf("new account rated %d (%v), want %d", v, err, rating.Default)
	}

	// A bot, or a development-mode connection with no token at all. Neither can
	// be a reason a queue join fails.
	if v, err := store.Get(ctx, "bot-7"); err != nil || v != rating.Default {
		t.Fatalf("unknown id = %d, %v; want the default and no error", v, err)
	}

	if _, err := svc.RecordMatch(ctx, Report{
		MatchID: "m1", Mode: "arena",
		Results: []Result{{AccountID: a.ID, Score: 3, Placement: 1, RatingBefore: 1000, RatingAfter: 1030}},
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if v, _ := store.Get(ctx, a.ID); v != 1030 {
		t.Fatalf("rating after the match = %d, want 1030", v)
	}

	// Put is closed: ratings move only through RecordMatch, so the number and
	// the history that explains it cannot drift apart.
	if err := store.Put(ctx, map[string]int{a.ID: 9999}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if v, _ := store.Get(ctx, a.ID); v != 1030 {
		t.Fatalf("Put moved the rating to %d", v)
	}
}

func TestBuyRejectsWhatTheCatalogAndTheKeyRulesDoNot(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	svc := NewService(Options{Store: mem, Iters: 1})
	a, _ := svc.Register(ctx, "shopper", "correct-horse", "")

	if _, err := svc.Buy(ctx, a.ID, "does.not.exist", 1, "k"); !errors.Is(err, ErrNoItem) {
		t.Fatalf("unknown item: %v", err)
	}
	if _, err := svc.Buy(ctx, a.ID, "emote.gg", 1, "  "); err == nil {
		t.Fatal("a purchase with no idempotency key was accepted")
	}
	if _, err := svc.Buy(ctx, a.ID, "emote.gg", 1, strings.Repeat("k", 65)); err == nil {
		t.Fatal("an unbounded idempotency key was accepted")
	}
	if _, err := svc.Buy(ctx, a.ID, "boost.xp", 1000, "k"); err == nil {
		t.Fatal("an unbounded quantity was accepted")
	}
}

func TestGrantMovesItemsAsWellAsCurrency(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	svc := NewService(Options{Store: mem, Iters: 1})
	a, err := svc.Register(ctx, "granted", "correct-horse", "")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Grant and Buy are one primitive with the sign of the currency flipped,
	// which is the reason there is only one transaction to keep honest.
	rec, err := svc.Grant(ctx, a.ID, "emote.gg", 1, 100, "award-launch")
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if rec.Item.Qty != 1 || rec.Profile.Currency != StartingCurrency+100 {
		t.Fatalf("grant = %+v", rec)
	}
	// The ceiling applies to a gift as much as to a purchase: a unique cosmetic
	// granted twice is still two of a thing that only comes as one.
	if _, err := svc.Grant(ctx, a.ID, "emote.gg", 1, 0, "award-again"); !errors.Is(err, ErrItemLimit) {
		t.Fatalf("second grant: want ErrItemLimit, got %v", err)
	}
	if _, err := svc.Grant(ctx, a.ID, "no.such.item", 1, 0, "award-bogus"); !errors.Is(err, ErrNoItem) {
		t.Fatalf("unknown item: want ErrNoItem, got %v", err)
	}
	// A server-chosen key is stable across retries by construction.
	replay, err := svc.Grant(ctx, a.ID, "emote.gg", 1, 100, "award-launch")
	if err != nil || !replay.Replay {
		t.Fatalf("replayed grant = %+v (%v)", replay, err)
	}
	if replay.Profile.Currency != StartingCurrency+100 {
		t.Fatalf("the replayed grant paid twice: %d", replay.Profile.Currency)
	}
}

func TestRecordMatchIgnoresAReportWithNothingInIt(t *testing.T) {
	ctx := context.Background()
	svc := NewService(Options{Store: NewMemory(), Iters: 1})

	// A room that ended with nobody in it, or a report whose seats were all
	// bots. Neither is an error — there is simply nothing to write.
	for _, rep := range []Report{
		{},
		{MatchID: "m", Mode: "arena"},
		{MatchID: "", Mode: "arena", Results: []Result{{AccountID: "a-1"}}},
		{MatchID: "m", Mode: "arena", Results: []Result{{AccountID: ""}, {AccountID: ""}}},
	} {
		n, err := svc.RecordMatch(ctx, rep)
		if err != nil || n != 0 {
			t.Fatalf("RecordMatch(%+v) = %d, %v; want 0 and no error", rep, n, err)
		}
	}
}

func TestPasswordRulesRejectWhatCannotBeTyped(t *testing.T) {
	// Length is the property that matters. Composition rules ("one digit, one
	// symbol") shrink the space a user actually explores and are why every
	// password is Password1!, so they are deliberately absent — but a control
	// character is not a password somebody meant to type, and an unbounded one
	// is an anonymous caller choosing how much PBKDF2 this process runs.
	for _, tc := range []struct{ name, pw string }{
		{"too short", "short"},
		{"embedded newline", "correct\nhorse"},
		{"embedded NUL", "correct\x00horse"},
		{"too long", strings.Repeat("a", 257)},
	} {
		err := ValidPassword(tc.pw)
		if err == nil {
			t.Fatalf("%s: accepted", tc.name)
		}
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s: %v does not wrap ErrInvalid, so HTTP would answer 500", tc.name, err)
		}
	}
	if err := ValidPassword("correct-horse-battery"); err != nil {
		t.Fatalf("a good password was rejected: %v", err)
	}
	// Unicode counts as runes, not bytes: a passphrase of emoji is eight
	// characters long even though it is thirty-two bytes.
	if err := ValidPassword(strings.Repeat("🐎", 8)); err != nil {
		t.Fatalf("a multi-byte passphrase was measured in bytes: %v", err)
	}
}

func TestUsernameErrorsWrapErrInvalid(t *testing.T) {
	// Same contract as passwords: the HTTP layer answers 400 by unwrapping, not
	// by matching on message text.
	for _, u := range []string{"ab", strings.Repeat("a", 33), "two words", "semi;colon"} {
		if err := ValidUsername(u); !errors.Is(err, ErrInvalid) {
			t.Fatalf("ValidUsername(%q) = %v, which does not wrap ErrInvalid", u, err)
		}
	}
}

func TestAnEmptyCredentialNeverVerifies(t *testing.T) {
	// A zero Credential is what a bug or a half-written row looks like, and the
	// one thing it must not be is a password that matches everything. PBKDF2
	// with zero iterations is an error in the standard library, but the guard
	// is here rather than relying on that.
	for _, c := range []Credential{
		{},
		{Iters: 10},                // no hash
		{Hash: []byte("x")},        // no cost
		{Hash: []byte{}, Iters: 1}, // empty hash
	} {
		if verify("", c) || verify("anything", c) {
			t.Fatalf("credential %+v verified", c)
		}
	}
}

func TestCatalogListsEverythingInAStableOrder(t *testing.T) {
	c := DefaultCatalog()
	got := c.List()
	if len(got) != len(c) {
		t.Fatalf("List returned %d of %d items", len(got), len(c))
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].ID >= got[i].ID {
			t.Fatalf("List is not sorted by id: %v then %v", got[i-1].ID, got[i].ID)
		}
	}
	// Map iteration is randomised, so an unsorted List would reshuffle the
	// store between two page loads.
	for i := 0; i < 20; i++ {
		again := c.List()
		for j := range got {
			if again[j].ID != got[j].ID {
				t.Fatalf("List reordered between calls: %v vs %v", got, again)
			}
		}
	}
	for _, it := range got {
		if it.Price <= 0 || it.Name == "" || it.Kind == "" {
			t.Fatalf("catalog entry is not sellable: %+v", it)
		}
	}
}

func TestNewPostgresRefusesAMalformedDSN(t *testing.T) {
	// Worth pinning because the failure is otherwise silent in a useful way:
	// sql.Open is lazy, so a store built from a bad DSN would come back
	// non-nil and fail on the first query instead of at startup. The Ping is
	// what turns a typo into a refused boot.
	s, err := NewPostgres(context.Background(), "postgres://user@nowhere.invalid:1/db?sslmode=disable&connect_timeout=1",
		PostgresOptions{MaxConns: 1})
	if err == nil {
		_ = s.Close()
		t.Fatal("an unreachable DSN produced a working store")
	}
	if s != nil {
		t.Fatal("a failed constructor returned a non-nil store")
	}
}
