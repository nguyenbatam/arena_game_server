package platform

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/rating"
)

// A whole room in one call.
//
// The Postgres store writes a match as two set-based statements rather than two
// per player, so the interesting case is no longer one row: it is a full room
// with unusable seats mixed into it, which has to leave the usable ones exactly
// as a per-row loop would.
func TestRecordMatchWritesAWholeRoomAtOnce(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})

		const players = 8
		accounts := make([]Account, players)
		results := make([]Result, 0, players+2)
		for i := range accounts {
			accounts[i] = mkAccount(t, s, fmt.Sprintf("room%02d", i))
			results = append(results, Result{
				AccountID: accounts[i].ID, Score: players - i, Placement: i + 1,
				RatingBefore: 1000, RatingAfter: 1000 + (players/2-i)*4,
			})
		}
		// Two seats with nothing behind them: a bot and a deleted player. They
		// must not be counted and must not cost anybody else their result.
		results = append(results,
			Result{AccountID: "a-bot", Score: 1, Placement: players + 1, RatingBefore: 1000, RatingAfter: 1000},
			Result{AccountID: "", Score: 0, Placement: players + 2},
		)

		rep := Report{MatchID: "big-1", Mode: "arena", EndedAt: time.Unix(1_700_000_500, 0), Results: results}
		n, err := svc.RecordMatch(ctx, rep)
		if err != nil || n != players {
			t.Fatalf("record = %d, %v; want %d", n, err, players)
		}

		want := make([]Profile, players)
		for i, a := range accounts {
			p, err := s.Profile(ctx, a.ID)
			if err != nil {
				t.Fatalf("profile %s: %v", a.ID, err)
			}
			r := results[i]
			if p.Matches != 1 || p.Kills != r.Score || p.Rating != r.RatingAfter {
				t.Fatalf("player %d profile = %+v, want matches=1 kills=%d rating=%d",
					i, p, r.Score, r.RatingAfter)
			}
			wins := 0
			if r.Placement == 1 {
				wins = 1
			}
			if p.Wins != wins {
				t.Fatalf("player %d wins = %d, want %d", i, p.Wins, wins)
			}
			if p.Currency != StartingCurrency+DefaultRewards.Payout(r) {
				t.Fatalf("player %d balance = %d", i, p.Currency)
			}
			want[i] = p
		}

		// Redelivered. Nothing may move a second time.
		if n, err := svc.RecordMatch(ctx, rep); err != nil || n != 0 {
			t.Fatalf("replay = %d, %v; want 0", n, err)
		}
		for i, a := range accounts {
			if got, _ := s.Profile(ctx, a.ID); got != want[i] {
				t.Fatalf("player %d moved on replay: %+v -> %+v", i, want[i], got)
			}
		}
	})
}

// A retry that carries some rows already written and some not writes exactly
// the missing ones and moves exactly those profiles — the property that keying
// on (match, account) buys over a single "seen this match" flag, now that the
// write is one statement for the whole set.
func TestRecordMatchCompletesAPartialRoom(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})

		a := mkAccount(t, s, "partone")
		b := mkAccount(t, s, "parttwo")
		c := mkAccount(t, s, "partthree")

		res := []Result{
			{AccountID: a.ID, Score: 5, Placement: 1, RatingBefore: 1000, RatingAfter: 1020},
			{AccountID: b.ID, Score: 3, Placement: 2, RatingBefore: 1000, RatingAfter: 1000},
			{AccountID: c.ID, Score: 1, Placement: 3, RatingBefore: 1000, RatingAfter: 980},
		}
		first := Report{MatchID: "part-1", Mode: "arena", Results: res[:1]}
		if n, err := svc.RecordMatch(ctx, first); err != nil || n != 1 {
			t.Fatalf("first write = %d, %v", n, err)
		}

		full := Report{MatchID: "part-1", Mode: "arena", Results: res}
		if n, err := svc.RecordMatch(ctx, full); err != nil || n != 2 {
			t.Fatalf("completion = %d, %v; want exactly the two missing rows", n, err)
		}

		for i, acc := range []Account{a, b, c} {
			p, _ := s.Profile(ctx, acc.ID)
			if p.Matches != 1 {
				t.Fatalf("player %d counted %d matches, want 1", i, p.Matches)
			}
			if p.Rating != res[i].RatingAfter {
				t.Fatalf("player %d rating = %d, want %d", i, p.Rating, res[i].RatingAfter)
			}
		}
	})
}

// One account named twice in a single report is one line and one profile
// movement. Nothing produces this today; the set-based insert is what makes it
// worth pinning, because a per-row loop got it right for free through
// ON CONFLICT and a single statement would not.
func TestRecordMatchAppliesARepeatedAccountOnce(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		acc := mkAccount(t, s, "twiceover")

		rep := Report{MatchID: "dup-1", Mode: "arena", Results: []Result{
			{AccountID: acc.ID, Score: 4, Placement: 1, RatingBefore: 1000, RatingAfter: 1030},
			{AccountID: acc.ID, Score: 9, Placement: 1, RatingBefore: 1000, RatingAfter: 1090},
		}}
		n, err := svc.RecordMatch(ctx, rep)
		if err != nil || n != 1 {
			t.Fatalf("record = %d, %v; want 1", n, err)
		}
		p, _ := s.Profile(ctx, acc.ID)
		if p.Matches != 1 || p.Kills != 4 || p.Rating != 1030 {
			t.Fatalf("profile = %+v; the first line is the one that counts", p)
		}
		rows, _ := s.History(ctx, acc.ID, 10)
		if len(rows) != 1 {
			t.Fatalf("history has %d lines for one match", len(rows))
		}
	})
}

// The ladder floor still applies, and it applies per player inside a batch
// rather than to the batch as a whole.
func TestRecordMatchStopsEachRatingAtTheFloor(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		svc := NewService(Options{Store: s, Iters: 1})
		sinking := mkAccount(t, s, "sinking")
		steady := mkAccount(t, s, "steady")

		rep := Report{MatchID: "floor-1", Mode: "arena", Results: []Result{
			{AccountID: sinking.ID, Score: 0, Placement: 2, RatingBefore: 1000, RatingAfter: -5000},
			{AccountID: steady.ID, Score: 6, Placement: 1, RatingBefore: 1000, RatingAfter: 1040},
		}}
		if n, err := svc.RecordMatch(ctx, rep); err != nil || n != 2 {
			t.Fatalf("record = %d, %v", n, err)
		}
		if p, _ := s.Profile(ctx, sinking.ID); p.Rating != rating.Floor {
			t.Fatalf("rating = %d, want the floor %d", p.Rating, rating.Floor)
		}
		if p, _ := s.Profile(ctx, steady.ID); p.Rating != 1040 {
			t.Fatalf("the floor moved a rating that was nowhere near it: %d", p.Rating)
		}
	})
}
