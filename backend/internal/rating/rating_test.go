package rating

import (
	"context"
	"testing"

	"github.com/nguyenbatam/arena_game_server/internal/redistest"
)

func forEachStore(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Run("memory", func(t *testing.T) { fn(t, NewMemory()) })
	t.Run("redis", func(t *testing.T) { fn(t, NewRedis(redistest.Client(t))) })
}

func TestUnratedPlayerStartsAtDefault(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		got, err := s.Get(context.Background(), "nobody")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got != Default {
			t.Fatalf("unrated player = %d, want %d", got, Default)
		}
	})
}

func TestPutThenGet(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		if err := s.Put(ctx, map[string]int{"a": 1234, "b": 900}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		for id, want := range map[string]int{"a": 1234, "b": 900} {
			got, err := s.Get(ctx, id)
			if err != nil {
				t.Fatalf("Get(%s): %v", id, err)
			}
			if got != want {
				t.Fatalf("Get(%s) = %d, want %d", id, got, want)
			}
		}
	})
}

func TestUpdateMovesWinnerUpAndLoserDown(t *testing.T) {
	out := Update([]int{1000, 1000}, []int{5, 2})
	if out[0] <= 1000 {
		t.Fatalf("winner rating %d did not rise", out[0])
	}
	if out[1] >= 1000 {
		t.Fatalf("loser rating %d did not fall", out[1])
	}
	// Equal ratings, so the match is a coin flip and K splits evenly.
	if up, down := out[0]-1000, 1000-out[1]; up != down {
		t.Fatalf("asymmetric swing between equals: +%d / -%d", up, down)
	}
}

func TestDrawLeavesEqualPlayersAlone(t *testing.T) {
	out := Update([]int{1200, 1200}, []int{3, 3})
	if out[0] != 1200 || out[1] != 1200 {
		t.Fatalf("a draw between equals moved ratings: %v", out)
	}
}

// Beating someone far above you is worth more than beating an equal — that is
// the whole point of the expected-score curve.
func TestUpsetIsWorthMoreThanAnExpectedWin(t *testing.T) {
	upset := Update([]int{800, 1600}, []int{5, 1})
	expectedWin := Update([]int{1600, 800}, []int{5, 1})
	if gain, small := upset[0]-800, expectedWin[0]-1600; gain <= small {
		t.Fatalf("upset gained %d, expected win gained %d — the curve is inverted", gain, small)
	}
}

// One match is one match's worth of movement no matter how many people were in
// it. Without the average, an eight-player room would swing ratings seven times
// as hard as a duel.
func TestSwingDoesNotGrowWithRoomSize(t *testing.T) {
	duel := Update([]int{1000, 1000}, []int{9, 0})[0] - 1000

	ratings := make([]int, 8)
	scores := make([]int, 8)
	for i := range ratings {
		ratings[i] = 1000
	}
	scores[0] = 9
	room := Update(ratings, scores)[0] - 1000

	if room > duel {
		t.Fatalf("an 8-player win moved %d and a duel moved %d", room, duel)
	}
}

func TestRatingNeverFallsBelowFloor(t *testing.T) {
	r := []int{Floor, 2500}
	for i := 0; i < 50; i++ {
		r = Update(r, []int{0, 9})
	}
	if r[0] < Floor {
		t.Fatalf("rating fell to %d, below the floor of %d", r[0], Floor)
	}
}

func TestUpdateIgnoresMalformedInput(t *testing.T) {
	if got := Update([]int{1000}, []int{3}); got[0] != 1000 {
		t.Fatalf("a one-player match moved a rating: %v", got)
	}
	if got := Update([]int{1000, 1000}, []int{3}); got[0] != 1000 || got[1] != 1000 {
		t.Fatalf("mismatched scores moved ratings: %v", got)
	}
}

// ---------------------------------------------------------------------------
// GetMany
//
// The write side has taken a whole match at once since the beginning; the read
// side did not, so recording a result cost one round trip per seat, in sequence,
// on the room's own goroutine. These hold the batched read to exactly what a
// loop of Get would have produced — both implementations, same assertions, the
// rule the rest of this repo follows for its Redis twins.

func TestGetManyAgreesWithGetOnePlayerAtATime(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		if err := s.Put(ctx, map[string]int{"a": 1234, "b": 900, "c": 1000}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		ids := []string{"a", "b", "c", "never-played"}

		want := make([]int, len(ids))
		for i, id := range ids {
			v, err := s.Get(ctx, id)
			if err != nil {
				t.Fatalf("Get(%s): %v", id, err)
			}
			want[i] = v
		}

		got, err := s.GetMany(ctx, ids)
		if err != nil {
			t.Fatalf("GetMany: %v", err)
		}
		if len(got) != len(ids) {
			t.Fatalf("GetMany returned %d values for %d ids", len(got), len(ids))
		}
		for i := range ids {
			if got[i] != want[i] {
				t.Errorf("GetMany[%d] (%s) = %d, want %d", i, ids[i], got[i], want[i])
			}
		}
	})
}

// Positional, not sorted and not deduplicated: the caller lines these up
// against its own seats, so entry i has to be the rating of id i whatever the
// order or the repeats.
func TestGetManyAnswersPositionally(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		if err := s.Put(ctx, map[string]int{"zed": 1500, "alpha": 800}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		ids := []string{"zed", "unknown", "alpha", "zed"}
		want := []int{1500, Default, 800, 1500}

		got, err := s.GetMany(ctx, ids)
		if err != nil {
			t.Fatalf("GetMany: %v", err)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("GetMany%v = %v, want %v", ids, got, want)
				break
			}
		}
	})
}

// A bot has no rating by construction and a development session has none
// either, so an unknown id is the ordinary case rather than an error.
func TestGetManyFillsUnknownPlayersWithTheDefault(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		got, err := s.GetMany(context.Background(), []string{"nobody", "nobody-else"})
		if err != nil {
			t.Fatalf("GetMany: %v", err)
		}
		for i, v := range got {
			if v != Default {
				t.Errorf("unknown player %d = %d, want %d", i, v, Default)
			}
		}
	})
}

func TestGetManyOfNothingIsNotAnError(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		got, err := s.GetMany(context.Background(), nil)
		if err != nil {
			t.Fatalf("GetMany(nil): %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("GetMany(nil) returned %d values", len(got))
		}
	})
}

// A store that cannot answer still hands back a usable slice. The caller is
// recording a match that was actually played, and a rating it could not read is
// worth guessing at rather than dropping the result over — the same trade Get
// already makes.
func TestGetManyReturnsDefaultsAlongsideAnError(t *testing.T) {
	rdb := redistest.Client(t)
	_ = rdb.Close() // every command from here on fails
	s := NewRedis(rdb)

	got, err := s.GetMany(context.Background(), []string{"a", "b", "c"})
	if err == nil {
		t.Fatal("a closed client reported no error")
	}
	if len(got) != 3 {
		t.Fatalf("returned %d values with the error, want 3", len(got))
	}
	for i, v := range got {
		if v != Default {
			t.Errorf("value %d = %d, want %d", i, v, Default)
		}
	}
}
