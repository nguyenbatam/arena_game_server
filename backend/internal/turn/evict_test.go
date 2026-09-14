package turn

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Expired matches must be reclaimed by ordinary traffic.
//
// evictExpired is amortised rather than run in full on every write: the walk it
// used to do was proportional to how many matches the process held, under the
// store lock, on the mode's only write path — so running N matches to
// completion cost O(N²) and every mover waited behind it.
//
// A partial sweep only works if it keeps ahead of admissions, which is the
// invariant stated at the constants:
//
//	sweepBatch / sweepEvery > 1
//
// This is what holds it. Fill the store, expire everything, then offer it an
// ordinary trickle of writes and require that the map actually shrinks — if the
// batch and the interval are ever retuned past each other, the leak this
// replaces comes back slower and therefore harder to see.
func TestExpiredMatchesAreEvicted(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	now := time.Now()
	store := NewMemoryStore(TTL{Live: time.Minute, Ended: time.Minute})
	store.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}

	const filled = 200
	for i := 0; i < filled; i++ {
		st := NewState(fmt.Sprintf("old-%03d", i), int64(i), [2]string{alice, bob})
		if _, err := store.Create(ctx, st, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := size(store); got != filled {
		t.Fatalf("store holds %d matches after filling, want %d", got, filled)
	}

	// Everything above is now past its TTL.
	mu.Lock()
	now = now.Add(2 * time.Minute)
	mu.Unlock()

	// An ordinary trickle: each admits one match and is a chance to sweep.
	const fresh = 40
	for i := 0; i < fresh; i++ {
		st := NewState(fmt.Sprintf("new-%03d", i), int64(i), [2]string{alice, bob})
		if _, err := store.Create(ctx, st, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Admissions must not outrun eviction. With sweepBatch/sweepEvery at 8x,
	// forty writes are far more than enough to clear two hundred dead entries;
	// the assertion is deliberately loose so it pins the property rather than
	// the arithmetic of the current constants.
	got := size(store)
	if got > fresh {
		t.Fatalf("store still holds %d matches after %d writes; only the %d live ones should remain — eviction is losing to admission",
			got, fresh, fresh)
	}
}

// A sweep must never drop a match that is still live.
func TestEvictionLeavesLiveMatchesAlone(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(TTL{Live: time.Hour, Ended: time.Hour})
	for i := 0; i < 5*sweepEvery; i++ {
		st := NewState(fmt.Sprintf("live-%03d", i), int64(i), [2]string{alice, bob})
		if _, err := store.Create(ctx, st, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := size(store); got != 5*sweepEvery {
		t.Fatalf("store holds %d live matches, want %d", got, 5*sweepEvery)
	}
	for i := 0; i < 5*sweepEvery; i++ {
		if _, _, err := store.Get(ctx, fmt.Sprintf("live-%03d", i)); err != nil {
			t.Fatalf("live-%03d: %v", i, err)
		}
	}
}

func size(s *MemoryStore) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}
