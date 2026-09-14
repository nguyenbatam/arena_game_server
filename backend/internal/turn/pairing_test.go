package turn

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Both implementations must behave identically, for the same reason the two
// Stores must: switching REDIS_ADDR on cannot be allowed to change who gets
// matched with whom.
func forEachPairing(t *testing.T, fn func(t *testing.T, p Pairing)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { fn(t, NewMemoryPairing()) })
	t.Run("redis", func(t *testing.T) { fn(t, NewRedisPairing(redisFor(t))) })
}

func TestPairingParksThenMatches(t *testing.T) {
	forEachPairing(t, func(t *testing.T, p Pairing) {
		ctx := context.Background()

		got, err := p.Claim(ctx, alice, time.Minute)
		if err != nil {
			t.Fatalf("Claim alice: %v", err)
		}
		if got != "" {
			t.Fatalf("first arrival must park, got opponent %q", got)
		}

		// Joining again is a repeated request, not a match against yourself.
		if got, err = p.Claim(ctx, alice, time.Minute); err != nil || got != "" {
			t.Fatalf("re-joining must stay parked, got %q err=%v", got, err)
		}

		got, err = p.Claim(ctx, bob, time.Minute)
		if err != nil {
			t.Fatalf("Claim bob: %v", err)
		}
		if got != alice {
			t.Fatalf("second arrival should be handed alice, got %q", got)
		}

		// The slot is empty again: bob was consumed, not left behind.
		if got, err = p.Claim(ctx, "carol", time.Minute); err != nil || got != "" {
			t.Fatalf("slot should be empty after a match, got %q err=%v", got, err)
		}
	})
}

func TestPairingCancelUnparks(t *testing.T) {
	forEachPairing(t, func(t *testing.T, p Pairing) {
		ctx := context.Background()
		if _, err := p.Claim(ctx, alice, time.Minute); err != nil {
			t.Fatal(err)
		}
		if err := p.Cancel(ctx, alice); err != nil {
			t.Fatalf("Cancel: %v", err)
		}
		got, err := p.Claim(ctx, bob, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("a cancelled park must not be handed out, got %q", got)
		}
	})
}

// A player who leaves long after their park was consumed must not evict
// whoever is parked now — their disconnect arrives late, and by then the slot
// belongs to somebody else.
func TestPairingCancelOnlyOwnPark(t *testing.T) {
	forEachPairing(t, func(t *testing.T, p Pairing) {
		ctx := context.Background()
		if _, err := p.Claim(ctx, alice, time.Minute); err != nil {
			t.Fatal(err)
		}
		// bob takes alice, which empties the slot; carol then parks in it.
		if got, _ := p.Claim(ctx, bob, time.Minute); got != alice {
			t.Fatalf("bob should have been handed alice, got %q", got)
		}
		if got, _ := p.Claim(ctx, "carol", time.Minute); got != "" {
			t.Fatalf("carol should have parked, got %q", got)
		}

		if err := p.Cancel(ctx, alice); err != nil {
			t.Fatal(err)
		}

		got, err := p.Claim(ctx, "dave", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if got != "carol" {
			t.Fatalf("alice's late cancel evicted carol: dave got %q", got)
		}
	})
}

// One parked player must be handed to exactly one arrival. Two callers both
// told they matched with the same person is three people in a two-seat game.
func TestPairingClaimIsExclusive(t *testing.T) {
	forEachPairing(t, func(t *testing.T, p Pairing) {
		ctx := context.Background()
		if _, err := p.Claim(ctx, alice, time.Minute); err != nil {
			t.Fatal(err)
		}

		const arrivals = 8
		var wg sync.WaitGroup
		var mu sync.Mutex
		matched := 0
		for i := 0; i < arrivals; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				got, err := p.Claim(ctx, string(rune('A'+i)), time.Minute)
				if err != nil {
					return
				}
				if got == alice {
					mu.Lock()
					matched++
					mu.Unlock()
				}
			}(i)
		}
		wg.Wait()
		if matched != 1 {
			t.Fatalf("alice was handed to %d arrivals, want exactly 1", matched)
		}
	})
}

func TestMemoryPairingParkExpires(t *testing.T) {
	p := NewMemoryPairing()
	now := time.Now()
	p.now = func() time.Time { return now }
	ctx := context.Background()

	if _, err := p.Claim(ctx, alice, time.Minute); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)

	got, err := p.Claim(ctx, bob, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("an expired park must not be handed out, got %q", got)
	}
}
