package turn

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Park is the repair path for a claim that could not be turned into a match:
// the opponent goes back in the slot rather than being left neither playing nor
// waiting.
func TestParkFillsAFreeSlot(t *testing.T) {
	forEachPairing(t, func(t *testing.T, p Pairing) {
		ctx := context.Background()

		ok, err := p.Park(ctx, alice, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("Park declined an empty slot")
		}
		// And the park is a real one: the next arrival is matched with them.
		other, err := p.Claim(ctx, bob, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if other != alice {
			t.Errorf("claimed %q, want the reparked %q", other, alice)
		}
	})
}

// The difference from Claim, and the reason this is its own operation: Park
// must never hand an occupant back. The caller holds no connection to either
// player and could not seat them.
func TestParkDoesNotUnparkAnOccupant(t *testing.T) {
	forEachPairing(t, func(t *testing.T, p Pairing) {
		ctx := context.Background()
		if _, err := p.Claim(ctx, bob, time.Minute); err != nil {
			t.Fatal(err)
		}

		ok, err := p.Park(ctx, alice, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Error("Park claimed an occupied slot")
		}
		// bob is still the one waiting — Park must not have evicted him.
		other, err := p.Claim(ctx, "acct-carol", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if other != bob {
			t.Errorf("claimed %q, want the still-parked %q", other, bob)
		}
	})
}

// Parking over yourself refreshes the park rather than being refused.
func TestParkIsIdempotentForTheSamePlayer(t *testing.T) {
	forEachPairing(t, func(t *testing.T, p Pairing) {
		ctx := context.Background()
		if _, err := p.Park(ctx, alice, time.Minute); err != nil {
			t.Fatal(err)
		}
		ok, err := p.Park(ctx, alice, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Error("Park refused to refresh its own park")
		}
	})
}

// A match must never be left live with no deadline queued for it: nothing else
// counts down in this mode, so it would sit until LiveTTL with one player
// waiting on a turn the other has abandoned.
func TestCreateLeavesNoMatchWhenTheDeadlineCannotBeArmed(t *testing.T) {
	store := NewMemoryStore(TTL{})
	svc := NewService(Options{
		Store:     store,
		Deadlines: failingDeadlines{},
		TurnLimit: time.Minute,
	})

	if _, err := svc.Create(context.Background(), "m1", 7, [2]string{alice, bob}); err == nil {
		t.Fatal("Create reported success with no deadline armed")
	}
	if _, _, err := store.Get(context.Background(), "m1"); !errors.Is(err, ErrNoMatch) {
		t.Errorf("match was written anyway: Get returned %v", err)
	}
}

// A deadline armed for a match that never got written is harmless — the
// sweeper already treats a missing match as the ordinary case, because an entry
// for a played turn is deliberately left to be rejected on pop.
func TestSweepIgnoresADeadlineForAMatchThatWasNeverWritten(t *testing.T) {
	store := NewMemoryStore(TTL{})
	dl := NewMemoryDeadlines()
	svc := NewService(Options{Store: store, Deadlines: dl, TurnLimit: time.Minute})

	if err := dl.Arm(context.Background(), "ghost", 1, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	// Must not panic, and must drain the entry rather than retry it forever.
	svc.sweepOnce(context.Background())

	due, err := dl.PopDue(context.Background(), time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Errorf("the orphan deadline was left queued: %+v", due)
	}
}

type failingDeadlines struct{}

func (failingDeadlines) Arm(context.Context, string, uint32, time.Time) error {
	return errors.New("deadline store down")
}

func (failingDeadlines) PopDue(context.Context, time.Time, int) ([]Due, error) {
	return nil, nil
}
