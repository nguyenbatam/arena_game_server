package turn

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var errArmFailed = errors.New("deadline store down")

// flakyDeadlines arms happily until it is switched off, which is how a Redis
// that starts refusing writes partway through a match looks from in here.
type flakyDeadlines struct {
	*MemoryDeadlines
	broken atomic.Bool
}

func (d *flakyDeadlines) Arm(ctx context.Context, matchID string, turn uint32, at time.Time) error {
	if d.broken.Load() {
		return errArmFailed
	}
	return d.MemoryDeadlines.Arm(ctx, matchID, turn, at)
}

// A move whose deadline cannot be armed must not be applied.
//
// Create has always refused to leave a match live with nothing counting down —
// TestCreateLeavesNoMatchWhenTheDeadlineCannotBeArmed pins that — but every
// move after the first went the other way: it armed *after* committing and
// discarded the error. The move was durable, the turn had advanced, and the one
// mechanism that would ever have moved it again had failed silently. Whoever
// was on the clock could then simply walk away, and the match would sit there
// until LiveTTL, which is twenty-four hours by default.
//
// So the arm comes first now, and a failure is an error the caller can report
// and the client can retry. The state must be exactly as it was.
func TestAMoveIsNotAppliedWhenItsDeadlineCannotBeArmed(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(TTL{})
	dl := &flakyDeadlines{MemoryDeadlines: NewMemoryDeadlines()}
	svc := NewService(Options{Store: store, Deadlines: dl, TurnLimit: time.Minute})

	st, err := svc.Create(ctx, "m1", 7, [2]string{alice, bob})
	if err != nil {
		t.Fatal(err)
	}
	mover := st.Players[st.Turn]
	card := st.LowestCard(st.Turn)
	before, versionBefore, err := store.Get(ctx, "m1")
	if err != nil {
		t.Fatal(err)
	}

	dl.broken.Store(true)
	if err := svc.Play(ctx, "m1", Move{PlayerID: mover, Card: card, TurnNumber: before.TurnNumber}); err == nil {
		t.Fatal("Play reported success with no deadline armed for the turn it handed over")
	}

	after, versionAfter, err := store.Get(ctx, "m1")
	if err != nil {
		t.Fatal(err)
	}
	if versionAfter != versionBefore {
		t.Errorf("state was committed anyway: version %d became %d", versionBefore, versionAfter)
	}
	if after.TurnNumber != before.TurnNumber {
		t.Errorf("turn advanced on a move that failed: %d became %d", before.TurnNumber, after.TurnNumber)
	}
	if len(after.Hands[st.Turn]) != len(before.Hands[st.Turn]) {
		t.Errorf("the card was spent on a move that failed: hand went from %d to %d",
			len(before.Hands[st.Turn]), len(after.Hands[st.Turn]))
	}

	// And the match is still playable once the store recovers — the failure
	// cost a move, not the game.
	dl.broken.Store(false)
	if err := svc.Play(ctx, "m1", Move{PlayerID: mover, Card: card, TurnNumber: before.TurnNumber}); err != nil {
		t.Fatalf("retry after recovery failed: %v", err)
	}
}
