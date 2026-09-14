package app

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/matchmaking"
)

// countingQueue is a Queue that reports how often a pass asked it to form a
// match. Depth alone cannot tell "stopped" from "spun and put everyone back".
type countingQueue struct {
	matchmaking.Queue
	forms atomic.Int32
}

func (q *countingQueue) TryForm(ctx context.Context, r matchmaking.Rules) (*matchmaking.Match, error) {
	q.forms.Add(1)
	return q.Queue.TryForm(ctx, r)
}

func fillQueue(t *testing.T, a *App, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("p%03d", i)
		if err := a.queue.Enqueue(ctx, &pb.QueuePlayer{
			ConnId: id, PlayerId: id, Name: id, Skill: 1000,
		}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
}

func depth(t *testing.T, a *App) int64 {
	t.Helper()
	n, err := a.queue.Depth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// One pass forms every match the queue can supply, not one.
//
// Forming a single match per 50 ms tick made the matchmaker a throughput
// ceiling rather than a latency one: twenty matches a second per replica, when
// the fleet this repo is sized for is 1250 rooms of eight. A cold queue took a
// minute to seat, and steady-state churn at ninety-second matches sat inside a
// factor of 1.5 of the ceiling with nothing left for a burst.
func TestOneMatchPassFormsEveryMatchTheQueueAllows(t *testing.T) {
	a := testApp(t, time.Minute) // RoomSize 2
	fillQueue(t, a, 8)

	a.matchPass(context.Background())

	if n := depth(t, a); n != 0 {
		t.Fatalf("queue depth after one pass = %d, want 0: the pass stopped after the first match", n)
	}
	// Four matches of two, each placed as a job on this node.
	for i := 0; i < 4; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		job, err := a.jobs.Take(ctx, a.cfg.NodeID)
		cancel()
		if err != nil || job == nil {
			t.Fatalf("job %d: %v (only %d matches were placed)", i, err, i)
		}
		if len(job.Req.Seats) != 2 {
			t.Fatalf("job %d has %d seats, want 2", i, len(job.Req.Seats))
		}
	}
}

// The pass is bounded. Draining the whole queue in one turn would hand Redis a
// script to run per match with everything else waiting behind it.
func TestMatchPassIsCappedPerPass(t *testing.T) {
	a := testApp(t, time.Minute) // RoomSize 2
	const extra = 3
	fillQueue(t, a, 2*(maxFormsPerPass+extra))

	a.matchPass(context.Background())

	if got, want := depth(t, a), int64(2*extra); got != want {
		t.Fatalf("queue depth after one pass = %d, want %d (cap is %d matches)", got, want, maxFormsPerPass)
	}
}

// When the fleet has no room, the pass stops instead of spinning.
//
// createMatch puts the players it could not place back in the queue, so a pass
// that carried on would be handed the same people by the next TryForm and would
// keep going until the cap — one refusal per iteration, each costing a
// placement lookup and a requeue, against a fleet that has already said no.
func TestMatchPassStopsWhenTheFleetIsFull(t *testing.T) {
	a := testApp(t, time.Minute)
	q := &countingQueue{Queue: a.queue}
	a.queue = q
	fillQueue(t, a, 8)

	// Take this node out of the placement pool: PickLeastLoaded now answers
	// "nobody", which is what createMatch turns into a refusal.
	if err := a.registry.Unregister(context.Background(), a.cfg.NodeID); err != nil {
		t.Fatal(err)
	}

	a.matchPass(context.Background())

	if got := q.forms.Load(); got != 1 {
		t.Fatalf("the pass called TryForm %d times against a full fleet, want 1", got)
	}
	if n := depth(t, a); n != 8 {
		t.Fatalf("queue depth = %d, want 8: refused players go back where they came from", n)
	}
}

// A pass whose budget has already run out does no work, rather than firing off
// forms that are certain to fail one at a time.
func TestMatchPassRespectsACancelledContext(t *testing.T) {
	a := testApp(t, time.Minute)
	q := &countingQueue{Queue: a.queue}
	a.queue = q
	fillQueue(t, a, 8)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a.matchPass(ctx)

	if got := q.forms.Load(); got != 0 {
		t.Fatalf("a cancelled pass formed %d matches, want 0", got)
	}
}
