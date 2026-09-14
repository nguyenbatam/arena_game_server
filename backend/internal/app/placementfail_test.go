package app

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/cluster"
)

// failingRegistry answers every placement with an error, which is what a Redis
// that has stopped taking writes looks like from the matchmaker.
type failingRegistry struct {
	cluster.Registry
}

var errPickFailed = errors.New("registry unavailable")

func (f failingRegistry) PickLeastLoaded(context.Context) (*pb.GameServer, error) {
	return nil, errPickFailed
}

// A placement that errors must put the players back in the queue.
//
// By the time createMatch runs, TryForm has already removed these players from
// the queue — so a path that returns without re-queueing does not retry the
// match later, it drops it. The players stay connected and stay waiting on a
// MATCH_FOUND that nobody will send, for the rest of their session, and nothing
// counts it: the queue is shorter, which is what a successful match looks like.
//
// This was reachable in exactly one way, and the way it was reachable is why it
// went unnoticed: PickLeastLoaded discarded the error from its own reservation
// write, so the only thing that could fail it was a read. Once that error was
// reported, an ordinary Redis blip started losing whole matches.
func TestAFailedPlacementPutsThePlayersBackInTheQueue(t *testing.T) {
	a := testApp(t, time.Minute) // RoomSize 2
	a.registry = failingRegistry{a.registry}
	fillQueue(t, a, 4)

	if got := depth(t, a); got != 4 {
		t.Fatalf("queue depth before the pass = %d, want 4", got)
	}
	a.matchPass(context.Background())

	// Every player is either in a match or in the queue. None started, so all
	// four are owed their place back.
	if got := depth(t, a); got != 4 {
		t.Fatalf("queue depth after a failed placement = %d, want 4 — %d players were dropped", got, 4-got)
	}
}

// The requeue must not cost them their place in line.
//
// Enqueue stamps the current time onto a player whose QueuedAt is zero, so a
// requeue that did not carry the original would send everyone to the back of
// the queue for a failure that was entirely the server's. At the front of a
// busy queue that is the difference between waiting again and never being
// matched at all.
func TestRequeueKeepsTheOriginalQueueTime(t *testing.T) {
	a := testApp(t, time.Minute)
	a.registry = failingRegistry{a.registry}

	ctx := context.Background()
	const early = int64(1_000_000)
	for _, id := range []string{"a", "b"} {
		if err := a.queue.Enqueue(ctx, &pb.QueuePlayer{
			ConnId: id, PlayerId: id, Name: id, Skill: 1000, QueuedAt: early,
		}); err != nil {
			t.Fatal(err)
		}
	}
	a.matchPass(ctx)

	m, err := a.queue.TryForm(ctx, a.matchRules())
	if err != nil {
		t.Fatal(err)
	}
	if m == nil {
		t.Fatal("no match formed from the requeued players")
	}
	for _, p := range m.Players {
		if p.QueuedAt != early {
			t.Fatalf("player %s came back with queued_at %d, want the original %d", p.ConnId, p.QueuedAt, early)
		}
	}
}
