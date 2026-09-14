package placement

import (
	"context"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/redistest"
	"github.com/redis/go-redis/v9"
)

func forEachQueue(t *testing.T, fn func(t *testing.T, q Queue)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { fn(t, NewMemory()) })
	t.Run("redis", func(t *testing.T) { fn(t, NewRedis(redistest.Client(t))) })
}

func req(id string) *pb.RoomRequest {
	return &pb.RoomRequest{RoomId: id, Seed: 7, TickRate: 20, MatchTicks: 1800}
}

// Jobs are addressed to one node. A game server must never pick up work meant
// for a different instance.
func TestJobsRouteToTheirOwner(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		if err := q.Enqueue(ctx, "gs-1", req("for-one")); err != nil {
			t.Fatal(err)
		}
		if err := q.Enqueue(ctx, "gs-2", req("for-two")); err != nil {
			t.Fatal(err)
		}

		got, err := q.Take(ctx, "gs-1")
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || got.Req.RoomId != "for-one" {
			t.Fatalf("gs-1 took %+v, want its own job", got)
		}
		if got.Req.Seed != 7 || got.Req.TickRate != 20 || got.Req.MatchTicks != 1800 {
			t.Fatalf("job did not round-trip: %+v", got)
		}

		// The other node's job is untouched.
		other, _ := q.Take(ctx, "gs-2")
		if other == nil || other.Req.RoomId != "for-two" {
			t.Fatalf("gs-2 lost its job: %+v", other)
		}
	})
}

// Both implementations must give the caller control back when there is nothing
// to do. The memory queue used to park forever here while the Redis one polled,
// which is the kind of divergence only a shared test catches.
func TestTakeOnEmptyQueueReturnsWithinTakeWait(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		start := time.Now()
		got, err := q.Take(context.Background(), "idle-node")
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("an empty queue should be empty, not an error: %v", err)
		}
		if got != nil {
			t.Fatalf("got %+v, want nil", got)
		}
		if elapsed > TakeWait*2 {
			t.Fatalf("Take parked for %s, want about %s", elapsed, TakeWait)
		}
	})
}

func TestTakeUnblocksOnContextCancel(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			_, _ = q.Take(ctx, "idle-node")
			close(done)
		}()
		cancel()
		select {
		case <-done:
		case <-time.After(TakeWait * 2):
			t.Fatal("Take ignored a cancelled context")
		}
	})
}

// A job taken by a node that then crashes must not be lost forever.
//
// In-flight timestamps are whole seconds, so rather than sleeping past a
// boundary the test ages the marker directly — deterministic, and it describes
// the real case: a node that took a job and never came back.
func TestReapStaleReturnsAbandonedWork(t *testing.T) {
	rdb := redistest.Client(t)
	q := NewRedis(rdb)
	ctx := context.Background()

	if err := q.Enqueue(ctx, "gs-1", req("abandoned")); err != nil {
		t.Fatal(err)
	}
	taken, err := q.Take(ctx, "gs-1")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v %v", taken, err)
	}

	// Nothing is stale yet.
	n, err := q.ReapStale(ctx, "gs-1", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("reaped %d jobs that were not stale", n)
	}

	ageOut(t, rdb, "abandoned")

	n, err = q.ReapStale(ctx, "gs-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reaped %d, want the one abandoned job", n)
	}

	back, _ := q.Take(ctx, "gs-1")
	if back == nil || back.Req.RoomId != "abandoned" {
		t.Fatalf("reaped job did not return to the queue: %+v", back)
	}
}

// Ack retires a job so reaping cannot resurrect work that actually finished.
func TestAckPreventsReap(t *testing.T) {
	rdb := redistest.Client(t)
	q := NewRedis(rdb)
	ctx := context.Background()

	_ = q.Enqueue(ctx, "gs-1", req("done"))
	taken, _ := q.Take(ctx, "gs-1")
	ageOut(t, rdb, "done")
	if err := q.Ack(ctx, "gs-1", taken); err != nil {
		t.Fatal(err)
	}

	n, err := q.ReapStale(ctx, "gs-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("reaped %d acked jobs — a finished match would be started twice", n)
	}
}

// ageOut backdates a job's in-flight marker so it looks abandoned.
func ageOut(t *testing.T, rdb *redis.Client, roomID string) {
	t.Helper()
	old := time.Now().Add(-10 * time.Minute).Unix()
	if err := rdb.Set(context.Background(), procTSKey(roomID), old, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
}
