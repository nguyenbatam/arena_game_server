package cluster

import (
	"context"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/redistest"
)

// Placement runs on the matchmaker's 50ms ticker. Reading inflight with a GET
// per candidate turns a fixed cost into one that grows with the fleet; it is
// read in a single MGet instead. This pins the behaviour that batching must
// preserve.
func TestPickAccountsForInflightAcrossTheFleet(t *testing.T) {
	rdb := redistest.Client(t)
	r := NewRedis(rdb)
	ctx := context.Background()

	for _, id := range []string{"gs-a", "gs-b", "gs-c"} {
		if err := r.Heartbeat(ctx, &pb.GameServer{Id: id, PublicAddr: id + ":1", Rooms: 5}); err != nil {
			t.Fatal(err)
		}
	}
	// Equal room counts: inflight is the only thing separating them.
	rdb.Set(ctx, inflightKey("gs-a"), 4, time.Minute)
	rdb.Set(ctx, inflightKey("gs-b"), 1, time.Minute)
	rdb.Set(ctx, inflightKey("gs-c"), 7, time.Minute)

	got, err := r.PickLeastLoaded(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Id != "gs-b" {
		t.Fatalf("picked %s, want gs-b (lowest rooms+inflight)", got.Id)
	}
}

// A server with no inflight key at all must read as zero, not be skipped.
func TestMissingInflightCountsAsZero(t *testing.T) {
	rdb := redistest.Client(t)
	r := NewRedis(rdb)
	ctx := context.Background()

	if err := r.Heartbeat(ctx, &pb.GameServer{Id: "gs-busy", Rooms: 2}); err != nil {
		t.Fatal(err)
	}
	if err := r.Heartbeat(ctx, &pb.GameServer{Id: "gs-idle", Rooms: 0}); err != nil {
		t.Fatal(err)
	}
	rdb.Set(ctx, inflightKey("gs-busy"), 0, time.Minute)
	// gs-idle deliberately has no inflight key.

	got, err := r.PickLeastLoaded(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Id != "gs-idle" {
		t.Fatalf("picked %s, want gs-idle", got.Id)
	}
}

// Equal load must break to the lowest id, so placement is stable rather than
// dependent on map order.
func TestPickTieBreaksOnID(t *testing.T) {
	rdb := redistest.Client(t)
	r := NewRedis(rdb)
	ctx := context.Background()
	for _, id := range []string{"gs-z", "gs-a", "gs-m"} {
		if err := r.Heartbeat(ctx, &pb.GameServer{Id: id, Rooms: 3}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		got, err := r.PickLeastLoaded(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got.Id != "gs-a" {
			t.Fatalf("attempt %d picked %s, want the lowest id gs-a", i, got.Id)
		}
		if err := r.ReleaseInflight(ctx, got.Id); err != nil {
			t.Fatal(err)
		}
	}
}

// Heartbeat must not stamp the caller's message: the heartbeat loop reuses one
// GameServer across ticks and is not expecting the call to write into it.
func TestHeartbeatDoesNotMutateItsArgument(t *testing.T) {
	rdb := redistest.Client(t)
	r := NewRedis(rdb)
	gs := &pb.GameServer{Id: "gs-1", PublicAddr: "a:1", Rooms: 2}
	if err := r.Heartbeat(context.Background(), gs); err != nil {
		t.Fatal(err)
	}
	if gs.UpdatedAt != 0 {
		t.Fatalf("Heartbeat wrote UpdatedAt=%d into the caller's message", gs.UpdatedAt)
	}
}

// Reserving must survive being released more times than it was taken, or a
// double release parks a node at a permanently negative load.
func TestReleaseNeverGoesNegative(t *testing.T) {
	rdb := redistest.Client(t)
	r := NewRedis(rdb)
	ctx := context.Background()
	if err := r.Heartbeat(ctx, &pb.GameServer{Id: "gs-1", Rooms: 0}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := r.ReleaseInflight(ctx, "gs-1"); err != nil {
			t.Fatal(err)
		}
	}
	n, err := rdb.Get(ctx, inflightKey("gs-1")).Int64()
	if err != nil {
		t.Fatal(err)
	}
	if n < 0 {
		t.Fatalf("inflight settled at %d", n)
	}
}

// A reservation has to outlive the heartbeat that follows it. The heartbeat
// reports the rooms a node has actually started, which does not yet include the
// one just placed on it — so a reservation folded into that number is erased a
// couple of seconds later, and the matchmaker hands the same node every match
// in the burst. The memory twin did exactly that until it grew its own counter.
func TestInflightSurvivesHeartbeat(t *testing.T) {
	forEachRegistry(t, func(t *testing.T, r Registry) {
		ctx := context.Background()
		for _, id := range []string{"a", "b"} {
			if err := r.Heartbeat(ctx, &pb.GameServer{Id: id, PublicAddr: "ws://" + id + "/ws"}); err != nil {
				t.Fatal(err)
			}
		}

		first, err := r.PickLeastLoaded(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// The node it picked heartbeats again before that room is running.
		if err := r.Heartbeat(ctx, &pb.GameServer{Id: first.Id, PublicAddr: "ws://" + first.Id + "/ws"}); err != nil {
			t.Fatal(err)
		}

		second, err := r.PickLeastLoaded(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if second.Id == first.Id {
			t.Fatalf("both matches went to %s: the heartbeat erased the reservation", first.Id)
		}
	})
}

// Releasing more often than reserving must not park a node at a negative load,
// or it collects every match in the fleet from then on.
func TestReleaseNeverGoesNegativeInBothRegistries(t *testing.T) {
	forEachRegistry(t, func(t *testing.T, r Registry) {
		ctx := context.Background()
		for _, id := range []string{"a", "b"} {
			if err := r.Heartbeat(ctx, &pb.GameServer{Id: id, PublicAddr: "ws://" + id + "/ws"}); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 3; i++ {
			if err := r.ReleaseInflight(ctx, "a"); err != nil {
				t.Fatal(err)
			}
		}
		// "a" is owed nothing, so an equal load must still break on id, and the
		// next placement must move on to "b".
		if got, err := r.PickLeastLoaded(ctx); err != nil || got.Id != "a" {
			t.Fatalf("first pick = %v (err %v), want a", got, err)
		}
		if got, err := r.PickLeastLoaded(ctx); err != nil || got.Id != "b" {
			t.Fatalf("second pick = %v (err %v), want b: a settled below zero", got, err)
		}
	})
}

// Single-process mode reaches placement through the self fallback, so a
// capacity the node heartbeats has to be visible there too. It was not: the
// fallback kept the record built at startup, which declares no ceiling, and the
// node was handed every match no matter how full it was — refusing each one and
// putting the players back in the queue, at the matchmaker's tick rate.
func TestMemoryFallbackRespectsCapacity(t *testing.T) {
	self := &pb.GameServer{Id: "solo", PublicAddr: "ws://localhost:8080/ws"}
	r := NewMemory(self)
	ctx := context.Background()

	if err := r.Heartbeat(ctx, &pb.GameServer{
		Id: "solo", PublicAddr: self.PublicAddr, Rooms: 2, Capacity: 2,
	}); err != nil {
		t.Fatal(err)
	}
	gs, err := r.PickLeastLoaded(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if gs != nil {
		t.Fatalf("placed onto %s, which is at its declared capacity", gs.Id)
	}

	// Room frees up, and it is eligible again.
	if err := r.Heartbeat(ctx, &pb.GameServer{
		Id: "solo", PublicAddr: self.PublicAddr, Rooms: 1, Capacity: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if gs, err = r.PickLeastLoaded(ctx); err != nil || gs == nil {
		t.Fatalf("a node with room to spare was not chosen (err=%v)", err)
	}
}

// Draining has to reach the fallback for the same reason.
func TestMemoryFallbackRespectsDraining(t *testing.T) {
	r := NewMemory(&pb.GameServer{Id: "solo", PublicAddr: "ws://localhost:8080/ws"})
	ctx := context.Background()
	if err := r.Heartbeat(ctx, &pb.GameServer{Id: "solo", Draining: true}); err != nil {
		t.Fatal(err)
	}
	if gs, _ := r.PickLeastLoaded(ctx); gs != nil {
		t.Fatal("placed onto a node that is draining")
	}
}
