package cluster

import (
	"context"
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/redistest"
)

func redisRegistry(t *testing.T) *Redis {
	t.Helper()
	return NewRedis(redistest.Client(t))
}

// Both registries are selected by one env var and must place matches the same
// way, so the interesting assertions run against each.
func forEachRegistry(t *testing.T, fn func(t *testing.T, r Registry)) {
	t.Helper()
	// In memory mode the local node is itself a placement candidate — that is
	// the point of single-process mode — so make it one of the nodes under
	// test rather than a surprise third server.
	t.Run("memory", func(t *testing.T) {
		fn(t, NewMemory(&pb.GameServer{Id: "a", PublicAddr: "ws://a/ws"}))
	})
	t.Run("redis", func(t *testing.T) { fn(t, redisRegistry(t)) })
}

// A room lives on exactly one node. If the directory ever returned a different
// address than the one holding the room, clients would be sent to a server that
// cannot serve them — split brain.
func TestRedisRoomDirectoryIsAuthoritative(t *testing.T) {
	r := redisRegistry(t)
	ctx := context.Background()

	info := &pb.RoomInfo{RoomId: "r1", ServerId: "gs-1", PublicAddr: "ws://gs-1:8080/ws", Seed: 42}
	if err := r.RegisterRoom(ctx, info); err != nil {
		t.Fatal(err)
	}

	got, err := r.GetRoom(ctx, "r1")
	if err != nil || got == nil {
		t.Fatalf("GetRoom: %v %v", got, err)
	}
	if got.ServerId != "gs-1" || got.PublicAddr != info.PublicAddr || got.Seed != 42 {
		t.Fatalf("room info did not round-trip: %+v", got)
	}

	if err := r.UnregisterRoom(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := r.GetRoom(ctx, "r1"); got != nil {
		t.Fatalf("room still in the directory after the match ended: %+v", got)
	}
}

func TestRedisMissingRoomIsNotAnError(t *testing.T) {
	r := redisRegistry(t)
	got, err := r.GetRoom(context.Background(), "never-existed")
	if err != nil {
		t.Fatalf("looking up an unknown room should be empty, not an error: %v", err)
	}
	if got != nil {
		t.Fatalf("got %+v, want nil", got)
	}
}

// Placement follows the heartbeat: the emptiest live server wins.
func TestRedisPicksLeastLoaded(t *testing.T) {
	r := redisRegistry(t)
	ctx := context.Background()

	for _, gs := range []*pb.GameServer{
		{Id: "busy", PublicAddr: "ws://busy/ws", Rooms: 40},
		{Id: "idle", PublicAddr: "ws://idle/ws", Rooms: 2},
		{Id: "mid", PublicAddr: "ws://mid/ws", Rooms: 12},
	} {
		if err := r.Heartbeat(ctx, gs); err != nil {
			t.Fatal(err)
		}
	}

	servers, err := r.ListServers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(servers) != 3 {
		t.Fatalf("ListServers returned %d, want 3", len(servers))
	}

	pick, err := r.PickLeastLoaded(ctx)
	if err != nil || pick == nil {
		t.Fatalf("PickLeastLoaded: %v %v", pick, err)
	}
	if pick.Id != "idle" {
		t.Fatalf("picked %q, want the emptiest node", pick.Id)
	}
}

// Heartbeats lag behind placement, so a node that was just handed a match still
// reports its old room count. Without counting that in-flight work, a burst of
// matches would all pile onto whichever node last reported empty.
func TestPickCountsInFlightWork(t *testing.T) {
	forEachRegistry(t, func(t *testing.T, r Registry) {
		ctx := context.Background()
		_ = r.Heartbeat(ctx, &pb.GameServer{Id: "a", PublicAddr: "ws://a/ws", Rooms: 0})
		_ = r.Heartbeat(ctx, &pb.GameServer{Id: "b", PublicAddr: "ws://b/ws", Rooms: 1})

		var got []string
		for i := 0; i < 3; i++ {
			pick, err := r.PickLeastLoaded(ctx)
			if err != nil || pick == nil {
				t.Fatalf("pick %d: %v %v", i, pick, err)
			}
			got = append(got, pick.Id)
		}

		// a starts one match emptier, so it takes the first two; by the third
		// its in-flight load has caught up with b and the tie goes elsewhere.
		want := []string{"a", "a", "b"}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("placements were %v, want %v — in-flight work is not counted", got, want)
			}
		}

		// Releasing a reservation puts that capacity back.
		if err := r.ReleaseInflight(ctx, "a"); err != nil {
			t.Fatal(err)
		}
		next, _ := r.PickLeastLoaded(ctx)
		if next == nil || next.Id != "a" {
			t.Fatalf("after releasing a's reservation it should win again, got %+v", next)
		}
	})
}

func TestRedisPickWithNoServers(t *testing.T) {
	r := redisRegistry(t)
	pick, err := r.PickLeastLoaded(context.Background())
	if err == nil && pick != nil {
		t.Fatalf("expected no placement when the fleet is empty, got %+v", pick)
	}
}
