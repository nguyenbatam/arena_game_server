package presence

import (
	"context"
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/redistest"
)

func forEachStore(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { fn(t, NewMemory()) })
	t.Run("redis", func(t *testing.T) { fn(t, NewRedis(redistest.Client(t))) })
}

// Reconnect depends on this: the seat a dropped player held has to survive in
// presence, or they come back to a lobby instead of their match.
func TestPresenceKeepsSeatAcrossDisconnect(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		if err := s.Set(ctx, &pb.Presence{
			PlayerId: "p1", Name: "tester", NodeId: "gs-1",
			Status: pb.PresenceStatus_PRESENCE_STATUS_IN_MATCH,
			RoomId: "r1", SeatId: 3, LastTick: 120,
		}); err != nil {
			t.Fatal(err)
		}

		got, err := s.Get(ctx, "p1")
		if err != nil || got == nil {
			t.Fatalf("Get: %v %v", got, err)
		}
		if got.RoomId != "r1" || got.SeatId != 3 || got.LastTick != 120 {
			t.Fatalf("presence did not round-trip: %+v", got)
		}
		if got.Seen == 0 {
			t.Error("Seen must be stamped — the grace window is measured from it")
		}

		// Marking them disconnected keeps the seat.
		if err := s.Set(ctx, &pb.Presence{
			PlayerId: "p1", NodeId: "gs-1",
			Status: pb.PresenceStatus_PRESENCE_STATUS_DISCONNECTED,
			RoomId: "r1", SeatId: 3,
		}); err != nil {
			t.Fatal(err)
		}
		got, _ = s.Get(ctx, "p1")
		if got == nil || got.Status != pb.PresenceStatus_PRESENCE_STATUS_DISCONNECTED {
			t.Fatalf("status not updated: %+v", got)
		}
		if got.RoomId != "r1" || got.SeatId != 3 {
			t.Fatalf("seat lost on disconnect: %+v", got)
		}
	})
}

func TestPresenceDeleteAndMissing(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		got, err := s.Get(ctx, "never")
		if err != nil {
			t.Fatalf("unknown player should be empty, not an error: %v", err)
		}
		if got != nil {
			t.Fatalf("got %+v, want nil", got)
		}

		_ = s.Set(ctx, &pb.Presence{PlayerId: "p2", Status: pb.PresenceStatus_PRESENCE_STATUS_ONLINE})
		if err := s.Delete(ctx, "p2"); err != nil {
			t.Fatal(err)
		}
		if got, _ := s.Get(ctx, "p2"); got != nil {
			t.Fatalf("still present after delete: %+v", got)
		}
	})
}
