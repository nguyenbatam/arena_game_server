package notify

import (
	"context"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/redistest"
)

// A match formed on the matchmaker has to reach the gateway holding that
// player's connection, which may be a different process entirely.
func TestAssignmentReachesSubscriber(t *testing.T) {
	for _, tc := range []struct {
		name string
		bus  func(t *testing.T) Bus
	}{
		{"memory", func(*testing.T) Bus { return NewMemory() }},
		{"redis", func(t *testing.T) Bus { return NewRedis(redistest.Client(t)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := tc.bus(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			got := make(chan *pb.Assignment, 4)
			go func() {
				_ = bus.Subscribe(ctx, func(a *pb.Assignment) { got <- a })
			}()
			// Give the subscription a moment to attach before publishing.
			time.Sleep(150 * time.Millisecond)

			want := &pb.Assignment{
				ConnId: "c1", PlayerId: 7, Name: "tester",
				RoomId: "r1", Host: "ws://gs-2:8080/ws", Seed: 99, TickRate: 20,
			}
			if err := bus.Publish(ctx, want); err != nil {
				t.Fatal(err)
			}

			select {
			case a := <-got:
				if a.ConnId != want.ConnId || a.RoomId != want.RoomId ||
					a.Host != want.Host || a.PlayerId != want.PlayerId || a.Seed != want.Seed {
					t.Fatalf("assignment did not round-trip: %+v", a)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("assignment never arrived — the player would sit in the queue forever")
			}
		})
	}
}
