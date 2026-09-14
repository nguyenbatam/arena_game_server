package placement

import (
	"context"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
)

func TestMemoryAckNoop(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	req := &pb.RoomRequest{RoomId: "r-1", TickRate: 20}
	if err := q.Enqueue(ctx, "gs-1", req); err != nil {
		t.Fatal(err)
	}
	got, err := q.Take(ctx, "gs-1")
	if err != nil || got.Req.RoomId != "r-1" {
		t.Fatalf("%+v %v", got, err)
	}
	if err := q.Ack(ctx, "gs-1", got); err != nil {
		t.Fatal(err)
	}
}

func TestReapStaleMemory(t *testing.T) {
	n, err := NewMemory().ReapStale(context.Background(), "gs-1", time.Minute)
	if err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}
