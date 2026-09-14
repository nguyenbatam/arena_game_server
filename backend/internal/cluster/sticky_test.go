package cluster

import (
	"context"
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/placement"
	"github.com/nguyenbatam/arena_game_server/internal/room"
)

func TestStickyRoomNoSplitBrain(t *testing.T) {
	reg := NewMemory(&pb.GameServer{Id: "gs-a", PublicAddr: "ws://a/ws"})
	ctx := context.Background()
	_ = reg.Heartbeat(ctx, &pb.GameServer{Id: "gs-a", PublicAddr: "ws://a/ws"})
	_ = reg.Heartbeat(ctx, &pb.GameServer{Id: "gs-b", PublicAddr: "ws://b/ws"})
	_ = reg.RegisterRoom(ctx, &pb.RoomInfo{RoomId: "r-1", ServerId: "gs-a", PublicAddr: "ws://a/ws"})

	if room.NewManager().Get("r-1") != nil {
		t.Fatal("other node must not have local room state")
	}
	info, err := reg.GetRoom(ctx, "r-1")
	if err != nil || info == nil || info.ServerId != "gs-a" || info.PublicAddr != "ws://a/ws" {
		t.Fatalf("registry %+v err=%v", info, err)
	}
	gs, err := reg.PickLeastLoaded(ctx)
	if err != nil || gs == nil {
		t.Fatal(err)
	}
}

func TestLeastLoadedPlacement(t *testing.T) {
	reg := NewMemory(&pb.GameServer{Id: "gs-a", PublicAddr: "a"})
	ctx := context.Background()
	_ = reg.Heartbeat(ctx, &pb.GameServer{Id: "gs-a", PublicAddr: "a", Rooms: 5})
	_ = reg.Heartbeat(ctx, &pb.GameServer{Id: "gs-b", PublicAddr: "b", Rooms: 1})
	gs, err := reg.PickLeastLoaded(ctx)
	if err != nil || gs.Id != "gs-b" {
		t.Fatalf("got %+v err=%v", gs, err)
	}
}

func TestPickReservesLoad(t *testing.T) {
	reg := NewMemory(&pb.GameServer{Id: "gs-a", PublicAddr: "a"})
	ctx := context.Background()
	_ = reg.Heartbeat(ctx, &pb.GameServer{Id: "gs-a", PublicAddr: "a", Rooms: 0})
	_ = reg.Heartbeat(ctx, &pb.GameServer{Id: "gs-b", PublicAddr: "b", Rooms: 0})
	first, err := reg.PickLeastLoaded(ctx)
	if err != nil || first.Id != "gs-a" {
		t.Fatalf("first %+v err=%v", first, err)
	}
	second, err := reg.PickLeastLoaded(ctx)
	if err != nil || second.Id != "gs-b" {
		t.Fatalf("second should spread to gs-b, got %+v err=%v", second, err)
	}
	_ = reg.ReleaseInflight(ctx, "gs-a")
	third, err := reg.PickLeastLoaded(ctx)
	if err != nil || third.Id != "gs-a" {
		t.Fatalf("after release expect gs-a, got %+v err=%v", third, err)
	}
}

func TestJobQueueRoutesToOwner(t *testing.T) {
	jobs := placement.NewMemory()
	ctx := context.Background()
	req := &pb.RoomRequest{RoomId: "r-x", TickRate: 20, Seats: []*pb.Seat{{ConnId: "c1", PlayerId: 1}}}
	if err := jobs.Enqueue(ctx, "gs-b", req); err != nil {
		t.Fatal(err)
	}
	got, err := jobs.Take(ctx, "gs-b")
	if err != nil || got == nil || got.Req.RoomId != "r-x" {
		t.Fatalf("%+v %v", got, err)
	}
}
