package protocol

import (
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"google.golang.org/protobuf/proto"
)

func snapWith(tick uint32, players int) *pb.Snapshot {
	s := &pb.Snapshot{Tick: tick, RoomId: "r-1", BaselineTick: tick - 1}
	for i := 1; i <= players; i++ {
		s.Players = append(s.Players, &pb.PlayerSnap{
			Id: uint32(i), X: int32(int(tick) * i), Y: int32(i), Hp: 100, Changed: 3,
		})
	}
	return s
}

// A reused encoder must produce exactly what the one-shot form produces, every
// time. Anything else and the tick path is shipping different bytes than every
// test in the repo checks.
func TestSnapshotEncoderMatchesSnapshot(t *testing.T) {
	var enc SnapshotEncoder
	for tick := uint32(1); tick <= 50; tick++ {
		// Varying shapes, so a field left over from a previous message would
		// have somewhere to show up.
		s := snapWith(tick, int(tick%7)+1)
		if tick%3 == 0 {
			s.Ended, s.Winner = true, tick
		}
		if tick%4 == 0 {
			s.Projectiles = append(s.Projectiles, &pb.ProjSnap{Id: tick, X: 5, Y: 6})
			s.RemovedProjectiles = append(s.RemovedProjectiles, tick-1)
		}

		want := Snapshot(s)
		got := enc.Marshal(s)
		if string(got) != string(want) {
			t.Fatalf("tick %d: encoder produced %d bytes, Snapshot produced %d", tick, len(got), len(want))
		}
	}
}

// The bytes handed back belong to the caller: the next message must not write
// over a slice somebody is still holding.
func TestSnapshotEncoderReturnsIndependentBytes(t *testing.T) {
	var enc SnapshotEncoder
	first := enc.Marshal(snapWith(10, 4))
	kept := append([]byte(nil), first...)

	for tick := uint32(11); tick < 20; tick++ {
		enc.Marshal(snapWith(tick, 8))
	}
	if string(first) != string(kept) {
		t.Error("a later Marshal wrote over the bytes an earlier one returned")
	}
}

// Every message the encoder emits must decode back to the snapshot it was
// given — no state carried over from the previous one.
func TestSnapshotEncoderRoundTrips(t *testing.T) {
	var enc SnapshotEncoder
	for tick := uint32(1); tick <= 20; tick++ {
		in := snapWith(tick, int(tick%5)+1)
		var env pb.Envelope
		if err := proto.Unmarshal(enc.Marshal(in), &env); err != nil {
			t.Fatalf("tick %d: %v", tick, err)
		}
		if env.Type != pb.MsgType_MSG_TYPE_SNAPSHOT {
			t.Fatalf("tick %d: type %v", tick, env.Type)
		}
		if !proto.Equal(env.GetSnapshot(), in) {
			t.Fatalf("tick %d: decoded a different snapshot than was encoded", tick)
		}
	}
}

// The encoder holds no reference to the snapshot once Marshal returns, which is
// what lets the room hand it a message it overwrites on the next tick.
func TestSnapshotEncoderDoesNotRetainTheSnapshot(t *testing.T) {
	var enc SnapshotEncoder
	s := snapWith(5, 3)
	before := enc.Marshal(s)
	kept := append([]byte(nil), before...)

	// The room recycles its messages; scribbling on one stands in for that.
	s.Players[0].X = 99999
	s.Tick = 4242

	if string(before) != string(kept) {
		t.Error("mutating the snapshot after Marshal changed bytes already returned")
	}
	// And the next call sees the new values, so nothing was cached either.
	var env pb.Envelope
	if err := proto.Unmarshal(enc.Marshal(s), &env); err != nil {
		t.Fatal(err)
	}
	if env.GetSnapshot().Tick != 4242 || env.GetSnapshot().Players[0].X != 99999 {
		t.Error("the encoder served a stale copy of the snapshot")
	}
}
