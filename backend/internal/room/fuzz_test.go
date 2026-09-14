package room

import (
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"google.golang.org/protobuf/proto"
)

// ApplyDelta is the client half of the delta scheme living in server code, and
// both the browser and the load bot run the same algorithm. It is fed a
// baseline it kept and a delta it was sent, and neither has to be well formed:
// a field bitmask can claim a field the message does not carry, a removal list
// can name projectiles that were never there, and a baseline tick can point
// anywhere.
//
// It must produce a snapshot or nil for any of that, and never panic.
func FuzzApplyDelta(f *testing.F) {
	base := &pb.Snapshot{
		Tick: 10, RoomId: "r",
		Players:     []*pb.PlayerSnap{{Id: 1, X: 5, Y: 6, Hp: 100}, {Id: 2, X: 9, Y: 1, Hp: 75}},
		Projectiles: []*pb.ProjSnap{{Id: 1, X: 3, Y: 3}},
	}
	delta := &pb.Snapshot{
		Tick: 11, BaselineTick: 10,
		Players:            []*pb.PlayerSnap{{Id: 1, X: 7, Changed: uint32(pb.PlayerField_PLAYER_FIELD_X)}},
		RemovedProjectiles: []uint32{1},
	}
	baseBytes, _ := proto.Marshal(base)
	deltaBytes, _ := proto.Marshal(delta)
	f.Add(baseBytes, deltaBytes)
	f.Add([]byte{}, deltaBytes)
	f.Add(baseBytes, []byte{})
	f.Add([]byte{0xff}, []byte{0xff})

	f.Fuzz(func(t *testing.T, baseRaw, deltaRaw []byte) {
		var b, d pb.Snapshot
		if proto.Unmarshal(baseRaw, &b) != nil || proto.Unmarshal(deltaRaw, &d) != nil {
			return
		}
		out := ApplyDelta(&b, &d)
		if out == nil {
			return
		}
		// A snapshot that came out has to be usable: re-encodable, and not
		// carrying a player twice, which would make the client's own lookup
		// ambiguous.
		if _, err := proto.Marshal(out); err != nil {
			t.Fatalf("applied snapshot will not re-marshal: %v", err)
		}
		seen := make(map[uint32]bool, len(out.Players))
		for _, p := range out.Players {
			if seen[p.Id] {
				t.Fatalf("player %d appears twice in the applied snapshot", p.Id)
			}
			seen[p.Id] = true
		}
	})
}
