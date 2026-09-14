package room

import (
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
)

// ApplyDelta is the client half of the scheme, so what reaches it is bytes, and
// bytes do not have to hold the invariant the encoder guarantees. Every walk in
// it is written against strictly ascending ids; a message that breaks that has
// to be refused rather than half-applied.
//
// Duplicated ids were the case that got through. cloneSnapshot copies a full
// snapshot verbatim and the merge emits a baseline entry once per occurrence,
// so a client ended up holding one player twice — which makes its own lookups
// ambiguous and leaves its reconciliation reading whichever copy it found
// first. FuzzApplyDelta is what turned it up, on a delta with two id-0 players
// and no baseline.
func TestApplyDeltaRefusesUnorderedIDs(t *testing.T) {
	player := func(id uint32) *pb.PlayerSnap { return &pb.PlayerSnap{Id: id, Changed: fieldAll} }
	proj := func(id uint32) *pb.ProjSnap { return &pb.ProjSnap{Id: id} }

	cases := []struct {
		name string
		base *pb.Snapshot
		d    *pb.Snapshot
	}{{
		// The shape the fuzzer found: no baseline, so this is a full snapshot,
		// and it names the same player twice.
		name: "full snapshot with a repeated player",
		d:    &pb.Snapshot{Tick: 4, Players: []*pb.PlayerSnap{player(0), player(0)}},
	}, {
		name: "full snapshot with players out of order",
		d:    &pb.Snapshot{Tick: 4, Players: []*pb.PlayerSnap{player(2), player(1)}},
	}, {
		name: "full snapshot with a repeated projectile",
		d:    &pb.Snapshot{Tick: 4, Projectiles: []*pb.ProjSnap{proj(7), proj(7)}},
	}, {
		name: "delta whose players are out of order",
		base: &pb.Snapshot{Tick: 3, Players: []*pb.PlayerSnap{player(1), player(2)}},
		d:    &pb.Snapshot{Tick: 4, BaselineTick: 3, Players: []*pb.PlayerSnap{player(2), player(1)}},
	}, {
		name: "delta whose removals are out of order",
		base: &pb.Snapshot{Tick: 3, Projectiles: []*pb.ProjSnap{proj(1), proj(2)}},
		d:    &pb.Snapshot{Tick: 4, BaselineTick: 3, RemovedProjectiles: []uint32{2, 1}},
	}, {
		name: "baseline with a repeated player",
		base: &pb.Snapshot{Tick: 3, Players: []*pb.PlayerSnap{player(1), player(1)}},
		d:    &pb.Snapshot{Tick: 4, BaselineTick: 3},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ApplyDelta(tc.base, tc.d); got != nil {
				ids := make([]uint32, 0, len(got.Players))
				for _, p := range got.Players {
					ids = append(ids, p.Id)
				}
				t.Fatalf("applied a malformed snapshot instead of refusing it: players %v, projectiles %d",
					ids, len(got.Projectiles))
			}
		})
	}
}

// And the guard must not refuse anything the server actually sends.
func TestApplyDeltaStillAppliesAWellFormedPair(t *testing.T) {
	base := &pb.Snapshot{
		Tick: 3, RoomId: "r",
		Players:     []*pb.PlayerSnap{{Id: 1, X: 5, Hp: 100}, {Id: 2, X: 9, Hp: 75}},
		Projectiles: []*pb.ProjSnap{{Id: 1, X: 3}, {Id: 4, X: 8}},
	}
	d := &pb.Snapshot{
		Tick: 4, BaselineTick: 3, RoomId: "r",
		Players:            []*pb.PlayerSnap{{Id: 2, X: 11, Changed: fieldX}},
		Projectiles:        []*pb.ProjSnap{{Id: 4, X: 12}},
		RemovedProjectiles: []uint32{1},
	}
	out := ApplyDelta(base, d)
	if out == nil {
		t.Fatal("a well-formed delta was refused")
	}
	if len(out.Players) != 2 || out.Players[0].X != 5 || out.Players[1].X != 11 {
		t.Fatalf("players = %+v", out.Players)
	}
	if len(out.Projectiles) != 1 || out.Projectiles[0].Id != 4 || out.Projectiles[0].X != 12 {
		t.Fatalf("projectiles = %+v", out.Projectiles)
	}
}

// A whole match's worth of real encodes has to pass the guard, or it is not a
// guard, it is a bug that only shows up under load.
func TestEveryDeltaAMatchProducesIsWellOrdered(t *testing.T) {
	snaps := busyMatch(t, 8, 120)
	for i := 1; i < len(snaps); i++ {
		if !wellOrdered(snaps[i]) {
			t.Fatalf("tick %d: the encoder produced a snapshot the client would refuse", i)
		}
		d := deltaSnapshot(snaps[i-1], snaps[i])
		if !wellOrdered(d) {
			t.Fatalf("tick %d: delta against the previous tick is not well ordered", i)
		}
		if ApplyDelta(snaps[i-1], d) == nil {
			t.Fatalf("tick %d: a delta this server produced was refused by the client half", i)
		}
	}
}
