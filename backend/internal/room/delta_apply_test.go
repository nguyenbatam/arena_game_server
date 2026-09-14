package room

import (
	"math/rand"
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"google.golang.org/protobuf/proto"
)

// mapApplyDelta is the map-based implementation ApplyDelta replaced.
//
// Kept for the same reason deltaSnapshot's reference is kept next door in
// TestDeltaMatchesTheMapBasedReference: the merge is the faster half of a pair
// whose slow half was obviously correct, and the cheapest way to keep believing
// the fast one is to run both. Delete this the day the merge stops being an
// optimisation of something.
func mapApplyDelta(base, d *pb.Snapshot) *pb.Snapshot {
	if d == nil {
		return nil
	}
	if d.BaselineTick == 0 {
		return cloneSnapshot(d)
	}
	if base == nil || base.Tick != d.BaselineTick {
		return nil
	}
	out := &pb.Snapshot{Tick: d.Tick, RoomId: d.RoomId, Ended: d.Ended, Winner: d.Winner}
	patch := make(map[uint32]*pb.PlayerSnap, len(d.Players))
	for _, p := range d.Players {
		patch[p.Id] = p
	}
	seen := make(map[uint32]struct{}, len(base.Players))
	for _, b := range base.Players {
		seen[b.Id] = struct{}{}
		out.Players = append(out.Players, applyPlayer(b, patch[b.Id]))
	}
	for _, p := range d.Players {
		if _, ok := seen[p.Id]; !ok {
			out.Players = append(out.Players, applyPlayer(nil, p))
		}
	}
	removed := make(map[uint32]struct{}, len(d.RemovedProjectiles))
	for _, id := range d.RemovedProjectiles {
		removed[id] = struct{}{}
	}
	projPatch := make(map[uint32]*pb.ProjSnap, len(d.Projectiles))
	for _, q := range d.Projectiles {
		projPatch[q.Id] = q
	}
	for _, b := range base.Projectiles {
		if _, drop := removed[b.Id]; drop {
			continue
		}
		if q, ok := projPatch[b.Id]; ok {
			out.Projectiles = append(out.Projectiles, &pb.ProjSnap{Id: q.Id, X: q.X, Y: q.Y})
			delete(projPatch, b.Id)
			continue
		}
		out.Projectiles = append(out.Projectiles, &pb.ProjSnap{Id: b.Id, X: b.X, Y: b.Y})
	}
	for _, q := range d.Projectiles {
		if _, ok := projPatch[q.Id]; ok {
			out.Projectiles = append(out.Projectiles, &pb.ProjSnap{Id: q.Id, X: q.X, Y: q.Y})
		}
	}
	return out
}

// randPair builds a (base, cur) pair the way the simulation actually produces
// them, which is what makes a diff meaningful: the roster is fixed for the life
// of a match, projectile ids come from a counter that only increases, and the
// live list is compacted in place, so cur's projectiles are a subsequence of
// base's survivors followed by strictly larger new ids.
func randPair(rng *rand.Rand, nPlayers int) (*pb.Snapshot, *pb.Snapshot) {
	mk := func(tick uint32) *pb.Snapshot {
		s := &pb.Snapshot{Tick: tick, RoomId: "r"}
		for i := 1; i <= nPlayers; i++ {
			s.Players = append(s.Players, &pb.PlayerSnap{
				Id: uint32(i), X: rng.Int31n(100), Y: rng.Int31n(100),
				Aim: rng.Int31n(360), Hp: rng.Int31n(100), Score: uint32(rng.Intn(5)),
				Bot: rng.Intn(2) == 0, Seq: uint32(rng.Intn(50)),
			})
		}
		return s
	}
	base, cur := mk(100), mk(101)

	next := uint32(rng.Intn(5) + 1)
	for k := 0; k < rng.Intn(12); k++ {
		base.Projectiles = append(base.Projectiles, &pb.ProjSnap{
			Id: next, X: rng.Int31n(100), Y: rng.Int31n(100),
		})
		next += uint32(rng.Intn(3) + 1)
	}
	// Survivors keep their ids and may have moved; the rest expired or hit.
	for _, b := range base.Projectiles {
		if rng.Intn(3) == 0 {
			continue
		}
		q := &pb.ProjSnap{Id: b.Id, X: b.X, Y: b.Y}
		if rng.Intn(2) == 0 {
			q.X, q.Y = rng.Int31n(100), rng.Int31n(100)
		}
		cur.Projectiles = append(cur.Projectiles, q)
	}
	// Shots fired this tick: ids from the same counter, so always above.
	for k := 0; k < rng.Intn(4); k++ {
		cur.Projectiles = append(cur.Projectiles, &pb.ProjSnap{
			Id: next, X: rng.Int31n(100), Y: rng.Int31n(100),
		})
		next += uint32(rng.Intn(3) + 1)
	}
	return base, cur
}

// TestApplyDeltaMatchesTheMapBasedReference diffs the two over input shaped the
// way the simulation shapes it, and then checks the thing that actually matters:
// that a delta reconstructs the tick it was encoded from.
//
// The generator respecting the real invariants is load-bearing, not politeness.
// An earlier version of it dealt each snapshot independent random projectile
// ids, which let cur carry ids below base's — something nextProj cannot produce,
// since it only counts up — and on that impossible input the two implementations
// legitimately disagree about ordering. The merge normalises to ascending; the
// map version emitted survivors first and new arrivals after.
func TestApplyDeltaMatchesTheMapBasedReference(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 20000; i++ {
		base, cur := randPair(rng, rng.Intn(24)+1)
		d := deltaSnapshot(base, cur)

		got := ApplyDelta(base, d)
		want := mapApplyDelta(base, d)
		if !proto.Equal(got, want) {
			t.Fatalf("iteration %d diverged\n got: %v\nwant: %v", i, got, want)
		}
		// And the real contract: the delta must reconstruct cur exactly.
		if !proto.Equal(got, &pb.Snapshot{
			Tick: cur.Tick, RoomId: cur.RoomId, Players: cur.Players,
			Projectiles: cur.Projectiles, Ended: cur.Ended, Winner: cur.Winner,
		}) {
			t.Fatalf("iteration %d did not reconstruct cur\n got: %v\nwant: %v", i, got, cur)
		}
	}
}
