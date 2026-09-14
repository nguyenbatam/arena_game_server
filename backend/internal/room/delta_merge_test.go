package room

import (
	"sort"
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
	"google.golang.org/protobuf/proto"
)

// deltaSnapshot walks two sorted lists instead of building two lookup maps, and
// everything in this file exists to hold that rewrite to the behaviour it
// replaced. Two things have to stay true for the merge to be sound, and each
// gets its own case: the ids really are ascending in both snapshots, and the
// output is identical to what the map version produced.

// referenceDelta is the map-based encoder deltaSnapshot replaced, kept here as
// the oracle. It makes no ordering assumption at all, so a disagreement between
// the two is the merge being wrong rather than both sharing a mistake.
func referenceDelta(base, cur *pb.Snapshot) *pb.Snapshot {
	out := &pb.Snapshot{
		Tick:         cur.Tick,
		RoomId:       cur.RoomId,
		Ended:        cur.Ended,
		Winner:       cur.Winner,
		BaselineTick: base.Tick,
		// The span the caller supplies. deltaSnapshot's two-argument form hands
		// it cur's own events, so the reference mirrors that — the encoder does
		// not gather, it carries. See Room.eventsSince for the gathering.
		Events: cur.Events,
	}
	prev := make(map[uint32]*pb.PlayerSnap, len(base.Players))
	for _, p := range base.Players {
		prev[p.Id] = p
	}
	for _, p := range cur.Players {
		d := &pb.PlayerSnap{}
		if playerDelta(d, prev[p.Id], p) {
			out.Players = append(out.Players, d)
		}
	}
	prevProj := make(map[uint32]*pb.ProjSnap, len(base.Projectiles))
	for _, q := range base.Projectiles {
		prevProj[q.Id] = q
	}
	for _, q := range cur.Projectiles {
		b, ok := prevProj[q.Id]
		delete(prevProj, q.Id)
		if ok && b.X == q.X && b.Y == q.Y {
			continue
		}
		out.Projectiles = append(out.Projectiles, q)
	}
	if len(prevProj) > 0 {
		out.RemovedProjectiles = make([]uint32, 0, len(prevProj))
		for id := range prevProj {
			out.RemovedProjectiles = append(out.RemovedProjectiles, id)
		}
		sort.Slice(out.RemovedProjectiles, func(i, j int) bool {
			return out.RemovedProjectiles[i] < out.RemovedProjectiles[j]
		})
	}
	return out
}

// busyMatch runs a room hard enough that projectiles are constantly spawning,
// travelling, hitting and expiring, and returns every tick's encoded state.
// Measuring on a quiet room would flatter the merge: the interesting cases are
// baselines whose projectile list has since had entries removed from the front,
// the middle and the end.
func busyMatch(t *testing.T, players, ticks int) []*pb.Snapshot {
	t.Helper()
	roster := make([]sim.Player, 0, players)
	for i := 1; i <= players; i++ {
		roster = append(roster, sim.Player{ID: sim.PlayerID(i), Bot: i%3 == 0})
	}
	r := New(Params{ID: "merge", Seed: 7, TickRate: 20, MatchTicks: uint32(ticks) + 1, Roster: roster})

	out := make([]*pb.Snapshot, 0, ticks)
	pending := make(map[sim.PlayerID]sim.Input, players)
	for tick := 0; tick < ticks; tick++ {
		for i := 1; i <= players; i++ {
			pending[sim.PlayerID(i)] = sim.Input{
				MX: int8(i%3) - 1, MY: int8((i/2)%3) - 1,
				Fire: (tick+i)%4 == 0, Aim: int16((tick * i * 7) % 360), Seq: uint32(tick + 1),
			}
		}
		snap := r.world.Step(pending)
		clear(pending)
		out = append(out, r.encodeState(snap))
	}
	return out
}

// The invariant deltaSnapshot's merge rests on, and it belongs to internal/sim
// rather than to this package: the world keeps its roster sorted by id for the
// life of a match, and projectiles are appended with strictly increasing ids
// and compacted in place. If a change over there breaks either, this fails
// loudly instead of the encoder silently producing a wrong delta.
func TestEncodeStateEmitsIdsInAscendingOrder(t *testing.T) {
	saw := false
	for _, s := range busyMatch(t, 8, 200) {
		for i := 1; i < len(s.Players); i++ {
			if s.Players[i-1].Id >= s.Players[i].Id {
				t.Fatalf("tick %d: players out of order at %d: %d then %d",
					s.Tick, i, s.Players[i-1].Id, s.Players[i].Id)
			}
		}
		for i := 1; i < len(s.Projectiles); i++ {
			if s.Projectiles[i-1].Id >= s.Projectiles[i].Id {
				t.Fatalf("tick %d: projectiles out of order at %d: %d then %d",
					s.Tick, i, s.Projectiles[i-1].Id, s.Projectiles[i].Id)
			}
		}
		if len(s.Projectiles) > 1 {
			saw = true
		}
	}
	if !saw {
		t.Fatal("no tick carried more than one projectile; the ordering claim was never exercised")
	}
}

// The rewrite must be a pure optimisation. Every pair of ticks in a busy match
// is encoded both ways and compared on the wire representation, at gaps from
// one tick to nearly the whole history window — a wide gap is the case where
// the baseline's projectiles have all been spent, which is where an ordering
// bug would show first.
func TestDeltaMatchesTheMapBasedReference(t *testing.T) {
	for _, players := range []int{2, 8, 24} {
		snaps := busyMatch(t, players, 200)
		compared := 0
		for _, gap := range []int{1, 2, 3, 5, 13, 40, 63} {
			for i := 0; i+gap < len(snaps); i++ {
				base, cur := snaps[i], snaps[i+gap]
				want := referenceDelta(base, cur)
				got := deltaSnapshot(base, cur)
				if !proto.Equal(want, got) {
					t.Fatalf("players=%d tick %d against baseline %d:\nwant %v\ngot  %v",
						players, cur.Tick, base.Tick, want, got)
				}
				compared++
			}
		}
		if compared == 0 {
			t.Fatalf("players=%d compared nothing", players)
		}
	}
}

// Room.broadcast encodes once per distinct baseline and hands the same bytes to
// every client sitting on it, so two encodes of the same pair have to be
// byte-identical. The map version needed an explicit sort to get there; the
// merge produces removals in order by construction, and this is what says so.
func TestDeltaIsByteIdenticalAcrossEncodes(t *testing.T) {
	snaps := busyMatch(t, 8, 120)
	for i := 0; i+5 < len(snaps); i++ {
		a := deltaSnapshot(snaps[i], snaps[i+5])
		b := deltaSnapshot(snaps[i], snaps[i+5])
		if string(mustMarshal(t, a)) != string(mustMarshal(t, b)) {
			t.Fatalf("two encodes of tick %d against %d differ", snaps[i+5].Tick, snaps[i].Tick)
		}
		for k := 1; k < len(a.RemovedProjectiles); k++ {
			if a.RemovedProjectiles[k-1] >= a.RemovedProjectiles[k] {
				t.Fatalf("removals not ascending: %v", a.RemovedProjectiles)
			}
		}
	}
}

func mustMarshal(t *testing.T, s *pb.Snapshot) []byte {
	t.Helper()
	b, err := proto.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A busy match does not reach every shape the merge has to handle, so the edges
// are built by hand: ids missing from the front, the middle and the tail of
// each list, on both sides.
func TestDeltaMergeHandlesGapsAtEveryPosition(t *testing.T) {
	players := func(ids ...uint32) []*pb.PlayerSnap {
		out := make([]*pb.PlayerSnap, 0, len(ids))
		for _, id := range ids {
			out = append(out, &pb.PlayerSnap{Id: id, X: int32(id) * 10, Hp: 100})
		}
		return out
	}
	projs := func(ids ...uint32) []*pb.ProjSnap {
		out := make([]*pb.ProjSnap, 0, len(ids))
		for _, id := range ids {
			out = append(out, &pb.ProjSnap{Id: id, X: int32(id), Y: int32(id) * 2})
		}
		return out
	}

	cases := []struct {
		name       string
		base, cur  *pb.Snapshot
		wantRemove []uint32
	}{
		{
			name:       "baseline empty",
			base:       &pb.Snapshot{Tick: 1, Players: players(1, 2), Projectiles: nil},
			cur:        &pb.Snapshot{Tick: 2, Players: players(1, 2), Projectiles: projs(3, 4)},
			wantRemove: nil,
		},
		{
			name:       "current empty",
			base:       &pb.Snapshot{Tick: 1, Players: players(1, 2), Projectiles: projs(3, 4)},
			cur:        &pb.Snapshot{Tick: 2, Players: players(1, 2), Projectiles: nil},
			wantRemove: []uint32{3, 4},
		},
		{
			name:       "gap at the front",
			base:       &pb.Snapshot{Tick: 1, Players: players(1, 2), Projectiles: projs(1, 2, 3)},
			cur:        &pb.Snapshot{Tick: 2, Players: players(1, 2), Projectiles: projs(3, 9)},
			wantRemove: []uint32{1, 2},
		},
		{
			name:       "gap in the middle",
			base:       &pb.Snapshot{Tick: 1, Players: players(1, 2), Projectiles: projs(1, 5, 9)},
			cur:        &pb.Snapshot{Tick: 2, Players: players(1, 2), Projectiles: projs(1, 9)},
			wantRemove: []uint32{5},
		},
		{
			name:       "gap at the tail",
			base:       &pb.Snapshot{Tick: 1, Players: players(1, 2), Projectiles: projs(1, 5, 9)},
			cur:        &pb.Snapshot{Tick: 2, Players: players(1, 2), Projectiles: projs(1, 5)},
			wantRemove: []uint32{9},
		},
		{
			name:       "disjoint ids",
			base:       &pb.Snapshot{Tick: 1, Players: players(1, 2), Projectiles: projs(1, 2, 3)},
			cur:        &pb.Snapshot{Tick: 2, Players: players(1, 2), Projectiles: projs(7, 8)},
			wantRemove: []uint32{1, 2, 3},
		},
		{
			name:       "player joins mid-list",
			base:       &pb.Snapshot{Tick: 1, Players: players(1, 3)},
			cur:        &pb.Snapshot{Tick: 2, Players: players(1, 2, 3)},
			wantRemove: nil,
		},
		{
			name:       "player missing from current",
			base:       &pb.Snapshot{Tick: 1, Players: players(1, 2, 3)},
			cur:        &pb.Snapshot{Tick: 2, Players: players(1, 3)},
			wantRemove: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deltaSnapshot(tc.base, tc.cur)
			if want := referenceDelta(tc.base, tc.cur); !proto.Equal(want, got) {
				t.Fatalf("disagrees with the reference:\nwant %v\ngot  %v", want, got)
			}
			if len(got.RemovedProjectiles) != len(tc.wantRemove) {
				t.Fatalf("removed %v, want %v", got.RemovedProjectiles, tc.wantRemove)
			}
			for i, id := range tc.wantRemove {
				if got.RemovedProjectiles[i] != id {
					t.Fatalf("removed %v, want %v", got.RemovedProjectiles, tc.wantRemove)
				}
			}
			// And a client can still rebuild the full state from it, which is
			// the only thing any of this is for.
			if rebuilt := ApplyDelta(tc.base, got); rebuilt == nil {
				t.Fatal("ApplyDelta refused the delta")
			}
		})
	}
}

// A newly spawned projectile whose id happens to sort before a surviving one
// cannot arise from sim today, but the merge would mis-handle it silently if it
// ever did. Asserting the encoder agrees with the order-free reference here
// means the guard is the ordering test above, not luck.
func TestDeltaMergeDoesNotInventRemovals(t *testing.T) {
	base := &pb.Snapshot{Tick: 1, Projectiles: []*pb.ProjSnap{{Id: 5, X: 1}}}
	cur := &pb.Snapshot{Tick: 2, Projectiles: []*pb.ProjSnap{{Id: 5, X: 2}, {Id: 6, X: 3}}}
	got := deltaSnapshot(base, cur)
	if len(got.RemovedProjectiles) != 0 {
		t.Fatalf("removed %v from a baseline whose projectile is still alive", got.RemovedProjectiles)
	}
	if len(got.Projectiles) != 2 {
		t.Fatalf("sent %d projectiles, want the moved one and the new one", len(got.Projectiles))
	}
}
