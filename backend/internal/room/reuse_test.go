package room

import (
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
	"google.golang.org/protobuf/proto"
)

func reuseRoom(players int) *Room {
	roster := make([]sim.Player, 0, players)
	for i := 1; i <= players; i++ {
		roster = append(roster, sim.Player{ID: sim.PlayerID(i)})
	}
	return New(Params{ID: "reuse", Seed: 99, TickRate: 20, MatchTicks: 1 << 20, Roster: roster})
}

// The delta message is scratch, so a field that did not change has to be
// cleared rather than left holding an earlier tick's value.
//
// proto3 puts any non-zero scalar on the wire whether or not its Changed bit is
// set, so a stale value does not confuse a client — it reads the mask — it
// simply ships the bytes of a full snapshot inside a delta, silently undoing
// the entire encoding. The size comparison is the only thing that catches it.
func TestReusedDeltaBufDoesNotLeakStaleFields(t *testing.T) {
	r := reuseRoom(8)

	// A busy tick first, so the scratch is full of values.
	busy := map[sim.PlayerID]sim.Input{}
	for i := 1; i <= 8; i++ {
		busy[sim.PlayerID(i)] = sim.Input{MX: 1, MY: 1, Aim: int16(i * 11), Fire: true, Seq: uint32(i)}
	}
	base := r.encodeState(r.world.Advance(busy))
	moved := r.encodeState(r.world.Advance(busy))

	// Then a tick where a single player moves and nobody else does.
	var quiet deltaBuf
	oneChange := quiet.encode(base, moved, moved.Events)
	reusedBytes := proto.Size(oneChange)

	// The same delta built into a message that has never been used before.
	var fresh deltaBuf
	freshBytes := proto.Size(fresh.encode(base, moved, moved.Events))

	if reusedBytes != freshBytes {
		t.Errorf("reused scratch encoded %d bytes, a fresh one %d: stale fields are reaching the wire",
			reusedBytes, freshBytes)
	}

	// And directly: a field with no Changed bit must be zero on the message.
	for _, p := range oneChange.Players {
		if p.Changed&fieldX == 0 && p.X != 0 {
			t.Errorf("player %d has X=%d with no X bit set", p.Id, p.X)
		}
		if p.Changed&fieldHP == 0 && p.Hp != 0 {
			t.Errorf("player %d has Hp=%d with no HP bit set", p.Id, p.Hp)
		}
		if p.Changed&fieldSeq == 0 && p.Seq != 0 {
			t.Errorf("player %d has Seq=%d with no Seq bit set", p.Id, p.Seq)
		}
	}
}

// Reusing one buf across a whole match must produce exactly what a fresh buf
// would, every tick — the encoding cannot depend on what the buf held before.
func TestReusedDeltaBufMatchesAFreshOneEveryTick(t *testing.T) {
	r := reuseRoom(6)
	var reused deltaBuf

	snaps := make([]*pb.Snapshot, 0, 40)
	for i := 0; i < 40; i++ {
		in := map[sim.PlayerID]sim.Input{}
		for p := 1; p <= 6; p++ {
			// Deliberately uneven: some players stand still on some ticks, so
			// the delta's player list changes length from tick to tick.
			if (i+p)%3 != 0 {
				in[sim.PlayerID(p)] = sim.Input{
					MX: int8((i+p)%3 - 1), Aim: int16((i * p) % 360),
					Fire: (i+p)%5 == 0, Seq: uint32(i + 1),
				}
			}
		}
		snaps = append(snaps, r.encodeState(r.world.Advance(in)))
	}

	for i := 1; i < len(snaps); i++ {
		var fresh deltaBuf
		want := protocol.Snapshot(fresh.encode(snaps[i-1], snaps[i], snaps[i].Events))
		got := protocol.Snapshot(reused.encode(snaps[i-1], snaps[i], snaps[i].Events))
		if string(got) != string(want) {
			t.Fatalf("tick %d: reused buf produced %d bytes, fresh produced %d",
				snaps[i].Tick, len(got), len(want))
		}
	}
}

// Several baselines in one fanout share the room's single scratch. Each delta
// is marshalled before the next is built, so the bytes must all be right.
func TestOneScratchServesSeveralBaselinesInATick(t *testing.T) {
	r := reuseRoom(4)

	got := map[uint32][]byte{}
	for seat := uint32(1); seat <= 3; seat++ {
		r.Subscribe(seat, func(b []byte) {
			got[seat] = append([]byte(nil), b...)
		})
	}

	in := map[sim.PlayerID]sim.Input{1: {MX: 1, Seq: 1}, 2: {MY: 1, Seq: 1}}
	for i := 0; i < 4; i++ {
		r.broadcast(r.world.Advance(in))
		// Each seat acks a different tick, so the fanout has to encode three
		// distinct baselines against the same scratch.
		for seat := uint32(1); seat <= 3; seat++ {
			if tick := r.LastTick(); tick > seat-1 {
				r.recordAck(seat, tick-(seat-1))
			}
		}
	}

	// Every client must be able to rebuild the current world from what it got.
	cur := r.ring[r.LastTick()%SnapshotHistory]
	for seat := uint32(1); seat <= 3; seat++ {
		var env pb.Envelope
		if err := proto.Unmarshal(got[seat], &env); err != nil {
			t.Fatalf("seat %d: %v", seat, err)
		}
		d := env.GetSnapshot()
		if d == nil {
			t.Fatalf("seat %d got no snapshot", seat)
		}
		base := r.ring[d.BaselineTick%SnapshotHistory]
		if d.BaselineTick == 0 {
			base = nil
		}
		out := ApplyDelta(base, d)
		if out == nil {
			t.Fatalf("seat %d: delta against tick %d could not be applied", seat, d.BaselineTick)
		}
		if !proto.Equal(stripBaseline(out), stripBaseline(cur)) {
			t.Errorf("seat %d rebuilt a different world than the server has", seat)
		}
	}
}

func stripBaseline(s *pb.Snapshot) *pb.Snapshot {
	c := proto.Clone(s).(*pb.Snapshot)
	c.BaselineTick = 0
	c.RemovedProjectiles = nil
	return c
}

// The ring slot a tick lands in is recycled, so it must already be out of reach
// as a baseline by the time it is overwritten. That holds only because
// baselineFor refuses an ack SnapshotHistory ticks old; shrink one without the
// other and a client is deltaed against a world that has been overwritten.
func TestRingSlotIsOnlyRecycledOnceItIsUnusable(t *testing.T) {
	r := reuseRoom(2)
	for i := 0; i < SnapshotHistory*2; i++ {
		r.broadcast(r.world.Advance(map[sim.PlayerID]sim.Input{1: {MX: 1, Seq: uint32(i + 1)}}))
	}
	now := r.LastTick()

	// The tick whose slot the next broadcast will take.
	doomed := now + 1 - SnapshotHistory
	if got := r.baselineFor(doomed, now+1); got != nil {
		t.Errorf("tick %d is still offered as a baseline, but tick %d is about to overwrite its slot",
			doomed, now+1)
	}
	// Everything newer than that is still usable, and is a different object.
	slot := (now + 1) % SnapshotHistory
	for ack := doomed + 1; ack <= now; ack++ {
		b := r.baselineFor(ack, now+1)
		if b == nil {
			t.Fatalf("tick %d should still be a usable baseline", ack)
		}
		if ack%SnapshotHistory == slot {
			t.Fatalf("tick %d shares the slot about to be recycled", ack)
		}
	}
}

// Recycling must not let a ring entry describe a world it does not belong to.
// Every slot has to hold its own tick's state, not a neighbour's.
func TestRecycledRingEntriesStayDistinct(t *testing.T) {
	r := reuseRoom(3)
	want := map[uint32][]int32{}

	for i := 0; i < SnapshotHistory+20; i++ {
		snap := r.world.Advance(map[sim.PlayerID]sim.Input{
			1: {MX: 1, Aim: int16(i % 360), Seq: uint32(i + 1)},
		})
		r.broadcast(snap)
		tick := r.LastTick()
		cur := r.ring[tick%SnapshotHistory]
		if cur.Tick != tick {
			t.Fatalf("slot %d holds tick %d, want %d", tick%SnapshotHistory, cur.Tick, tick)
		}
		xs := make([]int32, 0, len(cur.Players))
		for _, p := range cur.Players {
			xs = append(xs, p.X)
		}
		want[tick] = xs
	}

	// Every entry still in the ring must describe the tick it is filed under.
	for tick, xs := range want {
		cur := r.ring[tick%SnapshotHistory]
		if cur.Tick != tick {
			continue // legitimately recycled by a later tick
		}
		for i, p := range cur.Players {
			if p.X != xs[i] {
				t.Errorf("tick %d player %d has X=%d, recorded %d", tick, p.Id, p.X, xs[i])
			}
		}
	}
}

// A projectile list that shrinks and grows again must not reallocate a message
// for a slot that already had one, and must never leave a stale projectile
// visible past the new length.
func TestReuseProjsKeepsMessagesAndLength(t *testing.T) {
	var dst []*pb.ProjSnap

	dst = reuseProjs(dst, 4)
	if len(dst) != 4 {
		t.Fatalf("len %d, want 4", len(dst))
	}
	for i, q := range dst {
		if q == nil {
			t.Fatalf("slot %d is nil", i)
		}
		q.Id = uint32(i + 1)
	}
	first := dst[3]

	dst = reuseProjs(dst, 1)
	if len(dst) != 1 {
		t.Fatalf("len %d after shrink, want 1", len(dst))
	}

	dst = reuseProjs(dst, 4)
	if len(dst) != 4 {
		t.Fatalf("len %d after regrow, want 4", len(dst))
	}
	if dst[3] != first {
		t.Error("regrowing allocated a replacement for a message that was still there")
	}

	// Growing past anything seen before still fills every slot.
	dst = reuseProjs(dst, 9)
	for i, q := range dst {
		if q == nil {
			t.Fatalf("slot %d is nil after growing past capacity", i)
		}
	}
}

func TestReusePlayersKeepsMessagesAndLength(t *testing.T) {
	var dst []*pb.PlayerSnap
	dst = reusePlayers(dst, 3)
	for i, p := range dst {
		if p == nil {
			t.Fatalf("slot %d is nil", i)
		}
		p.Id = uint32(i + 1)
	}
	kept := dst[2]

	dst = reusePlayers(dst, 1)
	dst = reusePlayers(dst, 3)
	if dst[2] != kept {
		t.Error("regrowing allocated a replacement for a message that was still there")
	}
	dst = reusePlayers(dst, 7)
	if len(dst) != 7 {
		t.Fatalf("len %d, want 7", len(dst))
	}
	for i, p := range dst {
		if p == nil {
			t.Fatalf("slot %d is nil after growing past capacity", i)
		}
	}
}
