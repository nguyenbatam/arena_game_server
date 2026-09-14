package room

import (
	"bytes"
	"sync"
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
	"google.golang.org/protobuf/proto"
)

func roomWithPlayers(t *testing.T, n int) *Room {
	t.Helper()
	roster := make([]sim.Player, 0, n)
	for i := 1; i <= n; i++ {
		roster = append(roster, sim.Player{ID: sim.PlayerID(i), Bot: i > n/2})
	}
	return New(Params{ID: "r", Seed: 42, TickRate: 20, MatchTicks: 1 << 20, Roster: roster})
}

type recorder struct {
	mu   sync.Mutex
	msgs [][]byte
}

func (r *recorder) send(b []byte) {
	r.mu.Lock()
	r.msgs = append(r.msgs, b)
	r.mu.Unlock()
}

// all is every message this subscriber has been sent, in order.
func (r *recorder) all() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]byte(nil), r.msgs...)
}

func (r *recorder) last() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.msgs) == 0 {
		return nil
	}
	return r.msgs[len(r.msgs)-1]
}

func step(r *Room, in map[sim.PlayerID]sim.Input) sim.Snapshot {
	snap := r.world.Step(in)
	r.broadcast(snap)
	return snap
}

// Sharing one encode between clients on the same baseline is only sound if the
// bytes are identical to what a per-client encode would have produced. This is
// the property the optimisation rests on.
func TestSharedDeltaMatchesAPerClientEncode(t *testing.T) {
	r := roomWithPlayers(t, 8)
	recs := make([]*recorder, 4)
	for i := range recs {
		recs[i] = &recorder{}
		r.Subscribe(uint32(i+1), recs[i].send)
	}

	in := map[sim.PlayerID]sim.Input{1: {MX: 1, Aim: 30, Fire: true, Seq: 1}}
	step(r, in) // everyone starts on a full snapshot

	for _, s := range r.currentSubs().list {
		s.ack.Store(r.LastTick())
	}
	base := r.ring[r.LastTick()%SnapshotHistory]

	snap := step(r, map[sim.PlayerID]sim.Input{1: {MX: -1, Aim: 90, Seq: 2}})
	cur := r.ring[snap.Tick%SnapshotHistory]
	want := protocol.Snapshot(deltaSnapshot(base, cur))

	for i, rec := range recs {
		if got := rec.last(); !bytes.Equal(got, want) {
			t.Fatalf("subscriber %d got %d bytes, a per-client encode is %d", i, len(got), len(want))
		}
	}
}

// Clients that diverge — one dropped a packet — must each get a delta against
// their own baseline, not a shared one.
func TestDivergedClientsGetTheirOwnBaselines(t *testing.T) {
	r := roomWithPlayers(t, 6)
	fresh, stale := &recorder{}, &recorder{}
	r.Subscribe(1, fresh.send)
	r.Subscribe(2, stale.send)

	step(r, map[sim.PlayerID]sim.Input{1: {MX: 1, Seq: 1}})
	oldTick := r.LastTick()
	step(r, map[sim.PlayerID]sim.Input{1: {MX: 1, Seq: 2}})
	newTick := r.LastTick()

	r.recordAck(1, newTick)
	r.recordAck(2, oldTick)

	step(r, map[sim.PlayerID]sim.Input{1: {MX: 1, Seq: 3}})

	decode := func(b []byte) *pb.Snapshot {
		t.Helper()
		var e pb.Envelope
		if err := proto.Unmarshal(b, &e); err != nil {
			t.Fatal(err)
		}
		return e.GetSnapshot()
	}
	if got := decode(fresh.last()).BaselineTick; got != newTick {
		t.Fatalf("fresh client deltaed from %d, want %d", got, newTick)
	}
	if got := decode(stale.last()).BaselineTick; got != oldTick {
		t.Fatalf("stale client deltaed from %d, want %d", got, oldTick)
	}
	if bytes.Equal(fresh.last(), stale.last()) {
		t.Fatal("clients on different baselines were handed the same bytes")
	}
}

// Whatever the grouping does, every client must still be able to rebuild the
// exact world state from what it received.
//
// The client here is modelled the way a real one works: it keeps the snapshot
// it last *acknowledged* as its baseline, because that is the tick the server
// is encoding against. Keeping only the newest applied state instead would
// leave it unable to apply the next delta, which is precisely the failure the
// ack-based scheme exists to prevent.
func TestEveryClientRebuildsTheSameWorld(t *testing.T) {
	r := roomWithPlayers(t, 8)
	const clients = 5
	recs := make([]*recorder, clients)
	baseline := make([]*pb.Snapshot, clients) // last acked
	current := make([]*pb.Snapshot, clients)  // last applied
	for i := range recs {
		recs[i] = &recorder{}
		r.Subscribe(uint32(i+1), recs[i].send)
	}

	decode := func(b []byte) *pb.Snapshot {
		var e pb.Envelope
		if err := proto.Unmarshal(b, &e); err != nil {
			t.Fatal(err)
		}
		return e.GetSnapshot()
	}

	for tick := 0; tick < 40; tick++ {
		in := map[sim.PlayerID]sim.Input{
			1: {MX: int8(tick%3 - 1), Aim: int16(tick * 7), Fire: tick%4 == 0, Seq: uint32(tick + 1)},
			2: {MY: int8(tick%3 - 1), Aim: int16(tick * 11), Seq: uint32(tick + 1)},
		}
		step(r, in)

		for i := range recs {
			msg := decode(recs[i].last())
			next := ApplyDelta(baseline[i], msg)
			if next == nil {
				t.Fatalf("tick %d: client %d could not apply a delta against the tick it acked (baseline %d)",
					tick, i, msg.BaselineTick)
			}
			current[i] = next
			// Half the clients ack every tick, the rest only every third — so
			// the room really does hold several distinct baselines at once.
			if i%2 == 0 || tick%3 == 0 {
				baseline[i] = next
				r.recordAck(uint32(i+1), next.Tick)
			}
		}
	}

	truth := r.ring[r.LastTick()%SnapshotHistory]
	for i, got := range current {
		if got.Tick != truth.Tick {
			t.Fatalf("client %d is at tick %d, world is at %d", i, got.Tick, truth.Tick)
		}
		if !sameWorld(got, truth) {
			t.Fatalf("client %d rebuilt a different world", i)
		}
	}
}

// A client that stalls past the ring must be caught up with a full snapshot
// rather than a delta it has no baseline for.
func TestStalledClientFallsBackToAFullSnapshot(t *testing.T) {
	r := roomWithPlayers(t, 4)
	rec := &recorder{}
	r.Subscribe(1, rec.send)

	step(r, nil)
	r.recordAck(1, r.LastTick())
	for i := 0; i < SnapshotHistory+2; i++ {
		step(r, nil)
	}
	var e pb.Envelope
	if err := proto.Unmarshal(rec.last(), &e); err != nil {
		t.Fatal(err)
	}
	if got := e.GetSnapshot().BaselineTick; got != 0 {
		t.Fatalf("stalled client was sent a delta against tick %d, %d ticks in the past",
			got, r.LastTick()-got)
	}
}

func sameWorld(a, b *pb.Snapshot) bool {
	if len(a.Players) != len(b.Players) || len(a.Projectiles) != len(b.Projectiles) {
		return false
	}
	byID := func(ps []*pb.PlayerSnap) map[uint32]*pb.PlayerSnap {
		m := make(map[uint32]*pb.PlayerSnap, len(ps))
		for _, p := range ps {
			m[p.Id] = p
		}
		return m
	}
	am, bm := byID(a.Players), byID(b.Players)
	for id, ap := range am {
		bp, ok := bm[id]
		if !ok || ap.X != bp.X || ap.Y != bp.Y || ap.Aim != bp.Aim ||
			ap.Hp != bp.Hp || ap.Score != bp.Score || ap.Bot != bp.Bot || ap.Seq != bp.Seq {
			return false
		}
	}
	pj := func(qs []*pb.ProjSnap) map[uint32][2]int32 {
		m := make(map[uint32][2]int32, len(qs))
		for _, q := range qs {
			m[q.Id] = [2]int32{q.X, q.Y}
		}
		return m
	}
	aq, bq := pj(a.Projectiles), pj(b.Projectiles)
	for id, v := range aq {
		if bq[id] != v {
			return false
		}
	}
	return true
}

// A client that has acked nothing, or whose ack fell out of the ring, gets the
// one shared full encode — not a delta it cannot apply.
func TestClientsWithoutABaselineGetTheFullSnapshot(t *testing.T) {
	r := roomWithPlayers(t, 4)
	acked, silent := &recorder{}, &recorder{}
	r.Subscribe(1, acked.send)
	r.Subscribe(2, silent.send)

	step(r, nil)
	r.recordAck(1, r.LastTick())
	step(r, nil)

	var e pb.Envelope
	if err := proto.Unmarshal(silent.last(), &e); err != nil {
		t.Fatal(err)
	}
	if e.GetSnapshot().BaselineTick != 0 {
		t.Fatal("a client that acked nothing was sent a delta")
	}
	if err := proto.Unmarshal(acked.last(), &e); err != nil {
		t.Fatal(err)
	}
	if e.GetSnapshot().BaselineTick == 0 {
		t.Fatal("a client that acked was sent a full snapshot")
	}
}

// Grouping must not change how many messages a client receives.
func TestEverySubscriberGetsExactlyOneMessagePerTick(t *testing.T) {
	r := roomWithPlayers(t, 6)
	recs := make([]*recorder, 6)
	for i := range recs {
		recs[i] = &recorder{}
		r.Subscribe(uint32(i+1), recs[i].send)
	}
	const ticks = 10
	for i := 0; i < ticks; i++ {
		step(r, nil)
		for j := range recs {
			r.recordAck(uint32(j+1), r.LastTick())
		}
	}
	for i, rec := range recs {
		rec.mu.Lock()
		n := len(rec.msgs)
		rec.mu.Unlock()
		if n != ticks {
			t.Fatalf("subscriber %d received %d messages over %d ticks", i, n, ticks)
		}
	}
}

func BenchmarkBroadcast(b *testing.B) {
	for _, n := range []int{2, 8, 16, 24} {
		b.Run(nameFor(n), func(b *testing.B) {
			roster := make([]sim.Player, 0, n)
			for i := 1; i <= n; i++ {
				roster = append(roster, sim.Player{ID: sim.PlayerID(i)})
			}
			r := New(Params{ID: "b", Seed: 1, TickRate: 20, MatchTicks: 1 << 30, Roster: roster})
			for i := 1; i <= n; i++ {
				r.Subscribe(uint32(i), func([]byte) {})
			}
			pending := map[sim.PlayerID]sim.Input{}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Advance rather than Step, so this measures the tick the room
				// actually runs. See sim.Advance.
				snap := r.world.Advance(pending)
				r.broadcast(snap)
				for _, s := range r.currentSubs().list {
					s.ack.Store(snap.Tick)
				}
			}
		})
	}
}

func nameFor(n int) string {
	switch n {
	case 2:
		return "2players"
	case 8:
		return "8players"
	case 16:
		return "16players"
	default:
		return "24players"
	}
}

// BenchmarkBroadcastParallel is BenchmarkBroadcast with many rooms ticking at
// once, which is what a game server actually does — at 8 players and 10k CCU,
// around 1250 of them.
//
// The serial benchmark cannot see the cost that matters at that scale. Rooms
// share nothing but the process, so their broadcasts should scale with the
// cores available; anything in here touching a process-global cache line shows
// up as this benchmark failing to speed up under -cpu, and as nothing at all
// under the serial one.
func BenchmarkBroadcastParallel(b *testing.B) {
	const n = 8
	roster := make([]sim.Player, 0, n)
	for i := 1; i <= n; i++ {
		roster = append(roster, sim.Player{ID: sim.PlayerID(i)})
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		r := New(Params{ID: "b", Seed: 1, TickRate: 20, MatchTicks: 1 << 30, Roster: roster})
		for i := 1; i <= n; i++ {
			r.Subscribe(uint32(i), func([]byte) {})
		}
		pending := map[sim.PlayerID]sim.Input{}
		for pb.Next() {
			snap := r.world.Advance(pending)
			r.broadcast(snap)
			for i := 1; i <= n; i++ {
				r.recordAck(uint32(i), snap.Tick)
			}
		}
	})
}
