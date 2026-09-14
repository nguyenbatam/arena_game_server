package room

import (
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

// duelRoom is two players close enough that one can shoot the other on demand.
func duelRoom(t *testing.T) *Room {
	t.Helper()
	r := New(Params{ID: "ev", Seed: 3, TickRate: 20, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}, {ID: 2}}})
	ps := r.world.Players()
	ps[0].X, ps[0].Y, ps[0].Aim = 200_000, 1_000_000, 0
	ps[1].X, ps[1].Y = 200_000+218_000, 1_000_000
	return r
}

// fireOnce puts one shot in the air and ticks until it lands, returning every
// message the subscriber was sent.
func fireOnce(r *Room, ticks int) {
	tickOnce(r, map[sim.PlayerID]sim.Input{1: {Fire: true, Aim: 0, Seq: r.world.Players()[0].Seq + 1}})
	for i := 0; i < ticks; i++ {
		tickOnce(r, nil)
	}
}

func eventsIn(t *testing.T, msgs [][]byte) []*pb.GameEvent {
	t.Helper()
	var out []*pb.GameEvent
	for _, m := range msgs {
		out = append(out, decodeSnap(t, m).Events...)
	}
	return out
}

// A hit reaches the client as an event, with the shooter named.
func TestAHitReachesTheClient(t *testing.T) {
	r := duelRoom(t)
	rec := &recorder{}
	r.Subscribe(1, rec.send)
	fireOnce(r, 10)

	evs := eventsIn(t, rec.all())
	var hits int
	for _, e := range evs {
		if e.Kind == pb.GameEventKind_GAME_EVENT_KIND_HIT {
			hits++
			if e.Actor != 1 || e.Target != 2 {
				t.Fatalf("hit event actor=%d target=%d, want 1 and 2", e.Actor, e.Target)
			}
		}
	}
	if hits != 1 {
		t.Fatalf("client saw %d hit events, want 1", hits)
	}
}

// The property the whole design rests on: a client that misses packets is owed
// every event it did not see, and gets them in the first message it can apply.
//
// That is what makes this reliable without a channel or an ack scheme of its
// own — the delta is already encoded against a tick the client confirmed, so
// the window that decides which state to resend decides which events it has
// not seen. Losing a packet makes the next message carry more of both.
func TestAClientThatMissesPacketsStillGetsEveryEvent(t *testing.T) {
	r := duelRoom(t)
	rec := &recorder{}
	r.Subscribe(1, rec.send)

	// One tick so the client has something to ack, then it acks and goes quiet.
	tickOnce(r, nil)
	first := decodeSnap(t, rec.last())
	r.recordAck(1, first.Tick)

	// Several shots land while the client is receiving nothing it can use.
	// Everything after the ack is a delta against that same baseline.
	for shot := 0; shot < 3; shot++ {
		r.world.Players()[1].HP = sim.MaxHP
		r.world.Players()[0].Cooldown = 0
		fireOnce(r, 12)
	}

	// The newest message alone — which is all a client that dropped the rest
	// would have — must carry every hit.
	last := decodeSnap(t, rec.last())
	if last.BaselineTick != first.Tick {
		t.Fatalf("baseline %d, want the acked tick %d", last.BaselineTick, first.Tick)
	}
	hits := 0
	for _, e := range last.Events {
		if e.Kind == pb.GameEventKind_GAME_EVENT_KIND_HIT {
			hits++
		}
	}
	if hits != 3 {
		t.Fatalf("the newest delta carried %d hits, want all 3 the client missed", hits)
	}
}

// A client that keeps up is owed only the newest tick — the steady state, and
// the case that must not quietly resend a window's worth of events every tick.
//
// The ack has to be interleaved with the ticking, not applied afterwards: a
// client that acks only at the end was never keeping up, and every message it
// received was a delta against the same old baseline.
func TestAnUpToDateClientIsOwedOnlyTheNewestTick(t *testing.T) {
	r := duelRoom(t)
	rec := &recorder{}
	r.Subscribe(1, rec.send)

	seen := map[[3]uint32]int{}
	drainNew := func(from int) int {
		msgs := rec.all()
		for _, m := range msgs[from:] {
			s := decodeSnap(t, m)
			for _, e := range s.Events {
				seen[[3]uint32{e.Tick, uint32(e.Kind), e.Target}]++
			}
			r.recordAck(1, s.Tick)
		}
		return len(msgs)
	}

	read := 0
	for i := 0; i < 40; i++ {
		r.world.Players()[1].HP = sim.MaxHP
		r.world.Players()[1].Protect = 0
		r.world.Players()[0].Cooldown = 0
		tickOnce(r, map[sim.PlayerID]sim.Input{1: {Fire: true, Aim: 0, Seq: uint32(i + 1)}})
		read = drainNew(read)
	}

	if len(seen) == 0 {
		t.Fatal("no events were produced; the test proves nothing")
	}
	for key, n := range seen {
		if n > 1 {
			t.Fatalf("event %v was sent %d times to a client that acked every tick", key, n)
		}
	}
}

// A full snapshot carries only the tick it describes. A client with no baseline
// has just resynced and has nothing to attach older events to.
func TestAFullSnapshotCarriesOnlyItsOwnTick(t *testing.T) {
	r := duelRoom(t)
	rec := &recorder{}
	r.Subscribe(1, rec.send)
	fireOnce(r, 10)

	for _, m := range rec.all() {
		s := decodeSnap(t, m)
		if s.BaselineTick != 0 {
			continue
		}
		for _, e := range s.Events {
			if e.Tick != s.Tick {
				t.Fatalf("full snapshot for tick %d carried an event from tick %d", s.Tick, e.Tick)
			}
		}
	}
}

// The span is capped, because a message that outgrows one datagram is dropped
// by the UDP transport and costs the state as well as the events.
func TestTheEventSpanIsCapped(t *testing.T) {
	r := duelRoom(t)
	rec := &recorder{}
	r.Subscribe(1, rec.send)
	tickOnce(r, nil)
	r.recordAck(1, r.lastTick.Load())

	// Far more events than the cap, all inside one baseline window — the whole
	// run has to stay under SnapshotHistory ticks, or the client falls off the
	// ring and is sent a full snapshot instead, which is a different test.
	for i := 0; i < MaxSnapshotEvents*2; i++ {
		// Kept alive and unprotected so every shot is a plain hit: a death
		// would bring respawn immunity with it and stop the stream.
		r.world.Players()[1].HP = sim.MaxHP
		r.world.Players()[1].Protect = 0
		r.world.Players()[0].Cooldown = 0
		tickOnce(r, map[sim.PlayerID]sim.Input{1: {Fire: true, Aim: 0, Seq: uint32(i + 2)}})
	}
	// Let the last shots in the air land.
	for i := 0; i < 8; i++ {
		r.world.Players()[1].HP = sim.MaxHP
		r.world.Players()[1].Protect = 0
		tickOnce(r, nil)
	}
	if got := r.lastWorld.Load(); got >= SnapshotHistory {
		t.Fatalf("setup ran %d ticks, past the %d-tick history window", got, SnapshotHistory)
	}

	// Everything the match actually produced, straight out of the ring.
	var produced []*pb.GameEvent
	for tick := uint32(1); tick <= r.lastWorld.Load(); tick++ {
		if slot := r.ring[tick%SnapshotHistory]; slot != nil && slot.Tick == tick {
			produced = append(produced, slot.Events...)
		}
	}
	if len(produced) <= MaxSnapshotEvents {
		t.Fatalf("only %d events were produced; the cap of %d was never reached",
			len(produced), MaxSnapshotEvents)
	}

	last := decodeSnap(t, rec.last())
	if len(last.Events) != MaxSnapshotEvents {
		t.Fatalf("a delta carried %d events, want exactly the cap of %d",
			len(last.Events), MaxSnapshotEvents)
	}

	// Overflow keeps the newest: a killfeed missing its oldest lines is still a
	// killfeed, and the alternative is a message that does not arrive.
	want := produced[len(produced)-MaxSnapshotEvents:]
	for i, e := range last.Events {
		w := want[i]
		if e.Tick != w.Tick || e.Kind != w.Kind || e.Target != w.Target || e.Actor != w.Actor {
			t.Fatalf("kept event %d is %v, want the suffix entry %v", i, e, w)
		}
	}
}
