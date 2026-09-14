package room

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/metrics"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// rosterRecorder is a Recorder that only counts the roster events, which is the
// half of the interface these tests are about.
type rosterRecorder struct {
	mu               sync.Mutex
	departs, rejoins int
}

func (r *rosterRecorder) Frame(uint32, map[sim.PlayerID]sim.Input) {}

func (r *rosterRecorder) Depart(sim.PlayerID) {
	r.mu.Lock()
	r.departs++
	r.mu.Unlock()
}

func (r *rosterRecorder) Rejoin(sim.PlayerID) {
	r.mu.Lock()
	r.rejoins++
	r.mu.Unlock()
}

func (r *rosterRecorder) Close(uint32, uint64) error { return nil }

func (r *rosterRecorder) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.departs, r.rejoins
}

// counter reads a plain Prometheus counter's current value.
func counter(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

func droppedRosterEvents(t *testing.T) float64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.RosterEventsDropped.Write(&m); err != nil {
		t.Fatalf("read arena_roster_events_dropped_total: %v", err)
	}
	return m.GetCounter().GetValue()
}

// runRoom starts a room from explicit params and tears it down with the test.
func runRoom(t *testing.T, p Params) *Room {
	t.Helper()
	r := New(p)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		r.Stop()
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("room goroutine did not exit")
		}
	})
	return r
}

// watch subscribes a seat and reads the world back off the wire.
//
// The world itself belongs to the tick goroutine, so a test may not look at it
// directly — the snapshot is the only view another goroutine is allowed. A
// subscriber that never acks is sent a full snapshot every tick, which is
// exactly what a test wants: no baseline to track, every field present.
func watch(t *testing.T, r *Room, seat uint32) func(id uint32) int32 {
	t.Helper()
	rec := &recorder{}
	r.Subscribe(seat, rec.send)
	return func(id uint32) int32 {
		msg := rec.last()
		if msg == nil {
			return -1
		}
		e, err := protocol.UnmarshalEnv(msg)
		if err != nil {
			t.Fatalf("snapshot did not decode: %v", err)
		}
		snap := e.GetSnapshot()
		if snap == nil {
			return -1
		}
		if snap.BaselineTick != 0 {
			t.Fatalf("a subscriber that never acks must receive full snapshots, got a delta on %d", snap.Tick)
		}
		for _, p := range snap.Players {
			if p.Id == id {
				return p.Hp
			}
		}
		return -1
	}
}

// A seat that leaves is taken out of the simulation, not merely unsubscribed.
//
// Unsubscribing stops the snapshots; it leaves the avatar in the world as a
// motionless target that pays a kill every RespawnTicks to whoever shoots it —
// and those kills reach the ladder.
func TestLeaveTakesTheAvatarOutOfTheWorld(t *testing.T) {
	r := runRoom(t, Params{
		ID: "leave", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}, {ID: 2}},
	})

	hp := watch(t, r, 1)
	r.Leave(2)
	// The linger has to run out before the avatar goes.
	deadline := time.Now().Add(3 * time.Second)
	for hp(2) != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := hp(2); got != 0 {
		t.Fatalf("departed seat still has %d HP after its linger", got)
	}
}

// And comes back when its player does.
func TestRejoinPutsTheAvatarBack(t *testing.T) {
	r := runRoom(t, Params{
		ID: "rejoin", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}, {ID: 2}},
	})

	hp := watch(t, r, 1)
	r.Leave(2)
	deadline := time.Now().Add(3 * time.Second)
	for hp(2) != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if hp(2) != 0 {
		t.Fatal("setup: the seat never departed")
	}

	r.Rejoin(2)
	deadline = time.Now().Add(3 * time.Second)
	for hp(2) <= 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := hp(2); got <= 0 {
		t.Fatalf("seat did not come back: HP %d", got)
	}
}

// Both are safe on a room that has already finished, and on a seat that was
// never in it. A disconnect racing the end of a match is ordinary.
func TestLeaveAndRejoinAreSafeOnAClosedRoom(t *testing.T) {
	r := New(Params{ID: "closed", Seed: 1, TickRate: 20, MatchTicks: 1,
		Roster: []sim.Player{{ID: 1}}})
	r.Stop()
	r.Leave(1)
	r.Rejoin(1)
	r.Leave(999)
}

// The roster queue must not silently swallow a departure, so a full one is
// counted. Filling it takes a room that is not draining — which is the only
// state in which the counter should ever move.
func TestAFullRosterQueueIsCountedNotSwallowed(t *testing.T) {
	r := New(Params{ID: "flood", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}}})
	// The room is never run, so nothing drains the queue.
	for i := 0; i < cmdQueue; i++ {
		r.Leave(1)
	}
	if got := len(r.cmds); got != cmdQueue {
		t.Fatalf("queue holds %d, want it full at %d", got, cmdQueue)
	}
	before := droppedRosterEvents(t)
	r.Leave(1)
	if after := droppedRosterEvents(t); after <= before {
		t.Fatalf("a dropped roster event was not counted: %v -> %v", before, after)
	}
}

// A room that departs a seat must record it, or its replay reproduces a
// different match — the avatar stays in the world and the checksum disagrees
// with the one the server stamped.
func TestDeparturesReachTheRecorder(t *testing.T) {
	rec := &rosterRecorder{}
	r := runRoom(t, Params{
		ID: "recorded", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}, {ID: 2}}, Recorder: rec,
	})

	r.Leave(2)
	r.Rejoin(2)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if d, j := rec.counts(); d == 1 && j == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	d, j := rec.counts()
	t.Fatalf("recorder saw %d departures and %d rejoins, want 1 and 1", d, j)
}

// Lag compensation is accounted for once per room per tick, not once per shot.
//
// The histogram this replaces cost 11 ns uncontended and 275 ns at -cpu 10, and
// a player holding the fire button produces one observation per tick — so at
// 10k CCU it was 200k contended writes a second on one shared set of buckets,
// charged to the goroutines that have a tick budget to keep.
func TestLagCompensationIsAccountedPerTickNotPerShot(t *testing.T) {
	r := New(Params{ID: "lag", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}, {ID: 2}}})
	setTestClock(r, 100)

	// Two players, each with a shot owed compensation, inside one tick.
	for _, id := range []uint32{1, 2} {
		r.enqueue(arrival{playerID: id, ackTick: 90, in: sim.Input{Fire: true, Seq: 1}})
	}
	pending := map[sim.PlayerID]sim.Input{}
	r.fillPending(pending)

	if r.lagShots != 2 {
		t.Fatalf("accumulated %d shots, want 2", r.lagShots)
	}
	if r.lagTicks == 0 {
		t.Fatal("no compensation was accumulated")
	}

	shotsBefore := counter(t, metrics.LagCompShots)
	ticksBefore := counter(t, metrics.LagCompTicks)
	r.flushInputs()

	if got := counter(t, metrics.LagCompShots) - shotsBefore; got != 2 {
		t.Fatalf("flushed %v shots, want 2", got)
	}
	if got := counter(t, metrics.LagCompTicks) - ticksBefore; got <= 0 {
		t.Fatalf("flushed %v ticks of compensation, want more than zero", got)
	}
	if r.lagShots != 0 || r.lagTicks != 0 || r.lagCapped != 0 {
		t.Fatal("the accumulator was not reset by the flush")
	}
}

// A shot reaching straight for the ceiling is counted separately. That ratio is
// the whole detection signal: a population on bad links spreads out below the
// cap, a client turning the dial sits on it.
func TestShotsAtTheCompensationCapAreCountedSeparately(t *testing.T) {
	r := New(Params{ID: "cap", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}}})
	setTestClock(r, 10_000)

	// An ack far enough in the past that lagFor clamps to the cap.
	r.enqueue(arrival{playerID: 1, ackTick: 1, in: sim.Input{Fire: true, Seq: 1}})
	r.fillPending(map[sim.PlayerID]sim.Input{})

	if r.lagCapped != 1 {
		t.Fatalf("capped shots = %d, want 1", r.lagCapped)
	}
	before := counter(t, metrics.LagCompCapped)
	r.flushInputs()
	if got := counter(t, metrics.LagCompCapped) - before; got != 1 {
		t.Fatalf("flushed %v capped shots, want 1", got)
	}
}

// Movement frames are not shots, and a shot owed no compensation is not worth
// counting either — both would bury the signal under the ordinary traffic.
//
// "Owed nothing" means an ack of zero: a client that has acknowledged no tick
// has no rendered world to compensate against. It is deliberately not the same
// as acking the freshest tick in existence, which is still owed one — the world
// advances once more before the input lands, and lagFor measures against the
// tick being built rather than the one just finished.
func TestOnlyCompensatedShotsAreCounted(t *testing.T) {
	r := New(Params{ID: "quiet", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}, {ID: 2}}})
	setTestClock(r, 100)

	// Seat 1 moves with heavy lag but does not shoot; seat 2 shoots having
	// acked nothing at all.
	r.enqueue(arrival{playerID: 1, ackTick: 50, in: sim.Input{MX: 1, Seq: 1}})
	r.enqueue(arrival{playerID: 2, ackTick: 0, in: sim.Input{Fire: true, Seq: 1}})
	r.fillPending(map[sim.PlayerID]sim.Input{})

	if r.lagShots != 0 || r.lagTicks != 0 {
		t.Fatalf("counted %d shots / %d ticks, want nothing", r.lagShots, r.lagTicks)
	}
}

// The freshest possible ack is still a compensated shot, and it is counted.
// This is the boundary the test above is deliberately not standing on.
func TestAShotOnTheFreshestAckIsStillCounted(t *testing.T) {
	r := New(Params{ID: "fresh", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}}})
	setTestClock(r, 100)
	r.enqueue(arrival{playerID: 1, ackTick: 100, in: sim.Input{Fire: true, Seq: 1}})
	r.fillPending(map[sim.PlayerID]sim.Input{})
	if r.lagShots != 1 || r.lagTicks != 1 {
		t.Fatalf("counted %d shots / %d ticks, want 1 and 1", r.lagShots, r.lagTicks)
	}
	if r.lagCapped != 0 {
		t.Fatal("a one-tick shot was counted against the cap")
	}
}
