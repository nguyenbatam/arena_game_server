package room

import (
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

// tickOnce drives one tick the way loop does, without the timer: advance the
// world, move the simulation clock, then broadcast only if this tick is due.
func tickOnce(r *Room, inputs map[sim.PlayerID]sim.Input) {
	snap := r.world.Advance(inputs)
	r.lastWorld.Store(snap.Tick)
	if r.shouldSend(snap) {
		r.broadcast(snap)
	}
}

func rateRoom(t *testing.T, players, every int, matchTicks uint32) *Room {
	t.Helper()
	roster := make([]sim.Player, 0, players)
	for i := 1; i <= players; i++ {
		roster = append(roster, sim.Player{ID: sim.PlayerID(i)})
	}
	return New(Params{
		ID: "rate", Seed: 42, TickRate: 60, MatchTicks: matchTicks,
		Roster: roster, SnapshotEvery: every,
	})
}

// The default is unchanged: every tick goes out. The measured bandwidth table
// in the README is written against this, so it is worth a test of its own
// rather than an assumption.
func TestSnapshotEveryDefaultsToEveryTick(t *testing.T) {
	r := rateRoom(t, 4, 0, 1<<20)
	if r.sendEvery != 1 {
		t.Fatalf("sendEvery = %d, want 1 by default", r.sendEvery)
	}
	rec := &recorder{}
	r.Subscribe(1, rec.send)
	for i := 0; i < 12; i++ {
		tickOnce(r, nil)
	}
	if got := len(rec.all()); got != 12 {
		t.Fatalf("12 ticks produced %d messages, want one each", got)
	}
}

// Simulating three times as often as you send is the whole point: the tick rate
// buys hit resolution, the send rate costs egress.
func TestSimulationRunsFasterThanTheSendRate(t *testing.T) {
	const every = 3
	r := rateRoom(t, 4, every, 1<<20)
	rec := &recorder{}
	r.Subscribe(1, rec.send)

	const ticks = 30
	for i := 0; i < ticks; i++ {
		tickOnce(r, nil)
	}
	msgs := rec.all()
	if want := ticks / every; len(msgs) != want {
		t.Fatalf("%d ticks at one snapshot in %d produced %d messages, want %d",
			ticks, every, len(msgs), want)
	}
	// And the world really did advance every tick, not only on the ones that
	// were sent.
	if got := r.lastWorld.Load(); got != ticks {
		t.Fatalf("world reached tick %d, want %d — the simulation followed the send rate", got, ticks)
	}
}

// Delta encoding still works across a gap: a client acks a tick it was sent,
// and the next snapshot is a delta against exactly that one.
func TestDeltasAreStillEncodedAgainstAnAckedSendTick(t *testing.T) {
	const every = 4
	r := rateRoom(t, 4, every, 1<<20)
	rec := &recorder{}
	r.Subscribe(1, rec.send)

	for i := 0; i < every; i++ {
		tickOnce(r, map[sim.PlayerID]sim.Input{1: {MX: 1, Seq: uint32(i + 1)}})
	}
	first := decodeSnap(t, rec.last())
	if first.BaselineTick != 0 {
		t.Fatalf("first message should be a full snapshot, baseline %d", first.BaselineTick)
	}
	r.recordAck(1, first.Tick)

	for i := 0; i < every; i++ {
		tickOnce(r, map[sim.PlayerID]sim.Input{1: {MX: 1, Seq: uint32(every + i + 1)}})
	}
	second := decodeSnap(t, rec.last())
	if second.BaselineTick != first.Tick {
		t.Fatalf("delta baseline %d, want the tick the client acked (%d)", second.BaselineTick, first.Tick)
	}
	if rebuilt := ApplyDelta(first, second); rebuilt == nil {
		t.Fatal("the client could not apply the delta across the send gap")
	}
}

// An ack may only name a tick that was actually sent. With a send interval the
// simulation runs past ticks no client ever saw, and accepting one of those as
// a baseline would delta against a world the client cannot reconstruct.
func TestAnAckOfATickThatWasNeverSentIsRefused(t *testing.T) {
	const every = 5
	r := rateRoom(t, 2, every, 1<<20)
	rec := &recorder{}
	r.Subscribe(1, rec.send)
	// Deliberately not a multiple of the interval, so the simulation ends a
	// tick past the last one anybody was shown.
	for i := 0; i < every*2+1; i++ {
		tickOnce(r, nil)
	}

	sent := r.lastTick.Load()
	world := r.lastWorld.Load()
	if sent >= world {
		t.Fatalf("setup: sent tick %d should trail the world (%d)", sent, world)
	}
	// A tick the simulation reached but nobody was shown.
	r.recordAck(1, sent+1)
	if base := r.baselineFor(r.currentSubs().byID[1].ack.Load(), world); base != nil && base.Tick == sent+1 {
		t.Fatalf("a tick that was never broadcast was accepted as a baseline")
	}
}

// The last tick of a match always goes out, whatever the interval. Ended is the
// one thing a client cannot wait for the next snapshot to learn.
func TestTheFinalTickIsAlwaysSent(t *testing.T) {
	const every = 7
	// A match length that is deliberately not a multiple of the interval, so
	// the final tick is one the schedule would have skipped.
	const ticks = 10
	if ticks%every == 0 {
		t.Fatal("test setup no longer exercises the skipped case")
	}
	r := rateRoom(t, 2, every, ticks)
	rec := &recorder{}
	r.Subscribe(1, rec.send)
	for i := 0; i < ticks; i++ {
		tickOnce(r, nil)
	}
	last := decodeSnap(t, rec.last())
	if !last.Ended {
		t.Fatalf("the last message is tick %d and not the end of the match", last.Tick)
	}
	if last.Tick != ticks {
		t.Fatalf("final snapshot is tick %d, want %d", last.Tick, ticks)
	}
}

// An interval at or past the history window would make every broadcast a full
// snapshot, because no ack could ever still be in the ring. Clamped rather than
// accepted.
func TestASendIntervalCannotOutrunTheHistoryWindow(t *testing.T) {
	r := rateRoom(t, 2, SnapshotHistory+10, 1<<20)
	if r.sendEvery >= SnapshotHistory {
		t.Fatalf("sendEvery = %d, want it clamped below the %d-tick history", r.sendEvery, SnapshotHistory)
	}
}

// SnapshotEvery is a room-creation setting, so config has to turn a rate in Hz
// into it the same way at every tick rate.
func TestSendEveryIsANonZeroInterval(t *testing.T) {
	for _, every := range []int{-1, 0, 1} {
		r := rateRoom(t, 2, every, 1<<20)
		if r.sendEvery != 1 {
			t.Fatalf("SnapshotEvery %d became %d, want 1", every, r.sendEvery)
		}
	}
}

func decodeSnap(t *testing.T, msg []byte) *pb.Snapshot {
	t.Helper()
	if msg == nil {
		t.Fatal("no message was sent")
	}
	e, err := protocol.UnmarshalEnv(msg)
	if err != nil {
		t.Fatalf("snapshot did not decode: %v", err)
	}
	s := e.GetSnapshot()
	if s == nil {
		t.Fatalf("message is not a snapshot: %v", e.Type)
	}
	return s
}
