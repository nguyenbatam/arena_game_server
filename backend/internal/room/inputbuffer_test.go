package room

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

// bufRoom is a room built for driving the input path by hand: no goroutine, no
// clock. enqueue and fillPending both belong to the tick goroutine, so calling
// them directly is exactly what the loop does, minus the waiting.
func bufRoom(t *testing.T, depth int) *Room {
	t.Helper()
	return New(Params{
		ID: "buf", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Roster: RosterFromIDs([]uint32{1, 2}, 0), InputBuffer: depth,
	})
}

func in(seq uint32, mx int8) *pb.Input {
	return &pb.Input{PlayerId: 1, Seq: seq, Mx: int32(mx)}
}

// tick runs one pass of the input half of the loop and returns what the
// simulation would have been handed.
func tick(r *Room, arrivals ...*pb.Input) map[sim.PlayerID]sim.Input {
	for _, a := range arrivals {
		r.enqueue(arrivalFrom(a))
	}
	pending := make(map[sim.PlayerID]sim.Input, 4)
	r.fillPending(pending)
	return pending
}

// The whole point of the buffer. Two inputs arrive inside one tick window —
// which the slightest jitter produces, since a client at the tick rate sends
// one per tick — and the second is spent on the window that receives nothing,
// instead of being thrown away.
func TestBurstIsSpentOnTheFollowingGap(t *testing.T) {
	r := bufRoom(t, 2)

	first := tick(r, in(1, 1), in(2, 1))
	if got := first[1].Seq; got != 1 {
		t.Fatalf("first tick applied seq %d, want the older input (1)", got)
	}

	second := tick(r) // nothing arrives
	if got := second[1].Seq; got != 2 {
		t.Fatalf("the gap applied seq %d, want the input held back (2)", got)
	}

	third := tick(r)
	if _, ok := third[1]; ok {
		t.Fatal("the buffer produced an input it was never sent")
	}
}

// With no jitter there is nothing to absorb, and the buffer must not become a
// delay: one input per tick is applied on the tick it arrives.
func TestSteadyStreamIsNotDelayed(t *testing.T) {
	r := bufRoom(t, 2)
	for seq := uint32(1); seq <= 10; seq++ {
		got := tick(r, in(seq, 1))
		if got[1].Seq != seq {
			t.Fatalf("tick %d applied seq %d — the buffer added latency to a steady stream", seq, got[1].Seq)
		}
		if n := len(r.inboxes[1].q); n != 0 {
			t.Fatalf("tick %d left %d input(s) queued with nothing to absorb", seq, n)
		}
	}
}

// Order is the other half of the contract: inputs are movement deltas, and
// applying them out of order moves the player somewhere they never went.
func TestInputsAreAppliedInArrivalOrder(t *testing.T) {
	r := bufRoom(t, 3)
	tick(r, in(1, 1), in(2, 1), in(3, 1))

	// One was applied on the tick they arrived; the rest come out in order.
	for want := uint32(2); want <= 3; want++ {
		if got := tick(r)[1].Seq; got != want {
			t.Fatalf("applied seq %d, want %d", got, want)
		}
	}
}

// A client whose clock runs fast produces more inputs than the server consumes.
// Left alone it would build a backlog that never drains, and every input it
// sent would be simulated further into the past. The trim keeps that player in
// the present at the cost of the frames it drops.
func TestAClientRunningAheadIsTrimmedToTheTarget(t *testing.T) {
	r := bufRoom(t, 2)

	// Six inputs in one window against a target of two: the two newest survive,
	// one of which is applied now.
	got := tick(r, in(1, 1), in(2, 1), in(3, 1), in(4, 1), in(5, 1), in(6, 1))
	if got[1].Seq != 5 {
		t.Fatalf("applied seq %d, want 5 — the trim should keep the newest two", got[1].Seq)
	}
	if n := len(r.inboxes[1].q); n != 1 {
		t.Fatalf("%d inputs left queued, want 1", n)
	}
	if next := tick(r)[1].Seq; next != 6 {
		t.Fatalf("next tick applied seq %d, want 6", next)
	}
}

// Nothing a client sends may grow the queue without bound between two ticks,
// whatever the rate limiter upstream let through.
func TestQueueIsCappedBetweenTicks(t *testing.T) {
	r := bufRoom(t, 2)
	for seq := uint32(1); seq <= 200; seq++ {
		r.enqueue(arrivalFrom(in(seq, 1)))
	}
	if n := len(r.inboxes[1].q); n > inputQueueCap {
		t.Fatalf("queue grew to %d, past the cap of %d", n, inputQueueCap)
	}
	// What survived is the newest, so the player is current rather than behind.
	if got := r.inboxes[1].q[len(r.inboxes[1].q)-1].in.Seq; got != 200 {
		t.Fatalf("newest queued input is seq %d, want 200", got)
	}
}

// A packet that has not arrived cannot be simulated. The player holds position
// — the same as before the buffer existed — rather than having an input
// invented for them.
func TestNothingArrivedMeansNoInput(t *testing.T) {
	r := bufRoom(t, 2)
	tick(r, in(1, 1))
	if got := tick(r); len(got) != 0 {
		t.Fatalf("a tick with nothing queued handed the simulation %v", got)
	}
}

// An inbox that has been silent far longer than any live connection's gap
// belongs to somebody who has gone. It is dropped, so the map stays bounded by
// who is actually playing.
func TestIdleInboxIsForgotten(t *testing.T) {
	r := bufRoom(t, 2)
	tick(r, in(1, 1))
	if _, ok := r.inboxes[1]; !ok {
		t.Fatal("no inbox was created for a player who sent input")
	}

	// Pretend the room has ticked well past the idle window.
	setTestClock(r, idleTicks+2)
	tick(r)
	if _, ok := r.inboxes[1]; ok {
		t.Fatal("an inbox silent past the idle window was kept")
	}
}

// The lag a shot is compensated for is measured when it is simulated, not when
// it arrived. An input held back for a tick was produced one tick further in
// the past, and compensating it as though it were fresh would put the bullet
// somewhere the shooter never saw.
func TestLagIsMeasuredWhenTheInputIsApplied(t *testing.T) {
	r := bufRoom(t, 2)
	// The room has finished and broadcast tick 100, so the next Advance — the
	// one these inputs are being gathered for — produces tick 101.
	setTestClock(r, 100)

	// Both produced against tick 95, so six ticks of lag when applied at 101.
	r.enqueue(arrivalFrom(&pb.Input{PlayerId: 1, Seq: 1, Mx: 1, AckTick: 95}))
	r.enqueue(arrivalFrom(&pb.Input{PlayerId: 1, Seq: 2, Mx: 1, AckTick: 95}))

	first := make(map[sim.PlayerID]sim.Input)
	r.fillPending(first)
	if got := first[1].LagTicks; got != 6 {
		t.Fatalf("lag %d on the tick it arrived, want 6", got)
	}

	// The world moves on a tick, and the held input is now seven ticks old.
	setTestClock(r, 101)
	second := make(map[sim.PlayerID]sim.Input)
	r.fillPending(second)
	if got := second[1].LagTicks; got != 7 {
		t.Fatalf("lag %d for an input held one tick, want 7", got)
	}
}

// The freshest ack a client can possibly send is the tick the room has just
// broadcast, and its input still cannot land before the next one. That is one
// tick of lag, not zero: the world the shooter was looking at is already a tick
// old by the time the server acts on what they did about it.
//
// Measuring against the finished tick used to score this as zero, which meant
// the client with the best connection in the match was the one compensated
// least faithfully.
func TestFreshestPossibleAckStillCountsATick(t *testing.T) {
	r := bufRoom(t, 2)
	setTestClock(r, 100)
	r.enqueue(arrivalFrom(&pb.Input{PlayerId: 1, Seq: 1, Fire: true, AckTick: 100}))

	got := make(map[sim.PlayerID]sim.Input)
	r.fillPending(got)
	if l := got[1].LagTicks; l != 1 {
		t.Fatalf("lag %d for an ack of the tick just broadcast, want 1", l)
	}
}

// An ack from the future is not a client that is ahead, it is a client that is
// lying or confused. Either way there is no lag to compensate, and the arrival
// must not underflow its way to a huge one.
func TestAckAtOrBeyondTheTickBeingBuiltIsNoLag(t *testing.T) {
	r := bufRoom(t, 2)
	setTestClock(r, 100)
	for _, ack := range []uint32{101, 102, 5000} {
		r.enqueue(arrivalFrom(&pb.Input{PlayerId: 1, Seq: 1, Fire: true, AckTick: ack}))
		got := make(map[sim.PlayerID]sim.Input)
		r.fillPending(got)
		if l := got[1].LagTicks; l != 0 {
			t.Fatalf("ack %d gave lag %d, want 0", ack, l)
		}
	}
}

// Compensation stays capped however long an input waits: refusing to
// acknowledge must not buy unbounded advantage, and neither must a full queue.
func TestHeldInputStillCannotExceedTheLagCap(t *testing.T) {
	r := bufRoom(t, 2)
	setTestClock(r, 1000)
	r.enqueue(arrivalFrom(&pb.Input{PlayerId: 1, Seq: 1, Fire: true, AckTick: 1}))

	got := make(map[sim.PlayerID]sim.Input)
	r.fillPending(got)
	if got[1].LagTicks != sim.MaxLagCompTicks {
		t.Fatalf("lag %d, want it capped at %d", got[1].LagTicks, sim.MaxLagCompTicks)
	}
}

// A depth of one is the behaviour the room had before the buffer: a window that
// receives two inputs keeps the newer and discards the other.
func TestDepthOfOneIsTheOldLastWinsBehaviour(t *testing.T) {
	r := bufRoom(t, 1)
	got := tick(r, in(1, 1), in(2, 1))
	if got[1].Seq != 2 {
		t.Fatalf("applied seq %d, want the newest (2)", got[1].Seq)
	}
	if n := len(r.inboxes[1].q); n != 0 {
		t.Fatalf("%d input(s) held at depth one, want none", n)
	}
}

// Queues are per player: one client bursting must not delay or displace
// anybody else's input.
func TestQueuesAreIndependentPerPlayer(t *testing.T) {
	r := bufRoom(t, 2)
	r.enqueue(arrivalFrom(&pb.Input{PlayerId: 1, Seq: 1, Mx: 1}))
	r.enqueue(arrivalFrom(&pb.Input{PlayerId: 1, Seq: 2, Mx: 1}))
	r.enqueue(arrivalFrom(&pb.Input{PlayerId: 2, Seq: 7, Mx: -1}))

	first := make(map[sim.PlayerID]sim.Input)
	r.fillPending(first)
	if first[1].Seq != 1 || first[2].Seq != 7 {
		t.Fatalf("applied %v, want player 1 on seq 1 and player 2 on seq 7", first)
	}

	second := make(map[sim.PlayerID]sim.Input)
	r.fillPending(second)
	if second[1].Seq != 2 {
		t.Fatalf("player 1 applied seq %d, want the held 2", second[1].Seq)
	}
	if _, ok := second[2]; ok {
		t.Fatal("player 2 was handed an input they never sent")
	}
}

// countingRecorder collects what the simulation was actually given, tick by
// tick. The room hands the recorder the same map it hands the world, so this
// counts applications rather than arrivals — which is the thing under test.
type countingRecorder struct {
	mu      sync.Mutex
	applied map[sim.PlayerID][]uint32
}

func (c *countingRecorder) Frame(_ uint32, inputs map[sim.PlayerID]sim.Input) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, v := range inputs {
		c.applied[id] = append(c.applied[id], v.Seq)
	}
}

func (c *countingRecorder) Depart(sim.PlayerID) {}
func (c *countingRecorder) Rejoin(sim.PlayerID) {}

func (c *countingRecorder) Close(uint32, uint64) error { return nil }

func (c *countingRecorder) seqs(id sim.PlayerID) []uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]uint32(nil), c.applied[id]...)
}

// End to end, through the real tick loop: a burst that lands between two ticks
// is simulated in full rather than collapsed into its last member. Before the
// buffer this test would see one of the four inputs applied and the other three
// gone — a tenth of a second of a player's movement, discarded after the server
// had already received it.
func TestRoomAppliesEveryInputOfABurst(t *testing.T) {
	rec := &countingRecorder{applied: map[sim.PlayerID][]uint32{}}
	r := New(Params{
		ID: "burst", Seed: 5, TickRate: 20, MatchTicks: 1 << 20,
		Roster: RosterFromIDs([]uint32{1}, 0), Recorder: rec, InputBuffer: 4,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	defer r.Stop()

	for seq := uint32(1); seq <= 4; seq++ {
		r.SubmitInput(&pb.Input{PlayerId: 1, Seq: seq, Mx: 1})
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.seqs(1)) >= 4 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	got := rec.seqs(1)
	if len(got) < 4 {
		t.Fatalf("the simulation saw %v — a burst of four was collapsed", got)
	}
	for i, seq := range got[:4] {
		if seq != uint32(i+1) {
			t.Fatalf("simulated %v, want 1,2,3,4 in order", got[:4])
		}
	}
}

// The buffer must not break the property everything else rests on: the same
// seed and the same inputs produce the same world.
func TestBufferedInputsStayDeterministic(t *testing.T) {
	run := func() uint64 {
		r := bufRoom(t, 2)
		w := r.world
		for step := uint32(1); step <= 40; step++ {
			pending := make(map[sim.PlayerID]sim.Input)
			if step%3 == 0 {
				// A burst every third tick, so the buffer is doing work.
				r.enqueue(arrivalFrom(&pb.Input{PlayerId: 1, Seq: step, Mx: 1, Fire: step%9 == 0}))
				r.enqueue(arrivalFrom(&pb.Input{PlayerId: 2, Seq: step, My: 1}))
			}
			r.fillPending(pending)
			w.Step(pending)
			setTestClock(r, step)
		}
		return w.Checksum()
	}
	if a, b := run(), run(); a != b {
		t.Fatalf("two identical runs through the buffer gave %#x and %#x", a, b)
	}
}

// A client that draws other players in the past aimed at a world older than the
// tick it acked, and lag compensation owes it that on top of the round trip.
// This is the Client View Interpolation term in Source's formula, and leaving
// it out means compensating a world the shooter was never shown.
func TestInterpolationDelayIsAddedToTheRoundTrip(t *testing.T) {
	r := bufRoom(t, 2)
	setTestClock(r, 100)

	// Acked 97, applied at 101: four ticks of round trip. 100 ms of render
	// delay at 20 Hz is two more.
	r.enqueue(arrivalFrom(&pb.Input{PlayerId: 1, Seq: 1, Fire: true, AckTick: 97, InterpMs: 100}))

	got := make(map[sim.PlayerID]sim.Input)
	r.fillPending(got)
	if l := got[1].LagTicks; l != 6 {
		t.Fatalf("lag %d, want 4 ticks of round trip plus 2 of interpolation", l)
	}
}

// The same declared delay is a different number of ticks in a room that ticks
// faster, which is why it crosses the wire in milliseconds. A client does not
// choose its render delay as a function of a rate the server can change under
// it, so converting on the client would be converting against a guess.
func TestInterpolationIsConvertedAtTheRoomsRate(t *testing.T) {
	for _, tc := range []struct {
		rate int
		want uint8
	}{
		{20, 2}, // 50ms ticks
		{30, 3}, // 33.3ms ticks
		{60, 6}, // 16.7ms ticks
	} {
		r := bufRoom(t, 2)
		r.tickDur = time.Second / time.Duration(tc.rate)
		if got := r.interpTicks(100); got != tc.want {
			t.Errorf("100ms at %d Hz = %d ticks, want %d", tc.rate, got, tc.want)
		}
	}
}

// A delay that does not divide the tick evenly is rounded, not truncated:
// throwing away most of a tick because a client picked 90 ms instead of 100 is
// the same class of quiet undercompensation the round-trip term used to have.
func TestInterpolationRoundsRatherThanTruncates(t *testing.T) {
	r := bufRoom(t, 2)
	r.tickDur = 50 * time.Millisecond
	for _, tc := range []struct {
		ms   uint16
		want uint8
	}{
		{0, 0},
		{24, 0},
		{25, 1}, // exactly half a tick rounds up
		{74, 1},
		{90, 2}, // 1.8 ticks is much nearer two than one
		{100, 2},
	} {
		if got := r.interpTicks(tc.ms); got != tc.want {
			t.Errorf("%dms at 20 Hz = %d ticks, want %d", tc.ms, got, tc.want)
		}
	}
}

// The declared render delay is the one number on this path a client chooses, so
// it is a dial. The clamp is what makes it not worth turning, and it lives at
// the wire boundary so nothing downstream ever holds an unclamped figure.
func TestDeclaredInterpolationIsClampedAtTheWire(t *testing.T) {
	got := arrivalFrom(&pb.Input{PlayerId: 1, Seq: 1, AckTick: 1, InterpMs: 60000})
	if got.interpMs != maxInterpMs {
		t.Fatalf("a claim of 60s survived as %dms, want it clamped to %d", got.interpMs, maxInterpMs)
	}
	if honest := arrivalFrom(&pb.Input{PlayerId: 1, Seq: 1, AckTick: 1, InterpMs: 100}); honest.interpMs != 100 {
		t.Fatalf("an honest 100ms was altered to %dms", honest.interpMs)
	}
}

// Round trip and interpolation are capped as a sum, not one at a time. Capping
// the terms separately would let a client reach twice as far into the past as
// MaxLagCompTicks is meant to allow, which is the whole point of having it.
func TestTheCapAppliesToTheSumOfBothTerms(t *testing.T) {
	r := bufRoom(t, 2)
	setTestClock(r, 100)
	// Eight ticks of round trip, three more of declared interpolation: eleven,
	// over the cap of ten.
	r.enqueue(arrivalFrom(&pb.Input{PlayerId: 1, Seq: 1, Fire: true, AckTick: 93, InterpMs: 150}))

	got := make(map[sim.PlayerID]sim.Input)
	r.fillPending(got)
	if l := got[1].LagTicks; l != sim.MaxLagCompTicks {
		t.Fatalf("lag %d, want it capped at %d", l, sim.MaxLagCompTicks)
	}
}

// Interpolation is compensation for a world the client was shown. A client that
// has been shown nothing has none coming, however much delay it declares —
// otherwise the first input of a connection is a free rewind.
func TestInterpolationBuysNothingWithoutAnAck(t *testing.T) {
	r := bufRoom(t, 2)
	setTestClock(r, 100)
	r.enqueue(arrivalFrom(&pb.Input{PlayerId: 1, Seq: 1, Fire: true, AckTick: 0, InterpMs: 150}))

	got := make(map[sim.PlayerID]sim.Input)
	r.fillPending(got)
	if l := got[1].LagTicks; l != 0 {
		t.Fatalf("lag %d for an input that acked nothing, want 0", l)
	}
}
