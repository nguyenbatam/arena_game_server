package room

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/metrics"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
	dto "github.com/prometheus/client_model/go"
)

// Subscribe publishes the subscriber and sends nothing. Everything a client
// receives is produced by the tick goroutine, in tick order.
//
// The send it used to make came from the caller's goroutine, so it could land
// behind a newer tick that the broadcast had already pushed to the same
// subscriber — the client applied an older world over a newer one. It was also
// a duplicate: the next broadcast owes an ack-0 subscriber a full snapshot
// anyway, which is what the second half of this asserts.
func TestSubscribeSendsNothingAndTheNextTickSendsFull(t *testing.T) {
	r := roomWithPlayers(t, 4)
	pending := map[sim.PlayerID]sim.Input{}
	for i := 0; i < 3; i++ {
		r.broadcast(r.world.Step(pending))
	}

	rec := &recorder{}
	r.Subscribe(1, rec.send)
	rec.mu.Lock()
	n := len(rec.msgs)
	rec.mu.Unlock()
	if n != 0 {
		t.Fatalf("Subscribe sent %d messages; it must leave every send to the tick goroutine", n)
	}

	r.broadcast(r.world.Step(pending))
	rec.mu.Lock()
	msgs := append([][]byte(nil), rec.msgs...)
	rec.mu.Unlock()
	if len(msgs) != 1 {
		t.Fatalf("the first tick after joining sent %d messages, want exactly 1", len(msgs))
	}
	e, err := protocol.UnmarshalEnv(msgs[0])
	if err != nil {
		t.Fatal(err)
	}
	s := e.GetSnapshot()
	if s == nil || s.BaselineTick != 0 {
		t.Fatalf("a subscriber that has acked nothing must get a full snapshot, got %+v", s)
	}
	if s.Tick != r.LastTick() {
		t.Fatalf("signon snapshot is tick %d, want the tick just broadcast (%d)", s.Tick, r.LastTick())
	}
}

// The full encode is built at most once per tick and handed to everyone who
// needs it. Pointer identity is the assertion because that is the whole claim:
// not "the same bytes" — which a per-client encode would also satisfy — but the
// same slice, produced once.
func TestFullSnapshotIsEncodedOnceAndShared(t *testing.T) {
	r := roomWithPlayers(t, 6)
	recs := make([]*recorder, 3)
	for i := range recs {
		recs[i] = &recorder{}
		r.Subscribe(uint32(i+1), recs[i].send)
	}
	r.broadcast(r.world.Step(map[sim.PlayerID]sim.Input{}))

	first := recs[0].last()
	if len(first) == 0 {
		t.Fatal("no snapshot was sent")
	}
	for i, rec := range recs[1:] {
		got := rec.last()
		if len(got) == 0 {
			t.Fatalf("subscriber %d got nothing", i+1)
		}
		if &got[0] != &first[0] {
			t.Fatalf("subscriber %d was handed its own encode; the full snapshot must be built once a tick", i+1)
		}
	}
}

// A tick every one of whose subscribers has a baseline owes nobody a full
// snapshot, so none is built. The observable is the metric: it is charged when
// a full snapshot is sent, and nothing is sent here.
func TestNoFullEncodeOnATickWhereEveryClientHasABaseline(t *testing.T) {
	r := roomWithPlayers(t, 4)
	rec := &recorder{}
	r.Subscribe(1, rec.send)

	pending := map[sim.PlayerID]sim.Input{}
	r.broadcast(r.world.Step(pending)) // full: nothing acked yet
	sub := r.currentSubs().byID[1]
	sub.ack.Store(r.LastTick())

	before := fullSnapshotsSent(t)
	for i := 0; i < 5; i++ {
		r.broadcast(r.world.Step(pending))
		sub.ack.Store(r.LastTick())
	}
	if got := fullSnapshotsSent(t); got != before {
		t.Fatalf("full snapshots were sent on delta-only ticks: %v -> %v", before, got)
	}
}

// The full encode happens only when somebody is owed one.
//
// Asserted as a comparison rather than against a number, because the number is
// a property of the Go version and the protobuf runtime and this is not. A
// broadcast with a subscriber that has acked nothing has to build the full
// snapshot; a broadcast with no subscribers at all has nobody to build it for.
// If the encode were unconditional the two would cost the same, which is
// exactly the regression this is here to catch.
func TestTheFullEncodeIsOnlyPaidForWhenItIsNeeded(t *testing.T) {
	pending := map[sim.PlayerID]sim.Input{}

	idle := roomWithPlayers(t, 8)
	idleAllocs := testing.AllocsPerRun(50, func() {
		idle.broadcast(idle.world.Advance(pending))
	})

	owed := roomWithPlayers(t, 8)
	owed.Subscribe(1, func([]byte) {})
	owedAllocs := testing.AllocsPerRun(50, func() {
		owed.broadcast(owed.world.Advance(pending))
	})

	if owedAllocs <= idleAllocs {
		t.Fatalf("a tick that owes a full snapshot allocated %.0f and one that owes nothing allocated %.0f; "+
			"the full encode looks unconditional again", owedAllocs, idleAllocs)
	}
}

// The room must keep its timestep with a timer that is reset rather than
// rebuilt every tick. A Reset that leaves a stale expiry queued makes the loop
// spin, and one that never re-arms makes it stall; both are invisible to a test
// that only looks at a single tick.
func TestTickRateHoldsOverManyTicks(t *testing.T) {
	const rate = 50
	r := New(Params{
		ID: "rate", Seed: 5, TickRate: rate, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}, {ID: 1000, Bot: true}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); r.Run(ctx) }()
	t.Cleanup(func() { r.Stop(); wg.Wait() })

	const window = 600 * time.Millisecond
	start := r.LastTick()
	time.Sleep(window)
	ticks := r.LastTick() - start

	want := uint32(window.Seconds() * rate) // 30
	// Generous either side: this is catching a loop that spins or stalls, not
	// scheduler jitter on a loaded test machine.
	if ticks < want/2 || ticks > want*2 {
		t.Fatalf("%d ticks in %s at %d Hz; want roughly %d", ticks, window, rate, want)
	}
}

func fullSnapshotsSent(t *testing.T) float64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.SnapshotsFull.Write(&m); err != nil {
		t.Fatalf("read arena_snapshots_full_total: %v", err)
	}
	return m.GetCounter().GetValue()
}
