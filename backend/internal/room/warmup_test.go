package room

import (
	"context"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

// A room with seats still to fill holds the match at tick zero.
//
// The clock used to start when the placement job was taken, which is before
// anybody has been told the match exists. Across nodes that is a handoff — a
// new socket, a HELLO, a JOIN_ROOM — and every second of it came out of the
// match, unevenly, so whoever reconnected first got a head start on an empty
// map.
func TestTheMatchClockWaitsForItsPlayers(t *testing.T) {
	r := runRoom(t, Params{
		ID: "warm", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster:      []sim.Player{{ID: 1}, {ID: 2}},
		Warmup:      2 * time.Second,
		ExpectSeats: 2,
	})

	time.Sleep(150 * time.Millisecond)
	if got := r.lastWorld.Load(); got != 0 {
		t.Fatalf("the match ran to tick %d while a seat was still missing", got)
	}

	rec1, rec2 := &recorder{}, &recorder{}
	r.Subscribe(1, rec1.send)
	r.Subscribe(2, rec2.send)

	deadline := time.Now().Add(2 * time.Second)
	for r.lastWorld.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if r.lastWorld.Load() == 0 {
		t.Fatal("the match never started once every seat had joined")
	}
}

// A seat whose player closed the tab must not hold the rest of the room
// forever. The warmup is a budget, not a condition.
func TestWarmupGivesUpAndStartsShortHanded(t *testing.T) {
	start := time.Now()
	r := runRoom(t, Params{
		ID: "warm-timeout", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster:      []sim.Player{{ID: 1}, {ID: 2}},
		Warmup:      120 * time.Millisecond,
		ExpectSeats: 2,
	})
	// Only one of the two ever turns up.
	rec := &recorder{}
	r.Subscribe(1, rec.send)

	deadline := time.Now().Add(2 * time.Second)
	for r.lastWorld.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if r.lastWorld.Load() == 0 {
		t.Fatal("the match never started after the warmup budget ran out")
	}
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Fatalf("started after %s, before the %s warmup was up", waited, 120*time.Millisecond)
	}
}

// Zero disables it, which is what every test and every single-process
// deployment that does not set WARMUP_TIMEOUT gets.
func TestNoWarmupStartsImmediately(t *testing.T) {
	r := runRoom(t, Params{
		ID: "no-warm", Seed: 1, TickRate: 200, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}, {ID: 2}},
	})
	deadline := time.Now().Add(time.Second)
	for r.lastWorld.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if r.lastWorld.Load() == 0 {
		t.Fatal("a room with no warmup did not start ticking")
	}
}

// A match with nothing but bots has no connection to wait for, so it must not
// spend the warmup waiting for players who do not exist.
func TestAMatchWithNoSeatedPlayersDoesNotWait(t *testing.T) {
	start := time.Now()
	r := runRoom(t, Params{
		ID: "bots", Seed: 1, TickRate: 200, MatchTicks: 1 << 20,
		Roster:      []sim.Player{{ID: 1000, Bot: true}, {ID: 1001, Bot: true}},
		Warmup:      5 * time.Second,
		ExpectSeats: 0,
	})
	deadline := time.Now().Add(time.Second)
	for r.lastWorld.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if r.lastWorld.Load() == 0 {
		t.Fatalf("a bot-only match waited %s for nobody", time.Since(start))
	}
}

// Stopping a room that is still warming up has to release it, or a shutdown
// waits out the warmup of every match that had not started.
func TestStoppingARoomDuringWarmupReleasesIt(t *testing.T) {
	r := New(Params{
		ID: "warm-stop", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster:      []sim.Player{{ID: 1}, {ID: 2}},
		Warmup:      10 * time.Second,
		ExpectSeats: 2,
	})
	done := make(chan struct{})
	go func() {
		r.Run(context.Background())
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	r.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a room stopped during warmup did not return")
	}
	if got := r.lastWorld.Load(); got != 0 {
		t.Fatalf("a room stopped during warmup still simulated %d ticks", got)
	}
}

// And so does cancelling its context, which is the other way a drain ends.
func TestCancellingDuringWarmupReleasesTheRoom(t *testing.T) {
	r := New(Params{
		ID: "warm-cancel", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster:      []sim.Player{{ID: 1}, {ID: 2}},
		Warmup:      10 * time.Second,
		ExpectSeats: 2,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a room cancelled during warmup did not return")
	}
}

// Input that arrives before the match starts is drained rather than left to
// fill the inbound channel.
//
// A client that joins early and starts sending at the tick rate is ordinary. A
// warmup that ignored the channel would let it fill, and SubmitInput would then
// start evicting other players' messages the moment the match began — a burst
// of dropped input on the first tick of every match.
//
// What it does not promise is that every one of those inputs is simulated: the
// per-player buffer is trimmed to its target depth on the first tick, which is
// the same rule that applies to a client running ahead mid-match.
func TestInputDuringWarmupIsDrainedNotBanked(t *testing.T) {
	r := runRoom(t, Params{
		ID: "warm-input", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster:      []sim.Player{{ID: 1}, {ID: 2}},
		Warmup:      300 * time.Millisecond,
		ExpectSeats: 2,
	})
	for i := 0; i < 200; i++ {
		r.SubmitInput(&pb.Input{PlayerId: 1, Mx: 1, Seq: uint32(i + 1)})
	}

	// The second seat never joins, so the warmup ends on its budget.
	deadline := time.Now().Add(2 * time.Second)
	for r.lastWorld.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if r.lastWorld.Load() == 0 {
		t.Fatal("the match never started")
	}
	if backlog := len(r.inputs); backlog != 0 {
		t.Fatalf("%d messages were still sitting in the inbound channel when the match began", backlog)
	}
}

// A seat that never turns up is taken out of the match, and a room where
// nobody turns up ends instead of playing itself out.
//
// This is the failure the warmup exists to notice, and without it the room is
// stuck in the worst of both states: the match clock ran, so it is counted
// against the node's ceiling for its full length, and sim.abandoned() cannot
// end it because a player who never arrived has not *left*.
func TestSeatsThatNeverArriveAreTakenOutOfTheMatch(t *testing.T) {
	ended := make(chan sim.Snapshot, 1)
	r := runRoom(t, Params{
		ID: "ghosts", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster:      []sim.Player{{ID: 1}, {ID: 2}},
		Seats:       map[string]uint32{"a": 1, "b": 2},
		Warmup:      100 * time.Millisecond,
		ExpectSeats: 2,
		OnEnd:       func(s sim.Snapshot) { ended <- s },
	})
	_ = r

	select {
	case snap := <-ended:
		if !snap.Ended {
			t.Fatal("the room reported an end that was not an end")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a match nobody joined ran on regardless")
	}
}

// One seat arriving keeps the match alive — the no-show departure must not take
// the room down around the player who did turn up.
func TestOneArrivalKeepsAMatchAliveThroughTheWarmupTimeout(t *testing.T) {
	ended := make(chan sim.Snapshot, 1)
	r := runRoom(t, Params{
		ID: "one-shows", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster:      []sim.Player{{ID: 1}, {ID: 2}},
		Seats:       map[string]uint32{"a": 1, "b": 2},
		Warmup:      100 * time.Millisecond,
		ExpectSeats: 2,
		OnEnd:       func(s sim.Snapshot) { ended <- s },
	})
	rec := &recorder{}
	r.Subscribe(1, rec.send)

	select {
	case <-ended:
		t.Fatal("the match ended with a player still in it")
	case <-time.After(1500 * time.Millisecond):
	}
}

// A room with no warmup configured must never reach the no-show path: at tick
// zero every seat is empty, so running it there would end every match before it
// started.
func TestNoWarmupNeverDepartsAnybody(t *testing.T) {
	ended := make(chan sim.Snapshot, 1)
	runRoom(t, Params{
		ID: "no-warm-noshow", Seed: 1, TickRate: 200, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}, {ID: 2}},
		Seats:  map[string]uint32{"a": 1, "b": 2},
		OnEnd:  func(s sim.Snapshot) { ended <- s },
	})
	select {
	case <-ended:
		t.Fatal("a room with no warmup ended itself as abandoned")
	case <-time.After(time.Second):
	}
}
