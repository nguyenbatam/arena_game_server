package room

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func startRoom(t *testing.T, ticks uint32) (*Room, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	r := New(Params{
		ID: "t", Seed: 1, TickRate: 50, MatchTicks: ticks,
		Roster: []sim.Player{{ID: 1}, {ID: 2}},
	})
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
	return r, cancel, done
}

func TestConcurrentInputsDoNotRace(t *testing.T) {
	r, _, _ := startRoom(t, 80)
	var snaps atomic.Int64
	r.Subscribe(1, func([]byte) { snaps.Add(1) })
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for k := 0; k < 200; k++ {
				r.SubmitInput(&pb.Input{PlayerId: uint32(i%2 + 1), Mx: 1, Fire: k%3 == 0, Aim: int32(k)})
			}
		}(i)
	}
	wg.Wait()
	deadline := time.Now().Add(time.Second)
	for snaps.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if snaps.Load() == 0 {
		t.Fatal("no snapshots")
	}
}

func TestReconnectCatchUpSnapshot(t *testing.T) {
	r, _, _ := startRoom(t, 400)
	first := make(chan uint32, 8)
	r.Subscribe(1, func(msg []byte) {
		e, err := protocol.UnmarshalEnv(msg)
		if err != nil || e.GetSnapshot() == nil {
			return
		}
		select {
		case first <- e.GetSnapshot().Tick:
		default:
		}
	})
	var t0 uint32
	select {
	case t0 = <-first:
	case <-time.After(time.Second):
		t.Fatal("no initial snapshot")
	}
	r.Unsubscribe(1)
	time.Sleep(80 * time.Millisecond)
	resume := make(chan uint32, 4)
	r.Subscribe(1, func(msg []byte) {
		e, err := protocol.UnmarshalEnv(msg)
		if err != nil || e.GetSnapshot() == nil {
			return
		}
		select {
		case resume <- e.GetSnapshot().Tick:
		default:
		}
	})
	select {
	case t1 := <-resume:
		if t1 < t0 {
			t.Fatalf("catch-up tick %d < disconnect tick %d", t1, t0)
		}
	case <-time.After(time.Second):
		t.Fatal("no catch-up snapshot on reconnect")
	}
}

func TestSlowConsumerMonotonicTicks(t *testing.T) {
	r, _, _ := startRoom(t, 200)
	var last atomic.Uint32
	var regress atomic.Int64
	r.Subscribe(1, func(msg []byte) {
		time.Sleep(5 * time.Millisecond)
		e, err := protocol.UnmarshalEnv(msg)
		if err != nil || e.GetSnapshot() == nil {
			return
		}
		tick := e.GetSnapshot().Tick
		prev := last.Load()
		if prev != 0 && tick < prev {
			regress.Add(1)
		}
		last.Store(tick)
	})
	time.Sleep(200 * time.Millisecond)
	if last.Load() == 0 {
		t.Fatal("no ticks")
	}
	if regress.Load() != 0 {
		t.Fatalf("tick went backwards %d times", regress.Load())
	}
}

func TestBindSeatRejectsUnknownConn(t *testing.T) {
	r := New(Params{
		ID: "t", Seed: 1, TickRate: 20, MatchTicks: 10,
		Roster: []sim.Player{{ID: 1}, {ID: 2}},
		Seats:  map[string]uint32{"p1": 1},
	})
	if id, ok := r.BindSeat("p1", 99); !ok || id != 1 {
		t.Fatalf("must bind roster seat, got %d %v", id, ok)
	}
	if _, ok := r.BindSeat("intruder", 1); ok {
		t.Fatal("unknown conn must not steal seat")
	}
}

func TestLateInputStillApplied(t *testing.T) {
	r, _, _ := startRoom(t, 300)
	got := make(chan *pb.Snapshot, 1)
	r.Subscribe(1, func(msg []byte) {
		e, err := protocol.UnmarshalEnv(msg)
		if err != nil || e.GetSnapshot() == nil {
			return
		}
		select {
		case got <- e.GetSnapshot():
		default:
		}
	})
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("no snap")
	}
	time.Sleep(30 * time.Millisecond)
	r.SubmitInput(&pb.Input{PlayerId: 1, Mx: 1, Fire: true, Aim: 0})
	var startX int32
	seen := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s := <-got
		for _, p := range s.Players {
			if p.Id != 1 {
				continue
			}
			if !seen {
				startX = p.X
				seen = true
				continue
			}
			if p.X != startX || len(s.Projectiles) > 0 {
				return
			}
		}
	}
	t.Fatal("late input had no effect")
}

// setTestClock puts both of a room's clocks at tick t.
//
// A room has two: lastWorld is where the simulation has got to, and lastTick is
// the newest tick that has actually gone out. They differ only when the room
// sends less often than it ticks, and a test that drives fillPending or
// recordAck by hand wants them together — which of the two a given assertion is
// really about is stated by the test, not by which field it happened to poke.
func setTestClock(r *Room, t uint32) {
	r.lastWorld.Store(t)
	r.lastTick.Store(t)
}
