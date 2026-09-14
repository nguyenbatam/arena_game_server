package room

import (
	"context"
	"testing"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

// A panic raised while the tick goroutine is serving one room must cost that
// room and nothing else. Left unrecovered it takes the process down, and with
// it every other match the node is hosting — around 1250 of them at 8 players
// and 10k CCU.
func TestRoomPanicEndsOnlyThatRoom(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ended := make(chan sim.Snapshot, 1)
	bad := New(Params{
		ID: "bad", TickRate: 60, MatchTicks: 10_000,
		Roster: RosterFromIDs([]uint32{1, 2}, 0),
		OnEnd:  func(s sim.Snapshot) { ended <- s },
	})
	// A subscriber runs on the tick goroutine, which makes it the most
	// realistic place to inject one: encoding is where a malformed world would
	// blow up in practice.
	bad.Subscribe(1, func([]byte) { panic("boom in broadcast") })

	good := New(Params{
		ID: "good", TickRate: 60, MatchTicks: 10_000,
		Roster: RosterFromIDs([]uint32{1, 2}, 0),
	})
	ticked := make(chan struct{}, 1)
	good.Subscribe(1, func([]byte) {
		select {
		case ticked <- struct{}{}:
		default:
		}
	})

	go bad.Run(ctx)
	go good.Run(ctx)
	defer good.Stop()

	select {
	case snap := <-ended:
		if !snap.Ended {
			t.Fatal("aborted room reported a snapshot that was not ended")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a panicking room never reported its end")
	}
	if !bad.Closed() {
		t.Fatal("room stayed open after its tick goroutine panicked")
	}

	select {
	case <-ticked:
	case <-time.After(2 * time.Second):
		t.Fatal("the other room stopped ticking — the blast radius was not one room")
	}
}
