package room

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

func subsRoom(players int) *Room {
	roster := make([]sim.Player, 0, players)
	for i := 1; i <= players; i++ {
		roster = append(roster, sim.Player{ID: sim.PlayerID(i)})
	}
	return New(Params{ID: "subs", Seed: 1, TickRate: 20, MatchTicks: 1 << 20, Roster: roster})
}

// The published list is what both hot paths read, so a join has to be visible
// to the lookup and to the walk at the same moment — never to one of them.
func TestSubscribePublishesToBothViews(t *testing.T) {
	r := subsRoom(2)
	r.Subscribe(1, func([]byte) {})
	r.Subscribe(2, func([]byte) {})

	cur := r.currentSubs()
	if len(cur.byID) != 2 || len(cur.list) != 2 {
		t.Fatalf("byID has %d and list has %d, want 2 of each", len(cur.byID), len(cur.list))
	}
	for id, s := range cur.byID {
		found := false
		for _, l := range cur.list {
			found = found || l == s
		}
		if !found {
			t.Errorf("seat %d is in the lookup but not in the walk", id)
		}
	}
}

// Unsubscribe removes a seat from both views, and re-subscribing the same seat
// replaces it rather than doubling it up.
func TestUnsubscribeAndResubscribe(t *testing.T) {
	r := subsRoom(2)
	r.Subscribe(1, func([]byte) {})
	r.Subscribe(2, func([]byte) {})

	r.Unsubscribe(1)
	if cur := r.currentSubs(); len(cur.byID) != 1 || len(cur.list) != 1 {
		t.Fatalf("after Unsubscribe: byID %d, list %d, want 1 and 1", len(cur.byID), len(cur.list))
	}
	if r.currentSubs().byID[1] != nil {
		t.Error("seat 1 is still in the lookup")
	}

	r.Subscribe(1, func([]byte) {})
	r.Subscribe(1, func([]byte) {})
	cur := r.currentSubs()
	if len(cur.byID) != 2 || len(cur.list) != 2 {
		t.Fatalf("re-subscribing doubled a seat: byID %d, list %d", len(cur.byID), len(cur.list))
	}

	// Unsubscribing a seat that was never there must not rebuild the list.
	before := r.currentSubs()
	r.Unsubscribe(99)
	if r.currentSubs() != before {
		t.Error("unsubscribing an absent seat replaced the published list")
	}
}

// A seat that has left stops receiving, which is the property Unsubscribe
// exists for: the room must not keep writing into a connection that is gone.
func TestUnsubscribedSeatStopsReceiving(t *testing.T) {
	r := subsRoom(2)
	var got1, got2 atomic.Int64
	r.Subscribe(1, func([]byte) { got1.Add(1) })
	r.Subscribe(2, func([]byte) { got2.Add(1) })

	r.broadcast(r.world.Advance(map[sim.PlayerID]sim.Input{}))
	r.Unsubscribe(1)
	r.broadcast(r.world.Advance(map[sim.PlayerID]sim.Input{}))

	if got := got1.Load(); got != 1 {
		t.Errorf("unsubscribed seat received %d snapshots, want the 1 from before it left", got)
	}
	if got := got2.Load(); got != 2 {
		t.Errorf("remaining seat received %d snapshots, want 2", got)
	}
}

// Joins, leaves, acks and broadcasts all at once. The published list is
// replaced rather than mutated, so a broadcast holding one must stay valid for
// as long as it walks it — which is what -race is here to check.
func TestSubsAreSafeUnderConcurrentChurn(t *testing.T) {
	r := subsRoom(8)
	for i := 1; i <= 8; i++ {
		r.Subscribe(uint32(i), func([]byte) {})
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			r.broadcast(r.world.Advance(map[sim.PlayerID]sim.Input{}))
		}
		close(stop)
	}()

	for seat := 1; seat <= 8; seat++ {
		wg.Add(1)
		go func(seat uint32) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				r.Subscribe(seat, func([]byte) {})
				r.recordAck(seat, r.LastTick())
				r.Unsubscribe(seat)
			}
		}(uint32(seat))
	}
	wg.Wait()
}
