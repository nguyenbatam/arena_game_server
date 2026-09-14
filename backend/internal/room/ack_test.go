package room

import (
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

func ackRoom(t *testing.T, players int) *Room {
	t.Helper()
	roster := make([]sim.Player, 0, players)
	for i := 1; i <= players; i++ {
		roster = append(roster, sim.Player{ID: sim.PlayerID(i)})
	}
	return New(Params{ID: "ack", Seed: 1, TickRate: 20, MatchTicks: 1 << 20, Roster: roster})
}

// A client may only claim a tick this room has actually broadcast. Claiming a
// later one used to pin it to full snapshots for the rest of the match.
func TestAckAheadOfTheRoomIsIgnored(t *testing.T) {
	r := ackRoom(t, 4)

	var sizes []int
	r.Subscribe(1, func(msg []byte) { sizes = append(sizes, len(msg)) })

	tick := func() {
		r.broadcast(r.world.Advance(map[sim.PlayerID]sim.Input{}))
	}

	// Five honest ticks, acking what the server sent: the steady state is a
	// delta against the previous tick.
	for i := 0; i < 5; i++ {
		tick()
		r.recordAck(1, r.LastTick())
	}
	honest := sizes[len(sizes)-1]

	// Now claim a tick from the far future, the way SubmitInput takes it off
	// the wire, and keep acking honestly afterwards.
	r.SubmitInput(&pb.Input{PlayerId: 1, AckTick: 4_000_000_000})
	if got := r.currentSubs().byID[1].ack.Load(); got > r.LastTick() {
		t.Fatalf("ack stored as %d with the room at tick %d", got, r.LastTick())
	}
	for i := 0; i < 5; i++ {
		tick()
		r.recordAck(1, r.LastTick())
	}

	if after := sizes[len(sizes)-1]; after != honest {
		t.Errorf("bogus ack changed the payload: %dB honest, %dB after", honest, after)
	}
}

// The clamp must not cost an honest client its baseline: acking the tick just
// received is the steady state, and it has to keep working.
func TestAckOfTheCurrentTickStillAdvancesTheBaseline(t *testing.T) {
	r := ackRoom(t, 4)
	r.Subscribe(1, func([]byte) {})

	r.broadcast(r.world.Advance(map[sim.PlayerID]sim.Input{}))
	cur := r.LastTick()
	r.SubmitInput(&pb.Input{PlayerId: 1, AckTick: cur})

	got := r.currentSubs().byID[1].ack.Load()
	if got != cur {
		t.Fatalf("ack of the current tick %d was not recorded, got %d", cur, got)
	}
	if r.baselineFor(got, cur+1) == nil {
		t.Errorf("tick %d did not become a usable baseline", cur)
	}
}
