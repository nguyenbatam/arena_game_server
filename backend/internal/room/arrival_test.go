package room

import (
	"reflect"
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

// The queue holds values, so a submitted input is settled at the moment it is
// submitted. Whatever the caller does to the wire message afterwards — reuse
// it, pool it, let it be collected — cannot reach the simulation.
func TestSubmittedInputDoesNotAliasTheWireMessage(t *testing.T) {
	r := New(Params{ID: "a", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}}})

	wire := &pb.Input{PlayerId: 1, Mx: 1, My: -1, Aim: 30, Seq: 7, Fire: true, AckTick: 0}
	r.SubmitInput(wire)

	// The gateway is free to do anything with the message once SubmitInput has
	// returned. Scribbling on it stands in for reuse or pooling.
	wire.Mx, wire.My, wire.Aim, wire.Seq, wire.Fire = -1, 1, 300, 999, false

	r.drainNonblock()
	pending := map[sim.PlayerID]sim.Input{}
	r.fillPending(pending)

	want := sim.Input{MX: 1, MY: -1, Aim: 30, Seq: 7, Fire: true}
	if got := pending[1]; got != want {
		t.Errorf("simulated %+v, want the values as submitted %+v", got, want)
	}
}

// The channel's element type must stay pointer-free, which is the whole reason
// the flattening exists: a 1024-deep queue per room is a million slots across
// the fleet, and the collector only has to walk them if there is something in
// them to walk.
func TestArrivalHoldsNoPointers(t *testing.T) {
	var a arrival
	tp := reflect.TypeOf(a)
	var check func(reflect.Type, string)
	check = func(t2 reflect.Type, path string) {
		switch t2.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.String,
			reflect.Chan, reflect.Func, reflect.Interface, reflect.UnsafePointer:
			t.Errorf("%s is a %s: the input queue must stay scan-free", path, t2.Kind())
		case reflect.Struct:
			for i := 0; i < t2.NumField(); i++ {
				f := t2.Field(i)
				check(f.Type, path+"."+f.Name)
			}
		case reflect.Array:
			check(t2.Elem(), path+"[]")
		}
	}
	check(tp, "arrival")
}

// arrivalFrom is the one place the wire shape is read, so the mapping is
// pinned here rather than inferred from a behavioural test.
func TestArrivalFromMapsEveryField(t *testing.T) {
	got := arrivalFrom(&pb.Input{
		PlayerId: 9, Mx: -1, My: 1, Aim: 271, Seq: 4242, Fire: true, AckTick: 88,
		InterpMs: 100,
	})
	want := arrival{
		playerID: 9, ackTick: 88, interpMs: 100,
		in: sim.Input{MX: -1, MY: 1, Aim: 271, Seq: 4242, Fire: true},
	}
	if got != want {
		t.Errorf("arrivalFrom = %+v, want %+v", got, want)
	}
	// LagTicks is derived server-side when the input is simulated, never taken
	// from the client — see lagFor. InterpMs is the one figure that does come
	// from the client, and it is an input to that derivation, not the answer:
	// it arrives clamped and in milliseconds, and never as a tick count.
	if got.in.LagTicks != 0 {
		t.Errorf("LagTicks came off the wire as %d", got.in.LagTicks)
	}
}
