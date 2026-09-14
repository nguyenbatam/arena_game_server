package sim

import "testing"

// Advance hands back the world's own slices and Step hands back copies. The
// room's tick loop depends on the first — one allocation and a memcpy per tick
// per room is worth removing for a value that is encoded and dropped before the
// tick is over — and every other caller depends on the second.
//
// This is the test the comment on Advance points at. Without it the distinction
// lives only in prose, and the failure it prevents is silent: a caller that
// keeps an Advance result sees it mutate underneath them a tick later, which
// reads as the simulation being wrong rather than as the snapshot being stale.
func TestAdvanceAliasesTheWorldAndStepDoesNot(t *testing.T) {
	roster := []Player{{ID: 1}, {ID: 2}}

	w := NewWorld(1, 20, 1<<20, roster)
	owned := w.Step(map[PlayerID]Input{1: {MX: 1, Seq: 1}})
	ownedX := owned.Players[0].X
	for i := 0; i < 5; i++ {
		w.Step(map[PlayerID]Input{1: {MX: 1, Seq: uint32(i + 2)}})
	}
	if owned.Players[0].X != ownedX {
		t.Fatalf("Step's result changed under the caller: %d -> %d", ownedX, owned.Players[0].X)
	}

	w2 := NewWorld(1, 20, 1<<20, roster)
	view := w2.Advance(map[PlayerID]Input{1: {MX: 1, Seq: 1}})
	if &view.Players[0] != &w2.Players()[0] {
		t.Fatal("Advance copied the roster; it is meant to alias the world's own slice")
	}
	viewX := view.Players[0].X
	w2.Advance(map[PlayerID]Input{1: {MX: 1, Seq: 2}})
	if view.Players[0].X == viewX {
		t.Fatal("Advance's result did not move with the world, so it is not the alias it claims to be")
	}
}

// Advance and Step must describe the same tick. They are two views of one
// state, and a divergence here would mean the room broadcasts something other
// than what the match is recorded as ending on.
func TestAdvanceAndStepAgreeOnTheSameTick(t *testing.T) {
	roster := []Player{{ID: 1}, {ID: 2}, {ID: 1000, Bot: true}}
	in := map[PlayerID]Input{1: {MX: 1, MY: 1, Fire: true, Aim: 30, Seq: 1}}

	a := NewWorld(9, 20, 1<<20, roster)
	b := NewWorld(9, 20, 1<<20, roster)
	for i := 0; i < 40; i++ {
		got := a.Advance(in)
		want := b.Step(in)
		if got.Tick != want.Tick || got.Ended != want.Ended || got.Winner != want.Winner {
			t.Fatalf("tick %d: header %+v vs %+v", i, got, want)
		}
		if len(got.Players) != len(want.Players) || len(got.Projectiles) != len(want.Projectiles) {
			t.Fatalf("tick %d: %d/%d players, %d/%d projectiles",
				i, len(got.Players), len(want.Players), len(got.Projectiles), len(want.Projectiles))
		}
		for j := range want.Players {
			if got.Players[j] != want.Players[j] {
				t.Fatalf("tick %d player %d: %+v vs %+v", i, j, got.Players[j], want.Players[j])
			}
		}
		for j := range want.Projectiles {
			if got.Projectiles[j] != want.Projectiles[j] {
				t.Fatalf("tick %d projectile %d: %+v vs %+v", i, j, got.Projectiles[j], want.Projectiles[j])
			}
		}
		if a.Checksum() != b.Checksum() {
			t.Fatalf("tick %d: checksums diverged", i)
		}
	}
}

// Snapshot reports the current state without advancing. The room uses it to
// hand OnEnd something that outlives the tick goroutine.
func TestSnapshotDoesNotAdvanceAndIsOwned(t *testing.T) {
	w := NewWorld(3, 20, 1<<20, []Player{{ID: 1}})
	w.Step(nil)
	before := w.Tick
	s := w.Snapshot()
	if w.Tick != before || s.Tick != before {
		t.Fatalf("Snapshot advanced the world: tick %d, snapshot %d, want %d", w.Tick, s.Tick, before)
	}
	if len(s.Players) > 0 && &s.Players[0] == &w.Players()[0] {
		t.Fatal("Snapshot aliased the world; OnEnd reads it from another goroutine")
	}
}
