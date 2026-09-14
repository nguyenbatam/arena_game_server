package sim

import "testing"

func kinds(evs []Event, k EventKind) []Event {
	var out []Event
	for _, e := range evs {
		if e.Kind == k {
			out = append(out, e)
		}
	}
	return out
}

// A hit produces an event naming who did it. Nothing in PlayerSnap can: two
// hits inside one delta window are a single HP change, so a client watching
// state sees that HP fell and never that it fell twice or to whom.
func TestAHitIsReported(t *testing.T) {
	w := duel(t, 20, 218_000+200_000, 1_000_000)
	var got []Event
	for i := 0; i < 20 && len(got) == 0; i++ {
		var in map[PlayerID]Input
		if i == 0 {
			in = map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1}}
		}
		got = kinds(w.Step(in).Events, EventHit)
	}
	if len(got) != 1 {
		t.Fatalf("got %d hit events, want 1", len(got))
	}
	e := got[0]
	if e.Actor != 1 || e.Target != 2 {
		t.Fatalf("hit reported actor=%d target=%d, want 1 and 2", e.Actor, e.Target)
	}
	if e.HP != MaxHP-Damage {
		t.Fatalf("hit reported hp=%d, want %d", e.HP, MaxHP-Damage)
	}
	if e.Tick == 0 {
		t.Fatal("event carries no tick; a delta spans several and the client cannot infer it")
	}
}

// A kill carries a hit with it. Sending only the kill would make the shot that
// killed somebody the one shot that produced no hitmarker.
func TestAKillAlsoReportsTheHitThatCausedIt(t *testing.T) {
	w := duel(t, 20, 218_000+200_000, 1_000_000)
	w.player(2).HP = Damage // one hit from death

	var evs []Event
	for i := 0; i < 20 && len(kinds(evs, EventKill)) == 0; i++ {
		var in map[PlayerID]Input
		if i == 0 {
			in = map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1}}
		}
		evs = w.Step(in).Events
	}
	hits, kills := kinds(evs, EventHit), kinds(evs, EventKill)
	if len(kills) != 1 {
		t.Fatalf("got %d kill events, want 1", len(kills))
	}
	if len(hits) != 1 {
		t.Fatalf("got %d hit events alongside the kill, want 1", len(hits))
	}
	// Order matters to a client: the hitmarker belongs to the shot, the feed
	// line to its outcome.
	if evs[0].Kind != EventHit || evs[1].Kind != EventKill {
		t.Fatalf("events came out as %v then %v, want hit then kill", evs[0].Kind, evs[1].Kind)
	}
	if kills[0].Actor != 1 || kills[0].Target != 2 {
		t.Fatalf("kill reported actor=%d target=%d", kills[0].Actor, kills[0].Target)
	}
}

// A shot that hits a spawn-protected player produces no event at all, for the
// same reason it produces no damage and no score: it did not land.
func TestAProtectedPlayerProducesNoHitEvent(t *testing.T) {
	w := duel(t, 20, 218_000+200_000, 1_000_000)
	w.player(2).Protect = SpawnProtectTicks
	for i := 0; i < 20; i++ {
		var in map[PlayerID]Input
		if i == 0 {
			in = map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1}}
		}
		if evs := w.Step(in).Events; len(evs) != 0 {
			t.Fatalf("a shot at a protected player produced %v", evs)
		}
	}
}

// A despawn is reported, or the client is left with a player frozen at zero HP
// and no explanation for it.
func TestADespawnIsReported(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	w.Depart(2)
	var got []Event
	for i := 0; i < int(DepartLingerTicks)+3 && len(got) == 0; i++ {
		got = kinds(w.Step(nil).Events, EventDepart)
	}
	if len(got) != 1 {
		t.Fatalf("got %d depart events, want 1", len(got))
	}
	if got[0].Target != 2 || got[0].Actor != 0 {
		t.Fatalf("depart reported actor=%d target=%d, want 0 and 2", got[0].Actor, got[0].Target)
	}
}

// The buffer is cleared every tick. It is truncated rather than reallocated, so
// a tick that emits nothing must still report nothing.
func TestEventsDoNotCarryOverBetweenTicks(t *testing.T) {
	w := duel(t, 20, 218_000+200_000, 1_000_000)
	for i := 0; i < 20; i++ {
		var in map[PlayerID]Input
		if i == 0 {
			in = map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1}}
		}
		snap := w.Step(in)
		if len(snap.Events) > 0 {
			// The tick after a busy one must be quiet again.
			if next := w.Step(nil); len(next.Events) != 0 {
				t.Fatalf("events survived into the next tick: %v", next.Events)
			}
			return
		}
	}
	t.Fatal("the shot never landed; the test proved nothing")
}

// Every event carries the tick it happened on, and that is the tick of the
// snapshot reporting it.
func TestEventTicksMatchTheSnapshotThatCarriesThem(t *testing.T) {
	w := NewWorld(11, 20, 1<<20, []Player{{ID: 1}, {ID: 2}, {ID: 1000, Bot: true}})
	for tick := 0; tick < 300; tick++ {
		snap := w.Step(map[PlayerID]Input{
			1: {MX: 1, Fire: true, Aim: int16(tick * 13), Seq: uint32(tick + 1)},
			2: {MY: 1, Fire: true, Aim: int16(tick * 29), Seq: uint32(tick + 1)},
		})
		for _, e := range snap.Events {
			if e.Tick != snap.Tick {
				t.Fatalf("event %+v carried on the snapshot for tick %d", e, snap.Tick)
			}
		}
	}
}

// Events are an output of a tick, not part of the world: replaying the same
// inputs reproduces them, which is what keeps them out of the checksum.
func TestEventsAreDeterministic(t *testing.T) {
	run := func() []Event {
		w := NewWorld(4242, 20, 1<<20, []Player{{ID: 1}, {ID: 2}, {ID: 1000, Bot: true}})
		var all []Event
		for tick := 0; tick < 400; tick++ {
			snap := w.Step(map[PlayerID]Input{
				1: {MX: 1, Fire: tick%3 == 0, Aim: int16(tick * 7), Seq: uint32(tick + 1)},
				2: {MY: -1, Fire: tick%4 == 0, Aim: int16(tick * 17), Seq: uint32(tick + 1)},
			})
			all = append(all, snap.Events...)
		}
		return all
	}
	a, b := run(), run()
	if len(a) == 0 {
		t.Fatal("the run produced no events; the test proves nothing")
	}
	if len(a) != len(b) {
		t.Fatalf("event counts diverged: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("event %d diverged: %+v vs %+v", i, a[i], b[i])
		}
	}
}

// Step hands out an owned copy; Advance aliases. The events half must follow
// the same contract as the players and projectiles halves, or a caller that
// keeps a Step result finds its killfeed rewritten by the next tick.
func TestStepOwnsItsEventsAndAdvanceDoesNot(t *testing.T) {
	fire := map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1}}

	owned := duel(t, 20, 218_000+36_000, 1_000_000)
	var kept Snapshot
	for i := 0; i < 20 && len(kept.Events) == 0; i++ {
		var in map[PlayerID]Input
		if i == 0 {
			in = fire
		}
		kept = owned.Step(in)
	}
	if len(kept.Events) == 0 {
		t.Fatal("setup: no events were produced")
	}
	before := append([]Event(nil), kept.Events...)
	for i := 0; i < 5; i++ {
		owned.Step(nil)
	}
	for i := range before {
		if kept.Events[i] != before[i] {
			t.Fatal("a Step result's events were overwritten by a later tick")
		}
	}

	// And Advance really does alias, which is the property that makes it worth
	// having: the room encodes the events inside the tick and drops them.
	//
	// The assertion is on the contents, not on the length. Truncating the
	// world's buffer moves the world's own slice header and leaves a handed-out
	// one pointing at the same backing array with its old length — so a stale
	// view keeps reporting the right *number* of events while the next tick
	// writes different ones over them, which is the quieter half of the bug and
	// the reason this is worth a test rather than a comment.
	aliased := duel(t, 20, 218_000+36_000, 1_000_000)
	var view Snapshot
	for i := 0; i < 20 && len(view.Events) == 0; i++ {
		var in map[PlayerID]Input
		if i == 0 {
			in = fire
		}
		view = aliased.Advance(in)
	}
	if len(view.Events) == 0 {
		t.Fatal("setup: Advance produced no events")
	}
	first := view.Events[0]

	// Heal and shoot again, so the next tick that lands a hit writes a
	// different HP into the same slot.
	aliased.player(2).HP = MaxHP
	aliased.player(1).Cooldown = 0
	for i := 0; i < 20; i++ {
		var in map[PlayerID]Input
		if i == 0 {
			in = map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 2}}
		}
		if len(aliased.Advance(in).Events) > 0 {
			break
		}
	}
	if view.Events[0] == first {
		t.Fatal("Advance's events did not move with the world, so they are not the alias it claims to be")
	}
}

// A departure is one event, not one per tick for the rest of the match.
//
// It was the second: timers() reached the despawn branch on every tick once the
// linger had run out, so a single disconnect put a DEPART on the wire twenty
// times a second — a killfeed nobody can read, and a flood that pushed real
// hits out past the room's event cap. The unit test that was supposed to cover
// this stopped at the first event it saw, which is why it took an end-to-end
// test over a real socket to notice.
func TestADepartureIsReportedExactlyOnce(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	w.Depart(2)

	departs := 0
	for i := 0; i < int(DepartLingerTicks)*5; i++ {
		departs += len(kinds(w.Step(nil).Events, EventDepart))
	}
	if departs != 1 {
		t.Fatalf("one disconnect produced %d depart events over %d ticks, want 1",
			departs, int(DepartLingerTicks)*5)
	}
}

// Leaving and coming back and leaving again is two departures — the counter has
// to be re-armed, not merely never fired twice.
func TestAReDepartureIsReportedAgain(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	run := func(ticks int) int {
		n := 0
		for i := 0; i < ticks; i++ {
			n += len(kinds(w.Step(nil).Events, EventDepart))
		}
		return n
	}

	w.Depart(2)
	if got := run(int(DepartLingerTicks) + 2); got != 1 {
		t.Fatalf("first departure reported %d times, want 1", got)
	}
	w.Rejoin(2)
	run(int(RespawnTicks) + 2)
	w.Depart(2)
	if got := run(int(DepartLingerTicks) + 2); got != 1 {
		t.Fatalf("second departure reported %d times, want 1", got)
	}
}
