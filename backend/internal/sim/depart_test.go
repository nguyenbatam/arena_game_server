package sim

import "testing"

// shootUntilDown fires at seat 2 from point-blank until they stop being alive
// or the budget runs out, and reports the shooter's score.
func shootUntilDown(w *World, ticks int) uint16 {
	for i := 0; i < ticks; i++ {
		tg := w.player(2)
		sh := w.player(1)
		sh.X, sh.Y, sh.Aim = tg.X-60_000, tg.Y, 0
		w.Step(map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: uint32(i + 1)}})
	}
	return w.player(1).Score
}

// A disconnected player is still shootable for a moment. Despawning them the
// instant the socket drops would make pulling the cable the cheapest dodge in
// the game — the shot already in the air would find nothing.
func TestADepartedAvatarLingersBeforeItGoes(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	w.Depart(2)

	if !w.player(2).Alive {
		t.Fatal("the avatar vanished on the tick the player left")
	}
	tg := w.player(2)
	sh := w.player(1)
	sh.X, sh.Y, sh.Aim = tg.X-60_000, tg.Y, 0
	before := tg.HP
	w.Step(map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1}})
	w.Step(nil)
	w.Step(nil)
	if w.player(2).HP == before {
		t.Fatal("a shot fired during the linger did not land")
	}
}

// Once the linger is spent the avatar is out of the world: not a target, not
// respawned, and worth nothing to anybody.
func TestADepartedAvatarDespawnsAndStopsPayingScore(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	w.Depart(2)
	for i := 0; i < int(DepartLingerTicks)+2; i++ {
		w.Step(nil)
	}

	p := w.player(2)
	if p.Alive {
		t.Fatal("the avatar is still in play after the linger expired")
	}
	if p.HP != 0 {
		t.Fatalf("despawned avatar still has %d HP", p.HP)
	}

	// The match runs on for a long while with somebody emptying a magazine
	// into where they stood.
	if score := shootUntilDown(w, 400); score != 0 {
		t.Fatalf("farmed %d kills off a player who had left", score)
	}
	if w.player(2).Alive {
		t.Fatal("a departed player came back on their own")
	}
}

// The whole point of the despawn: this is the number that reaches the ladder.
// Left in the world, a motionless avatar pays a kill every RespawnTicks for the
// rest of the match.
func TestLeavingDoesNotTurnAPlayerIntoAScoreFarm(t *testing.T) {
	const ticks = 600

	farm := func(depart bool) uint16 {
		w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
		if depart {
			w.Depart(2)
			for i := 0; i < int(DepartLingerTicks)+2; i++ {
				w.Step(nil)
			}
		}
		return shootUntilDown(w, ticks)
	}

	present := farm(false)
	if present == 0 {
		t.Fatal("the control case scored nothing; the test proves nothing")
	}
	if left := farm(true); left != 0 {
		t.Fatalf("a departed player paid out %d kills over %d ticks (a present one pays %d)",
			left, ticks, present)
	}
}

// Coming back inside the grace window puts the player back in the match.
func TestRejoinDuringTheLingerKeepsTheAvatarWhereItStood(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	w.Step(nil)
	at := [2]Milli{w.player(2).X, w.player(2).Y}

	w.Depart(2)
	w.Step(nil)
	w.Rejoin(2)
	for i := 0; i < int(DepartLingerTicks)+5; i++ {
		w.Step(nil)
	}

	p := w.player(2)
	if p.Left || !p.Alive {
		t.Fatalf("player did not come back: left=%v alive=%v", p.Left, p.Alive)
	}
	if ([2]Milli{p.X, p.Y}) != at {
		t.Fatalf("a rejoin inside the linger moved the avatar from %v to %v", at, [2]Milli{p.X, p.Y})
	}
}

// Coming back after the avatar has gone goes through the ordinary respawn,
// rather than reappearing on the spot where the body was left.
func TestRejoinAfterTheDespawnComesBackThroughRespawn(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	w.Depart(2)
	for i := 0; i < int(DepartLingerTicks)+2; i++ {
		w.Step(nil)
	}
	if w.player(2).Alive {
		t.Fatal("setup: the avatar should be gone")
	}

	w.Rejoin(2)
	if w.player(2).Alive {
		t.Fatal("rejoin put the player straight back in play with no respawn")
	}
	for i := 0; i < int(RespawnTicks)+2; i++ {
		w.Step(nil)
	}
	p := w.player(2)
	if !p.Alive || p.HP != MaxHP {
		t.Fatalf("player did not respawn after rejoining: alive=%v hp=%d", p.Alive, p.HP)
	}
	if p.Protect == 0 {
		t.Fatal("a rejoin respawn should carry the same protection as any other")
	}
}

// Depart and Rejoin are idempotent — the gateway can call either more than
// once, and a second Depart must not restart the linger clock.
func TestDepartAndRejoinAreIdempotent(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	w.Depart(2)
	w.Step(nil)
	w.Step(nil)
	after := w.player(2).Linger
	w.Depart(2)
	if got := w.player(2).Linger; got != after {
		t.Fatalf("a repeated Depart restarted the linger: %d -> %d", after, got)
	}

	w.Rejoin(2)
	alive := w.player(2).Alive
	w.Rejoin(2)
	if w.player(2).Alive != alive || w.player(2).Left {
		t.Fatal("a repeated Rejoin changed the player")
	}

	// Unknown ids are a no-op rather than a panic: a seat can leave a room it
	// was never seated in when a reconnect races a disconnect.
	w.Depart(999)
	w.Rejoin(999)
}

// Departure is simulation state, so it has to be in the fingerprint — a replay
// that leaves it out reproduces a different match and reports a clean checksum.
func TestDepartureIsPartOfTheChecksum(t *testing.T) {
	build := func(depart bool) uint64 {
		w := NewWorld(5, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
		if depart {
			w.Depart(2)
		}
		w.Step(nil)
		return w.Checksum()
	}
	if a, b := build(false), build(true); a == b {
		t.Fatal("a departed player leaves the checksum unchanged")
	}
}

// Once every human has left there is nobody to play for. The match ends rather
// than letting the bots run out the clock on a node that is counting the room
// against its ceiling the whole time.
func TestAMatchEndsWhenEveryHumanHasGone(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}, {ID: 1000, Bot: true}})

	w.Depart(1)
	w.Depart(2)
	var snap Snapshot
	for i := 0; i < int(DepartLingerTicks)+3; i++ {
		snap = w.Step(nil)
		if snap.Ended {
			break
		}
	}
	if !snap.Ended {
		t.Fatal("the match ran on with nobody in it")
	}
}

// The linger applies here too: one player dropping for a moment must not end
// the match for everybody.
func TestOneDepartureDoesNotEndTheMatch(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	w.Depart(2)
	for i := 0; i < int(DepartLingerTicks)*3; i++ {
		if w.Step(nil).Ended {
			t.Fatal("the match ended while a player was still in it")
		}
	}
}

// And a player who comes back inside the grace keeps the match alive.
func TestARejoinInsideTheGraceKeepsTheMatchRunning(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	w.Depart(1)
	w.Depart(2)
	for i := 0; i < int(DepartLingerTicks)/2; i++ {
		if w.Step(nil).Ended {
			t.Fatal("the match ended inside the linger")
		}
	}
	w.Rejoin(1)
	for i := 0; i < int(DepartLingerTicks)*2; i++ {
		if w.Step(nil).Ended {
			t.Fatal("the match ended with a player back in it")
		}
	}
}

// Scores stand when a match is abandoned. Walking out is not a way to avoid
// the rating — recordResult reads this snapshot.
func TestAnAbandonedMatchKeepsItsScores(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	w.player(1).Score = 5
	w.Depart(1)
	w.Depart(2)
	var snap Snapshot
	for i := 0; i < int(DepartLingerTicks)+3 && !snap.Ended; i++ {
		snap = w.Step(nil)
	}
	if !snap.Ended {
		t.Fatal("setup: the match did not end")
	}
	if snap.Winner != 1 {
		t.Fatalf("winner = %d, want 1 — the scores should stand", snap.Winner)
	}
}

// A roster with no human seats is not abandoned: nobody left it. The matchmaker
// does not produce one, but a bot-only world must not end on its first tick.
func TestABotOnlyRosterIsNotAbandoned(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1000, Bot: true}, {ID: 1001, Bot: true}})
	for i := 0; i < 50; i++ {
		if w.Step(nil).Ended {
			t.Fatal("a bot-only match ended as abandoned")
		}
	}
}

// A match where nobody has left is never abandoned, however long it runs — the
// check must key on departure and not on, say, players who are merely dead.
func TestDeadPlayersAreNotAbandonment(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	for _, id := range []PlayerID{1, 2} {
		p := w.player(id)
		p.Alive = false
		p.HP = 0
		p.Respawn = RespawnTicks
	}
	for i := 0; i < int(RespawnTicks)*2; i++ {
		if w.Step(nil).Ended {
			t.Fatal("a match with everyone dead was treated as abandoned")
		}
	}
}
