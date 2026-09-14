package sim

import "testing"

// killAndRespawn kills seat 2 and runs the world forward until they are back,
// returning the world with the fresh spawn in place.
func killAndRespawn(t *testing.T, w *World) {
	t.Helper()
	tg := w.player(2)
	tg.HP = 0
	tg.Alive = false
	tg.Respawn = RespawnTicks
	for i := 0; i <= int(RespawnTicks)+1; i++ {
		w.Step(nil)
		if w.player(2).Alive {
			return
		}
	}
	t.Fatal("target never came back")
}

// A player who has just respawned cannot be shot off the spawn point.
//
// Without this the optimal play is to stand on the point somebody is about to
// reappear on: the countdown is in the snapshot and the ring never moves, so
// the kill is free and repeatable.
func TestARespawnedPlayerCannotBeShotImmediately(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	killAndRespawn(t, w)

	tg := w.player(2)
	if tg.Protect == 0 {
		t.Fatal("respawn granted no protection")
	}
	// Put the camper on top of them and fire.
	sh := w.player(1)
	sh.X, sh.Y, sh.Aim = tg.X-100_000, tg.Y, 0
	before := tg.HP
	w.Step(map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1}})
	w.Step(nil)
	w.Step(nil)

	if got := w.player(2).HP; got != before {
		t.Fatalf("protected player took damage: HP %d -> %d", before, got)
	}
}

// Protection is a head start, not a state. It runs out on its own.
func TestSpawnProtectionExpires(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	killAndRespawn(t, w)
	for i := 0; i < int(SpawnProtectTicks)+1; i++ {
		w.Step(nil)
	}
	if w.player(2).Protect != 0 {
		t.Fatalf("protection still standing after %d ticks", SpawnProtectTicks+1)
	}

	tg := w.player(2)
	sh := w.player(1)
	sh.X, sh.Y, sh.Aim = tg.X-100_000, tg.Y, 0
	before := tg.HP
	w.Step(map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1}})
	w.Step(nil)
	w.Step(nil)
	if w.player(2).HP == before {
		t.Fatal("player is still immune after the protection expired")
	}
}

// Firing gives it up. Protection exists to get off the spawn point, and a
// player who can shoot from inside it is holding a shield, not a head start.
func TestFiringCancelsSpawnProtection(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	killAndRespawn(t, w)
	if w.player(2).Protect == 0 {
		t.Fatal("respawn granted no protection")
	}

	// The protected player takes a shot of their own.
	w.Step(map[PlayerID]Input{2: {Fire: true, Aim: 90, Seq: 1}})
	if got := w.player(2).Protect; got != 0 {
		t.Fatalf("protection survived a shot: %d ticks left", got)
	}
}

// A protected player must not absorb the bullet either. Stopping shots dead on
// somebody who takes no damage from them turns the spawn point into cover,
// which is worse than having no protection at all.
func TestAProtectedPlayerDoesNotSoakTheShot(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}, {ID: 3}})
	sh, shield, victim := w.player(1), w.player(2), w.player(3)

	sh.X, sh.Y, sh.Aim = 200_000, 1_000_000, 0
	shield.X, shield.Y = 400_000, 1_000_000
	shield.Protect = SpawnProtectTicks
	victim.X, victim.Y = 700_000, 1_000_000

	before := victim.HP
	w.Step(map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1}})
	for i := 0; i < 15 && w.player(3).HP == before; i++ {
		w.Step(nil)
	}

	if w.player(2).HP != MaxHP {
		t.Fatalf("protected player took damage: %d", w.player(2).HP)
	}
	if w.player(3).HP == before {
		t.Fatal("the shot was absorbed by the protected player instead of passing through")
	}
}

// A kill that lands on a protected player must not be scored either — the
// protection is checked inside damage, which is the one place score is awarded.
func TestNoScoreIsPaidForShootingAProtectedPlayer(t *testing.T) {
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}})
	sh, tg := w.player(1), w.player(2)
	sh.X, sh.Y, sh.Aim = 200_000, 1_000_000, 0
	tg.X, tg.Y = 400_000, 1_000_000
	tg.HP = Damage // one hit from death, if the hit landed
	tg.Protect = SpawnProtectTicks

	for i := 0; i < 10; i++ {
		w.Step(map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: uint32(i + 1)}})
	}
	if got := w.player(1).Score; got != 0 {
		t.Fatalf("shooter scored %d off a protected player", got)
	}
}

// Respawning must not put somebody back on top of a player who is standing
// there waiting. The slot is chosen by distance to the nearest living player,
// which is the rule every arena shooter converges on.
func TestRespawnAvoidsTheCampedPoint(t *testing.T) {
	roster := []Player{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}
	w := NewWorld(3, 20, 1<<20, roster)

	// Seat 1 dies; seats 2..4 crowd around the point seat 1 opened on.
	opening := [2]Milli{w.player(1).X, w.player(1).Y}
	for _, id := range []PlayerID{2, 3, 4} {
		p := w.player(id)
		p.X, p.Y = opening[0], opening[1]+Milli(20_000*int(id))
	}
	dead := w.player(1)
	dead.Alive = false
	dead.HP = 0
	dead.Respawn = 0
	w.respawn()

	back := w.player(1)
	if !back.Alive {
		t.Fatal("player did not come back")
	}
	if ([2]Milli{back.X, back.Y}) == opening {
		t.Fatal("respawned onto the point three players are standing on")
	}
	// And it really is the furthest free point, not merely a different one.
	nearest := int64(-1)
	for _, id := range []PlayerID{2, 3, 4} {
		o := w.player(id)
		if d := dist2(back.X, back.Y, o.X, o.Y); nearest < 0 || d < nearest {
			nearest = d
		}
	}
	n := len(w.players)
	for slot := 0; slot < n; slot++ {
		sx, sy := spawnPoint(slot, n)
		worst := int64(-1)
		for _, id := range []PlayerID{2, 3, 4} {
			o := w.player(id)
			if d := dist2(sx, sy, o.X, o.Y); worst < 0 || d < worst {
				worst = d
			}
		}
		if worst > nearest {
			t.Fatalf("slot %d was safer (%d) than the one chosen (%d)", slot, worst, nearest)
		}
	}
}

// Respawn selection must stay deterministic: it is inside the simulation, so a
// replay of the same inputs has to land on the same points.
func TestRespawnSelectionIsDeterministic(t *testing.T) {
	run := func() uint64 {
		w := NewWorld(99, 20, 400, []Player{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 1000, Bot: true}})
		for tick := 0; tick < 400; tick++ {
			w.Step(map[PlayerID]Input{
				1: {MX: 1, Fire: tick%3 == 0, Aim: int16(tick % 360), Seq: uint32(tick + 1)},
				2: {MY: 1, Fire: tick%4 == 0, Aim: int16((tick * 7) % 360), Seq: uint32(tick + 1)},
				3: {MX: -1, Fire: tick%5 == 0, Aim: int16((tick * 11) % 360), Seq: uint32(tick + 1)},
			})
		}
		return w.Checksum()
	}
	if a, b := run(), run(); a != b {
		t.Fatalf("respawn selection diverged between identical runs: %d vs %d", a, b)
	}
}
