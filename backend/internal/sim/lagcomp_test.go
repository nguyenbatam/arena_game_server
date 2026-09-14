package sim

import "testing"

func fireWithLag(t *testing.T, lag uint8) Projectile {
	t.Helper()
	w := NewWorld(9, 20, 10_000, []Player{{ID: 1}})
	w.Step(nil)
	snap := w.Step(map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1, LagTicks: lag}})
	if len(snap.Projectiles) != 1 {
		t.Fatalf("expected exactly one projectile, got %d", len(snap.Projectiles))
	}
	return snap.Projectiles[0]
}

// A lagged shot starts further along its path, so it lands where the shooter
// was actually aiming rather than trailing behind by the round-trip.
func TestLagCompensationAdvancesProjectile(t *testing.T) {
	none := fireWithLag(t, 0)
	lagged := fireWithLag(t, 5)

	if lagged.X <= none.X {
		t.Fatalf("compensated shot should start ahead: lagged x=%d, uncompensated x=%d", lagged.X, none.X)
	}
	want := none.X + none.VX*5
	if lagged.X != want {
		t.Fatalf("x = %d, want %d (five ticks of travel)", lagged.X, want)
	}
	// It must not also get a full lifetime, or compensation would quietly
	// extend weapon range.
	if lagged.TTL != none.TTL-5 {
		t.Fatalf("ttl = %d, want %d", lagged.TTL, none.TTL-5)
	}
}

// Without a ceiling, a client that simply stops acking would buy itself an
// ever-growing head start.
func TestLagCompensationIsCapped(t *testing.T) {
	atCap := fireWithLag(t, MaxLagCompTicks)
	beyond := fireWithLag(t, MaxLagCompTicks+50)

	if beyond.X != atCap.X || beyond.Y != atCap.Y {
		t.Fatalf("lag beyond the cap must be clamped: got (%d,%d), cap gives (%d,%d)",
			beyond.X, beyond.Y, atCap.X, atCap.Y)
	}
}

// Compensation is part of the input record, so a replay of the same inputs must
// still reproduce the same world — determinism is not negotiable.
func TestLagCompensationStaysDeterministic(t *testing.T) {
	run := func() Snapshot {
		w := NewWorld(4242, 20, 200, []Player{{ID: 1}, {ID: 2}, {ID: 1000, Bot: true}})
		var last Snapshot
		for tick := 0; tick < 120; tick++ {
			last = w.Step(map[PlayerID]Input{
				1: {MX: 1, Fire: tick%6 == 0, Aim: int16(tick % 360), Seq: uint32(tick + 1), LagTicks: uint8(tick % 13)},
				2: {MY: 1, Fire: tick%5 == 0, Aim: int16((tick * 3) % 360), Seq: uint32(tick + 1), LagTicks: uint8(tick % 7)},
			})
		}
		return last
	}

	a, b := run(), run()
	if a.Tick != b.Tick || len(a.Players) != len(b.Players) || len(a.Projectiles) != len(b.Projectiles) {
		t.Fatalf("shape diverged: %+v vs %+v", a, b)
	}
	for i := range a.Players {
		if a.Players[i] != b.Players[i] {
			t.Fatalf("player %d diverged:\n %+v\n %+v", i, a.Players[i], b.Players[i])
		}
	}
	for i := range a.Projectiles {
		if a.Projectiles[i] != b.Projectiles[i] {
			t.Fatalf("projectile %d diverged:\n %+v\n %+v", i, a.Projectiles[i], b.Projectiles[i])
		}
	}
}

// A lagged shot must still hit someone standing in its path. Teleporting the
// bullet to the end of its catch-up would pass straight through them: the
// shooter sees a hit, the server records nothing.
func TestLagCompensationHitsTargetsAlongTheCatchUpPath(t *testing.T) {
	// Two players; the shooter aims at the other and fires with heavy lag.
	w := NewWorld(3, 20, 10_000, []Player{{ID: 1}, {ID: 2}})
	w.Step(nil)

	shooter := w.player(1)
	target := w.player(2)

	// Put the target a few ticks of flight down the +x axis, and aim there.
	projPerTick := w.projTick
	target.X = shooter.X + projPerTick*3
	target.Y = shooter.Y
	shooter.Aim = 0

	before := target.HP
	w.Step(map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1, LagTicks: 8}})

	if got := w.player(2).HP; got >= before {
		t.Fatalf("target HP %d unchanged from %d — the catch-up skipped over them", got, before)
	}
}

// Scoring and respawn must be identical whether the hit lands during catch-up
// or during a normal tick, since both go through the same damage path.
func TestCatchUpKillScoresLikeANormalKill(t *testing.T) {
	w := NewWorld(11, 20, 10_000, []Player{{ID: 1}, {ID: 2}})
	w.Step(nil)

	shooter := w.player(1)
	target := w.player(2)
	target.X = shooter.X + w.projTick*2
	target.Y = shooter.Y
	target.HP = Damage // one hit from death
	shooter.Aim = 0

	w.Step(map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1, LagTicks: 6}})

	killed := w.player(2)
	if killed.Alive {
		t.Fatal("target survived a lethal catch-up hit")
	}
	if killed.Respawn == 0 {
		t.Fatal("killed target was not put on a respawn timer")
	}
	if got := w.player(1).Score; got != 1 {
		t.Fatalf("shooter score = %d, want 1 — a catch-up kill did not credit the shooter", got)
	}
}
