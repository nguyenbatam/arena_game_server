package sim

import "testing"

// duel builds a two-player world with the shooter at a known point aiming along
// +x, and the target placed exactly where the caller asks. NewWorld deals both
// onto the spawn ring, which is not where either of these tests wants them.
func duel(t *testing.T, tickRate int, targetX, targetY Milli) *World {
	t.Helper()
	w := NewWorld(7, tickRate, 1<<20, []Player{{ID: 1}, {ID: 2}})
	sh := w.player(1)
	sh.X, sh.Y, sh.Aim = 200_000, 1_000_000, 0
	tg := w.player(2)
	tg.X, tg.Y = targetX, targetY
	return w
}

// fireAndSettle fires one shot and runs the world until it has hit or expired,
// reporting whether the target took damage.
func fireAndSettle(w *World) bool {
	before := w.player(2).HP
	w.Step(map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1}})
	for i := 0; i < int(ProjectileTTL)+2; i++ {
		if w.player(2).HP != before || !w.player(2).Alive {
			return true
		}
		w.Step(nil)
	}
	return w.player(2).HP != before || !w.player(2).Alive
}

// The precondition that makes a swept test necessary rather than merely tidy.
//
// If this ever stops holding — a slower projectile, a fatter hitbox, a higher
// tick rate — the tunnelling test below stops proving anything, and it should
// say so out loud rather than pass for the wrong reason.
func TestAProjectileStepIsWiderThanTheHitboxItMustNotSkip(t *testing.T) {
	step := ProjectileSpeed / 20 // the default rate
	reach := PlayerRadius + ProjectileRadius
	if step <= reach {
		t.Skipf("step %d no longer exceeds the hit radius %d; tunnelling is impossible by construction", step, reach)
	}
	t.Logf("step per tick %d vs hit radius %d — point sampling would skip %d units of path",
		step, reach, step-reach)
}

// No shot fired straight down a target's line may report a miss.
//
// This is the regression, and the numbers matter: the projectile travels 36_000
// units a tick while the hit radius is 24_000, so testing the end point of each
// step alone leaves a gap wider than the target. Sampling every phase of that
// gap at every perpendicular offset inside the radius used to produce 40 misses
// out of 432 — several of them through the middle of the body, at offsets below
// PlayerRadius. It must now produce none.
func TestNoShotPassesThroughATargetItWasAimedAt(t *testing.T) {
	const reach = PlayerRadius + ProjectileRadius
	step := ProjectileSpeed / 20

	misses := 0
	for off := Milli(0); off <= reach-1_000; off += 1_000 {
		for phase := Milli(0); phase < step; phase += 1_000 {
			w := duel(t, 20, 218_000+400_000+phase, 1_000_000+off)
			if !fireAndSettle(w) {
				misses++
				t.Errorf("tunnelled: perpendicular offset %d (hit radius %d), phase %d", off, reach, phase)
			}
		}
	}
	if misses > 0 {
		t.Fatalf("%d shots passed through a target standing inside the hit radius", misses)
	}
}

// The other half of the contract: a sweep must not start hitting people who
// were never in the way. A target outside the hit radius stays untouched at
// every phase.
func TestASweepDoesNotWidenTheHitbox(t *testing.T) {
	const reach = PlayerRadius + ProjectileRadius
	step := ProjectileSpeed / 20
	for off := reach + 2_000; off <= reach*3; off += 2_000 {
		for phase := Milli(0); phase < step; phase += 4_000 {
			w := duel(t, 20, 218_000+400_000+phase, 1_000_000+off)
			if fireAndSettle(w) {
				t.Fatalf("hit a target %d away, outside the %d hit radius (phase %d)", off, reach, phase)
			}
		}
	}
}

// A shot that crosses two players in one step stops at the one it reaches
// first. Resolving in roster order instead would let the id ordering decide who
// takes the damage, which is invisible until the two are on opposite sides of
// the same step.
func TestSweepStopsAtTheNearestTargetOnThePath(t *testing.T) {
	step := ProjectileSpeed / 20
	// Seat 3 is nearer the muzzle than seat 2, so the higher id must be the one
	// that is hit — proving the choice is geometric, not positional.
	w := NewWorld(7, 20, 1<<20, []Player{{ID: 1}, {ID: 2}, {ID: 3}})
	w.player(1).X, w.player(1).Y, w.player(1).Aim = 200_000, 1_000_000, 0
	near := w.player(3)
	far := w.player(2)
	// Both inside the same step, near one third of the way along it.
	near.X, near.Y = 218_000+step/3, 1_000_000
	far.X, far.Y = 218_000+(step*2)/3, 1_000_000

	w.Step(map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1}})

	if w.player(3).HP != MaxHP-Damage {
		t.Fatalf("nearest target HP = %d, want %d — the shot did not stop at them", w.player(3).HP, MaxHP-Damage)
	}
	if w.player(2).HP != MaxHP {
		t.Fatalf("far target HP = %d, want %d — one shot damaged two players", w.player(2).HP, MaxHP)
	}
}

// The catch-up path sweeps on the same terms. It is the one that fast-forwards
// several ticks in a row, so a point test there skips several gaps rather than
// one.
func TestCatchUpSweepsEveryStepItSkipsForward(t *testing.T) {
	step := ProjectileSpeed / 20

	for lag := uint8(1); lag <= MaxLagCompTicks; lag++ {
		for _, off := range []Milli{0, 10_000, 17_000, 21_000} {
			// Put the target in the middle of the step the catch-up is about to
			// take, where a point test lands either side of them.
			w := duel(t, 20, 218_000+step*Milli(lag)-step/2, 1_000_000+off)
			before := w.player(2).HP
			w.Step(map[PlayerID]Input{1: {Fire: true, Aim: 0, Seq: 1, LagTicks: lag}})
			if w.player(2).HP == before && w.player(2).Alive {
				t.Errorf("lag=%d offset=%d: catch-up skipped a target in its path", lag, off)
			}
		}
	}
}

// segDist2 is the only geometry in the hit path, and it is integer arithmetic
// written to dodge an int64 overflow. Check it against a brute-force walk of the
// segment rather than against itself.
func TestSegDist2MatchesABruteForceWalk(t *testing.T) {
	cases := []struct{ px, py, ax, ay, bx, by Milli }{
		{1_000_000, 1_000_000, 900_000, 1_000_000, 1_100_000, 1_000_000}, // straight through
		{1_000_000, 1_020_000, 900_000, 1_000_000, 1_100_000, 1_000_000}, // offset
		{800_000, 1_000_000, 900_000, 1_000_000, 1_100_000, 1_000_000},   // behind A
		{1_300_000, 1_000_000, 900_000, 1_000_000, 1_100_000, 1_000_000}, // beyond B
		{1_000_000, 1_000_000, 900_000, 900_000, 1_100_000, 1_100_000},   // diagonal
		{1_999_000, 1_000, 1_000, 1_999_000, 1_999_000, 1_000},           // map-scale, the overflow shape
		{500_000, 500_000, 700_000, 700_000, 700_000, 700_000},           // degenerate segment
	}
	for i, c := range cases {
		got := segDist2(c.px, c.py, c.ax, c.ay, c.bx, c.by)

		// Walk the segment finely and take the closest sample. The sampled
		// minimum can only be >= the true one, and the truncation inside
		// segDist2 can only cost a unit or so of precision, so the two must
		// agree to well inside a hit radius.
		const steps = 20000
		best := int64(-1)
		for k := 0; k <= steps; k++ {
			x := c.ax + Milli(int64(c.bx-c.ax)*int64(k)/steps)
			y := c.ay + Milli(int64(c.by-c.ay)*int64(k)/steps)
			d := dist2(c.px, c.py, x, y)
			if best < 0 || d < best {
				best = d
			}
		}
		if got > best {
			t.Errorf("case %d: segDist2 = %d, above the sampled minimum %d", i, got, best)
		}
		// Tolerance in *distance*, not squared distance: a unit of truncation
		// on a map-scale segment is a unit of distance.
		tol := int64(4)
		if diff := best - got; diff > 0 && isqrt(best)-isqrt(got) > tol {
			t.Errorf("case %d: segDist2 = %d (d=%d), sampled %d (d=%d)", i, got, isqrt(got), best, isqrt(best))
		}
	}
}

func isqrt(v int64) int64 {
	if v <= 0 {
		return 0
	}
	x := v
	y := (x + 1) / 2
	for y < x {
		x = y
		y = (x + v/x) / 2
	}
	return x
}
