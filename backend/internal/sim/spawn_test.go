package sim

import "testing"

// Spawn points must stay distinct across a respawn, not just on the opening
// deal. They did not: respawn derived the ring slot from the player id modulo a
// hardcoded 8, so any two ids congruent mod 8 landed on the same point — and
// the default roster guarantees a pair, since seats count from 1 and bots from
// 1000.
func TestRespawnKeepsPlayersApart(t *testing.T) {
	for _, n := range []int{2, 4, 8, 9, 16, 24} {
		roster := make([]Player, 0, n)
		for i := 1; i <= n; i++ {
			roster = append(roster, Player{ID: PlayerID(i)})
		}
		w := NewWorld(1, 20, 1<<20, roster)

		assertDistinct := func(stage string) {
			t.Helper()
			seen := map[[2]Milli]PlayerID{}
			for _, p := range w.players {
				k := [2]Milli{p.X, p.Y}
				if other, dup := seen[k]; dup {
					t.Errorf("n=%d %s: players %d and %d share %v", n, stage, other, p.ID, k)
				}
				seen[k] = p.ID
			}
		}
		assertDistinct("spawn")

		for i := range w.players {
			w.players[i].Alive = false
			w.players[i].Respawn = 0
		}
		w.respawn()
		assertDistinct("respawn")
	}
}

// The exact shape that shipped: a full default room, humans on seats 1..8 and
// bots numbered from 1000.
func TestRespawnSeparatesBotsFromSeats(t *testing.T) {
	roster := make([]Player, 0, 8)
	for i := 1; i <= 4; i++ {
		roster = append(roster, Player{ID: PlayerID(i)})
	}
	for i := 0; i < 4; i++ {
		roster = append(roster, Player{ID: PlayerID(1000 + i), Bot: true})
	}
	w := NewWorld(9, 20, 1<<20, roster)
	for i := range w.players {
		w.players[i].Alive = false
		w.players[i].Respawn = 0
	}
	w.respawn()

	seen := map[[2]Milli]PlayerID{}
	for _, p := range w.players {
		k := [2]Milli{p.X, p.Y}
		if other, dup := seen[k]; dup {
			t.Fatalf("player %d and player %d respawn on the same point %v", other, p.ID, k)
		}
		seen[k] = p.ID
	}
}

// With nobody alive to avoid, the safest-slot rule degenerates to the even ring
// — every slot is equally safe, ties go to the lowest free index, and the
// roster is walked in its sorted order. So a whole-room respawn reproduces the
// opening deal exactly.
//
// This used to be the rule itself rather than its degenerate case: respawn
// always returned a player to the slot they opened on, which made the point
// fixed for the whole match and free to camp. It is kept because it is still
// the behaviour when the map is empty, and because it pins the tie-break that
// keeps two simultaneous respawns apart.
func TestRespawnFallsBackToTheEvenRingWhenNobodyIsAlive(t *testing.T) {
	roster := []Player{{ID: 3}, {ID: 1000, Bot: true}, {ID: 77}}
	w := NewWorld(4, 20, 1<<20, roster)
	opening := map[PlayerID][2]Milli{}
	for _, p := range w.players {
		opening[p.ID] = [2]Milli{p.X, p.Y}
	}
	for i := range w.players {
		w.players[i].Alive = false
		w.players[i].Respawn = 0
		w.players[i].X, w.players[i].Y = 1, 1
	}
	w.respawn()
	for _, p := range w.players {
		if got := ([2]Milli{p.X, p.Y}); got != opening[p.ID] {
			t.Errorf("player %d respawned at %v, opened at %v", p.ID, got, opening[p.ID])
		}
	}
}

// Spawn slots survive the roster sort: NewWorld reorders by id, so a slot taken
// from the caller's ordering would drift.
func TestSpawnSlotFollowsSortedRoster(t *testing.T) {
	unsorted := []Player{{ID: 90}, {ID: 2}, {ID: 40}}
	sorted := []Player{{ID: 2}, {ID: 40}, {ID: 90}}
	a := NewWorld(1, 20, 1<<20, unsorted)
	b := NewWorld(1, 20, 1<<20, sorted)
	for i := range a.players {
		if a.players[i].ID != b.players[i].ID ||
			a.players[i].X != b.players[i].X || a.players[i].Y != b.players[i].Y {
			t.Fatalf("roster order changed the deal: %+v vs %+v", a.players[i], b.players[i])
		}
	}
}
