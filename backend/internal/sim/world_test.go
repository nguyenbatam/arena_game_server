package sim

import "testing"

func TestDeterministicReplay(t *testing.T) {
	roster := []Player{{ID: 1}, {ID: 2, Bot: true}, {ID: 3}}
	script := []map[PlayerID]Input{
		{1: {MX: 1, MY: 0, Aim: 0}, 3: {MX: -1, Fire: true, Aim: 180}},
		{1: {MX: 1, MY: 1, Fire: true, Aim: 45}},
		{1: {MY: -1, Aim: 270}, 3: {MX: 1, MY: 1, Fire: true, Aim: 10}},
	}

	run := func() Snapshot {
		w := NewWorld(42, 20, 200, roster)
		var last Snapshot
		for i := 0; i < 80; i++ {
			in := map[PlayerID]Input{}
			if i < len(script) {
				in = script[i]
			} else if i%7 == 0 {
				in[1] = Input{MX: int8(i%3 - 1), Fire: i%5 == 0, Aim: int16(i * 17)}
			}
			last = w.Step(in)
		}
		return last
	}

	a, b := run(), run()
	if a.Tick != b.Tick || a.Winner != b.Winner || len(a.Players) != len(b.Players) {
		t.Fatalf("mismatch header %+v vs %+v", a, b)
	}
	for i := range a.Players {
		if a.Players[i] != b.Players[i] {
			t.Fatalf("player %d: %+v vs %+v", i, a.Players[i], b.Players[i])
		}
	}
	if len(a.Projectiles) != len(b.Projectiles) {
		t.Fatalf("proj count %d vs %d", len(a.Projectiles), len(b.Projectiles))
	}
}

// A match nobody won is a draw, not a win for whoever holds the lowest seat
// number. internal/rating already scored it that way; the snapshot did not, so
// the scoreboard and the ladder disagreed about the same match.
func TestLeaderTieBreakIsADraw(t *testing.T) {
	w := NewWorld(1, 20, 1, []Player{{ID: 3}, {ID: 1}, {ID: 2}})
	snap := w.Step(nil)
	if !snap.Ended {
		t.Fatal("match should have ended")
	}
	if snap.Winner != 0 {
		t.Fatalf("0-0-0 is a draw, got winner=%d", snap.Winner)
	}
}

// An outright lead is still reported, and a tie *below* the lead does not turn
// the match into a draw.
func TestLeaderReportsAnOutrightWinner(t *testing.T) {
	w := NewWorld(1, 20, 1<<20, []Player{{ID: 1}, {ID: 2}, {ID: 3}})
	w.player(2).Score = 5
	w.player(1).Score = 3
	w.player(3).Score = 3
	if got := w.leader(); got != 2 {
		t.Fatalf("winner = %d, want 2", got)
	}
}

// Two players tied at the top is a draw even when a third trails them.
func TestLeaderDrawsWhenTheTopIsShared(t *testing.T) {
	w := NewWorld(1, 20, 1<<20, []Player{{ID: 1}, {ID: 2}, {ID: 3}})
	w.player(1).Score = 4
	w.player(2).Score = 4
	w.player(3).Score = 1
	if got := w.leader(); got != 0 {
		t.Fatalf("winner = %d, want 0 (draw)", got)
	}
}

func TestStaleSeqIgnored(t *testing.T) {
	w := NewWorld(1, 20, 200, []Player{{ID: 1}})
	w.Step(map[PlayerID]Input{1: {MX: 1, Seq: 5}})
	x := w.Players()[0].X
	w.Step(map[PlayerID]Input{1: {MX: 1, Seq: 4}})
	if w.Players()[0].X != x {
		t.Fatal("stale seq moved player")
	}
	w.Step(map[PlayerID]Input{1: {MX: 1, Seq: 6}})
	if w.Players()[0].X == x {
		t.Fatal("fresh seq should move")
	}
}

func TestIntegerBounds(t *testing.T) {
	w := NewWorld(1, 20, 50, []Player{{ID: 1}})
	for i := 0; i < 200; i++ {
		w.Step(map[PlayerID]Input{1: {MX: 1, MY: 1, Fire: true, Aim: 45}})
	}
	p := w.Players()[0]
	if p.X < 0 || p.Y < 0 || p.X > MapW || p.Y > MapH {
		t.Fatalf("out of bounds %d,%d", p.X, p.Y)
	}
}
