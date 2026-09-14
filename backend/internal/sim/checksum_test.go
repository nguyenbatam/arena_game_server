package sim

import "testing"

func replayWorld(t *testing.T, seed int64) *World {
	t.Helper()
	w := NewWorld(seed, 20, 200, []Player{{ID: 1}, {ID: 2}, {ID: 3, Bot: true}})
	for tick := 0; tick < 120; tick++ {
		in := map[PlayerID]Input{
			1: {MX: 1, MY: int8(tick % 2), Fire: tick%5 == 0, Aim: int16(tick % 360), Seq: uint32(tick + 1)},
			2: {MX: -1, Fire: tick%7 == 0, Aim: int16((tick * 3) % 360), Seq: uint32(tick + 1)},
		}
		w.Step(in)
	}
	return w
}

func TestChecksumIsStableAcrossRuns(t *testing.T) {
	a := replayWorld(t, 42).Checksum()
	b := replayWorld(t, 42).Checksum()
	if a != b {
		t.Fatalf("same seed and inputs gave %#x then %#x", a, b)
	}
}

func TestChecksumSeparatesDifferentWorlds(t *testing.T) {
	if replayWorld(t, 42).Checksum() == replayWorld(t, 43).Checksum() {
		t.Fatal("two different seeds produced the same checksum")
	}
}

// A checksum that does not move when the world does would verify nothing.
func TestChecksumMovesWithTheWorld(t *testing.T) {
	w := NewWorld(7, 20, 200, []Player{{ID: 1}, {ID: 2}})
	before := w.Checksum()
	w.Step(map[PlayerID]Input{1: {MX: 1, Seq: 1}})
	if w.Checksum() == before {
		t.Fatal("a player moved and the checksum did not change")
	}
}
