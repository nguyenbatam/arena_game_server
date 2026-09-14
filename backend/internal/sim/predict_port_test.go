package sim

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// web/predict.js is a hand port of the movement math in this package. A port
// silently drifting from its original is the classic way client prediction
// starts feeling wrong, so run both and compare.
func TestWebPredictionMatchesSim(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed; skipping JS parity check")
	}
	script, err := filepath.Abs("../../../web/predict.js")
	if err != nil {
		t.Fatal(err)
	}

	type step struct {
		MX, MY   int8
		TickRate int
	}
	steps := []step{
		{1, 0, 20}, {-1, 0, 20}, {0, 1, 20}, {0, -1, 20},
		{1, 1, 20}, {-1, -1, 20}, {1, -1, 20}, {-1, 1, 20},
		{1, 1, 30}, {-1, 1, 30}, {1, 0, 60}, {1, 1, 60},
		{2, -3, 20}, // out-of-range input must clamp the same way on both sides
		{0, 0, 20},
	}

	type pair struct {
		X0, Y0   int32
		MX, MY   int8
		SpeedTck int32
		X1, Y1   int32
	}
	cases := make([]pair, 0, len(steps))

	for _, st := range steps {
		w := NewWorld(1, st.TickRate, 10_000, []Player{{ID: 1}})
		before := w.Step(nil) // settle at the spawn point
		p0 := before.Players[0]

		after := w.Step(map[PlayerID]Input{1: {MX: st.MX, MY: st.MY, Seq: 1}})
		p1 := after.Players[0]

		cases = append(cases, pair{
			X0: int32(p0.X), Y0: int32(p0.Y), MX: st.MX, MY: st.MY,
			SpeedTck: int32(w.speedTick), X1: int32(p1.X), Y1: int32(p1.Y),
		})
	}

	in, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}

	js := fmt.Sprintf(`
require(%q);
const cases = %s;
const out = cases.map(c => {
  const p = globalThis.simStepMove(c.X0, c.Y0, c.MX, c.MY, c.SpeedTck);
  return [p.x, p.y];
});
console.log(JSON.stringify(out));
`, script, string(in))

	raw, err := exec.Command(node, "-e", js).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got [][2]int64
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(raw))), &got); err != nil {
		t.Fatalf("decoding node output %q: %v", raw, err)
	}
	if len(got) != len(cases) {
		t.Fatalf("node returned %d results, want %d", len(got), len(cases))
	}

	for i, c := range cases {
		if int32(got[i][0]) != c.X1 || int32(got[i][1]) != c.Y1 {
			t.Errorf("step %d (mx=%d my=%d speedTick=%d): js gave (%d,%d), sim gave (%d,%d)",
				i, c.MX, c.MY, c.SpeedTck, got[i][0], got[i][1], c.X1, c.Y1)
		}
	}

	// A speedTick mismatch would make every step wrong, so check it explicitly.
	for _, tr := range []int{20, 30, 60} {
		want := int32(PlayerSpeed / Milli(tr))
		out, err := exec.Command(node, "-e",
			fmt.Sprintf("require(%q);console.log(globalThis.simSpeedTickFor(%d))", script, tr)).Output()
		if err != nil {
			t.Fatalf("node: %v", err)
		}
		var gotTick int32
		fmt.Sscan(strings.TrimSpace(string(out)), &gotTick)
		if gotTick != want {
			t.Errorf("speedTick at %d Hz: js=%d sim=%d", tr, gotTick, want)
		}
	}
}
