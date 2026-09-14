package turn

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// web/cursor.js decides when the browser has missed events and must resync.
// Get it wrong in the permissive direction and a client silently drops part of
// the log — the one failure this sync family does not tolerate — so the cases
// are pinned here rather than left to whoever edits the page next.
func TestWebCursorGapDetection(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed; skipping JS parity check")
	}
	script, err := filepath.Abs("../../../web/cursor.js")
	if err != nil {
		t.Fatal(err)
	}

	type event struct {
		Seq uint64 `json:"seq"`
	}
	type update struct {
		CurrentSeq uint64  `json:"current_seq"`
		Events     []event `json:"events"`
	}
	type kase struct {
		name   string
		seq    uint64
		update update
		want   bool
	}
	cases := []kase{
		{"contiguous run", 4, update{CurrentSeq: 6, Events: []event{{5}, {6}}}, false},
		{"single next event", 0, update{CurrentSeq: 1, Events: []event{{1}}}, false},
		{"nothing new", 6, update{CurrentSeq: 6}, false},
		{"stale duplicate", 6, update{CurrentSeq: 5, Events: []event{{5}}}, false},

		{"owed events but given none", 4, update{CurrentSeq: 6}, true},
		{"run starts too late", 4, update{CurrentSeq: 7, Events: []event{{6}, {7}}}, true},
		{"hole inside the run", 4, update{CurrentSeq: 7, Events: []event{{5}, {7}}}, true},
		{"run stops short of current", 4, update{CurrentSeq: 7, Events: []event{{5}, {6}}}, true},
	}

	payload := make([]update, len(cases))
	seqs := make([]uint64, len(cases))
	for i, c := range cases {
		payload[i], seqs[i] = c.update, c.seq
	}
	blob, err := json.Marshal(map[string]any{"updates": payload, "seqs": seqs})
	if err != nil {
		t.Fatal(err)
	}

	js := fmt.Sprintf(`
const { hasGap } = require(%q);
const { updates, seqs } = JSON.parse(process.argv[1]);
console.log(JSON.stringify(updates.map((u, i) => hasGap(u, seqs[i]))));
`, script)

	out, err := exec.Command(node, "-e", js, string(blob)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var got []bool
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(out))), &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if len(got) != len(cases) {
		t.Fatalf("got %d results for %d cases", len(got), len(cases))
	}
	for i, c := range cases {
		if got[i] != c.want {
			t.Errorf("%s: hasGap = %v, want %v", c.name, got[i], c.want)
		}
	}
}
