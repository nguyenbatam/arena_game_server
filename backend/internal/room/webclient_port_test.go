package room

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
)

// web/pb.js carries a hand port of ApplyDelta, and says so in a comment. A port
// whose original moves on is how a client starts disagreeing with the server
// about what the world looks like — silently, in the browser, where none of the
// Go tests are watching.
//
// What is compared is the decision and the result: whether each implementation
// accepts a snapshot at all, and the exact state it ends up holding — every
// field of every player, not just the ids.

type jsPlayer struct {
	ID      uint32 `json:"id"`
	X       int32  `json:"x"`
	Y       int32  `json:"y"`
	Aim     int32  `json:"aim"`
	HP      int32  `json:"hp"`
	Score   uint32 `json:"score"`
	Bot     bool   `json:"bot"`
	Changed uint32 `json:"changed"`
	Seq     uint32 `json:"seq"`
}

type jsProj struct {
	ID uint32 `json:"id"`
	X  int32  `json:"x"`
	Y  int32  `json:"y"`
}

type jsEvent struct {
	Tick   uint32 `json:"tick"`
	Kind   int32  `json:"kind"`
	Actor  uint32 `json:"actor"`
	Target uint32 `json:"target"`
	HP     int32  `json:"hp"`
}

type jsSnap struct {
	Tick               uint32     `json:"tick"`
	BaselineTick       uint32     `json:"baseline_tick"`
	RoomID             string     `json:"room_id"`
	Ended              bool       `json:"ended"`
	Winner             uint32     `json:"winner"`
	Players            []jsPlayer `json:"players"`
	Projectiles        []jsProj   `json:"projectiles"`
	RemovedProjectiles []uint32   `json:"removed_projectiles"`
	Events             []jsEvent  `json:"events"`
}

// toJS renders a snapshot the way web/pb.js's decoder would hand it over.
func toJS(s *pb.Snapshot) *jsSnap {
	if s == nil {
		return nil
	}
	out := &jsSnap{
		Tick: s.Tick, BaselineTick: s.BaselineTick, RoomID: s.RoomId,
		Ended: s.Ended, Winner: s.Winner,
		Players:            []jsPlayer{},
		Projectiles:        []jsProj{},
		RemovedProjectiles: []uint32{},
		Events:             []jsEvent{},
	}
	for _, p := range s.Players {
		out.Players = append(out.Players, jsPlayer{
			ID: p.Id, X: p.X, Y: p.Y, Aim: p.Aim, HP: p.Hp,
			Score: p.Score, Bot: p.Bot, Changed: p.Changed, Seq: p.Seq,
		})
	}
	for _, q := range s.Projectiles {
		out.Projectiles = append(out.Projectiles, jsProj{ID: q.Id, X: q.X, Y: q.Y})
	}
	out.RemovedProjectiles = append(out.RemovedProjectiles, s.RemovedProjectiles...)
	for _, e := range s.Events {
		out.Events = append(out.Events, jsEvent{
			Tick: e.Tick, Kind: int32(e.Kind), Actor: e.Actor, Target: e.Target, HP: e.Hp,
		})
	}
	return out
}

// appliedPlayer is one rebuilt player, as both implementations hand it back.
// No `changed` — that belongs to the message, not to the state it produced.
type appliedPlayer struct {
	ID    uint32 `json:"id"`
	X     int32  `json:"x"`
	Y     int32  `json:"y"`
	Aim   int32  `json:"aim"`
	HP    int32  `json:"hp"`
	Score uint32 `json:"score"`
	Bot   bool   `json:"bot"`
	Seq   uint32 `json:"seq"`
}

// applied is what both sides are reduced to for comparison: refused, or the
// state they came back holding.
type applied struct {
	Refused     bool            `json:"refused"`
	Players     []appliedPlayer `json:"players"`
	Projectiles []jsProj        `json:"projectiles"`
	// Events are carried through rather than merged, so both sides have to hand
	// back the same list — a port that dropped them would leave the browser
	// with no killfeed and no test to say so.
	Events []jsEvent `json:"events"`
}

func goApplied(base, d *pb.Snapshot) applied {
	out := ApplyDelta(base, d)
	if out == nil {
		return applied{Refused: true, Players: []appliedPlayer{}, Projectiles: []jsProj{}, Events: []jsEvent{}}
	}
	got := applied{Players: []appliedPlayer{}, Projectiles: []jsProj{}, Events: []jsEvent{}}
	for _, p := range out.Players {
		got.Players = append(got.Players, appliedPlayer{
			ID: p.Id, X: p.X, Y: p.Y, Aim: p.Aim, HP: p.Hp,
			Score: p.Score, Bot: p.Bot, Seq: p.Seq,
		})
	}
	for _, q := range out.Projectiles {
		got.Projectiles = append(got.Projectiles, jsProj{ID: q.Id, X: q.X, Y: q.Y})
	}
	for _, e := range out.Events {
		got.Events = append(got.Events, jsEvent{
			Tick: e.Tick, Kind: int32(e.Kind), Actor: e.Actor, Target: e.Target, HP: e.Hp,
		})
	}
	return got
}

type portCase struct {
	Name  string  `json:"name"`
	Base  *jsSnap `json:"base"`
	Delta *jsSnap `json:"delta"`
}

// runJS feeds the cases through web/pb.js under node.
func runJS(t *testing.T, cases []portCase) []applied {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed; skipping JS parity check")
	}
	script, err := filepath.Abs("../../../web/pb.js")
	if err != nil {
		t.Fatal(err)
	}
	// Read here, and the contents deliberately thrown away.
	//
	// go test caches a result against the files the test binary opened, and
	// node opening pb.js is invisible to it — so editing the port and re-running
	// the suite returned a cached pass. A guard whose whole job is to catch that
	// file drifting must not be able to go stale when it drifts. Opening it is
	// what puts it in the cache key.
	if _, err := os.ReadFile(script); err != nil {
		t.Fatalf("reading the port under test: %v", err)
	}
	in, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	// pb.js publishes onto `window`, which the browser has and node does not.
	// Shimming it here rather than changing the file keeps the parity harness
	// the thing that adapts.
	// The cases arrive on stdin, not in the script text. A whole match's worth
	// of them is a few hundred KB of JSON, and a single argv entry that size is
	// past Linux's MAX_ARG_STRLEN — `node -e` with them inlined fails with
	// "argument list too long" there while still fitting on macOS.
	js := fmt.Sprintf(`
globalThis.window = globalThis;
require(%q);
const cases = JSON.parse(require("fs").readFileSync(0, "utf8"));
const out = cases.map(c => {
  const r = globalThis.pbApplyDelta(c.base, c.delta);
  if (!r) return { refused: true, players: [], projectiles: [], events: [] };
  return {
    refused: false,
    players: r.players.map(p => ({
      id: p.id, x: p.x, y: p.y, aim: p.aim, hp: p.hp, score: p.score, bot: p.bot, seq: p.seq
    })),
    projectiles: r.projectiles.map(q => ({ id: q.id, x: q.x, y: q.y })),
    events: (r.events || []).map(e => ({
      tick: e.tick, kind: e.kind, actor: e.actor, target: e.target, hp: e.hp
    }))
  };
});
console.log(JSON.stringify(out));
`, script)

	cmd := exec.Command(node, "-e", js)
	cmd.Stdin = bytes.NewReader(in)
	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got []applied
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(raw))), &got); err != nil {
		t.Fatalf("decoding node output %q: %v", raw, err)
	}
	if len(got) != len(cases) {
		t.Fatalf("node returned %d results, want %d", len(got), len(cases))
	}
	return got
}

func compare(t *testing.T, names []string, want, got []applied) {
	t.Helper()
	for i, name := range names {
		w, g := want[i], got[i]
		if w.Refused != g.Refused {
			t.Errorf("%s: go refused=%v, js refused=%v", name, w.Refused, g.Refused)
			continue
		}
		if w.Refused {
			continue
		}
		if !reflect.DeepEqual(w.Players, g.Players) {
			t.Errorf("%s: players\n  go = %+v\n  js = %+v", name, w.Players, g.Players)
		}
		if !reflect.DeepEqual(w.Projectiles, g.Projectiles) {
			t.Errorf("%s: projectiles\n  go = %+v\n  js = %+v", name, w.Projectiles, g.Projectiles)
		}
		if !reflect.DeepEqual(w.Events, g.Events) {
			t.Errorf("%s: events\n  go = %+v\n  js = %+v", name, w.Events, g.Events)
		}
	}
}

// The malformed inputs the Go guard refuses must be refused by the browser too.
// A client that accepted what the server's own reference implementation throws
// away is the divergence this pair exists to prevent.
func TestWebApplyDeltaRefusesWhatGoRefuses(t *testing.T) {
	const shared = uint32(0x3f) // the field bits both implementations know
	player := func(id uint32) *pb.PlayerSnap { return &pb.PlayerSnap{Id: id, Changed: shared} }
	proj := func(id uint32) *pb.ProjSnap { return &pb.ProjSnap{Id: id} }

	type pair struct {
		name string
		base *pb.Snapshot
		d    *pb.Snapshot
	}
	pairs := []pair{{
		name: "full snapshot with a repeated player",
		d:    &pb.Snapshot{Tick: 4, Players: []*pb.PlayerSnap{player(0), player(0)}},
	}, {
		name: "full snapshot with players out of order",
		d:    &pb.Snapshot{Tick: 4, Players: []*pb.PlayerSnap{player(2), player(1)}},
	}, {
		name: "full snapshot with a repeated projectile",
		d:    &pb.Snapshot{Tick: 4, Projectiles: []*pb.ProjSnap{proj(7), proj(7)}},
	}, {
		name: "delta whose players are out of order",
		base: &pb.Snapshot{Tick: 3, Players: []*pb.PlayerSnap{player(1), player(2)}},
		d:    &pb.Snapshot{Tick: 4, BaselineTick: 3, Players: []*pb.PlayerSnap{player(2), player(1)}},
	}, {
		name: "delta whose removals are out of order",
		base: &pb.Snapshot{Tick: 3, Projectiles: []*pb.ProjSnap{proj(1), proj(2)}},
		d:    &pb.Snapshot{Tick: 4, BaselineTick: 3, RemovedProjectiles: []uint32{2, 1}},
	}, {
		name: "baseline with a repeated player",
		base: &pb.Snapshot{Tick: 3, Players: []*pb.PlayerSnap{player(1), player(1)}},
		d:    &pb.Snapshot{Tick: 4, BaselineTick: 3},
	}, {
		name: "baseline the client no longer holds",
		d:    &pb.Snapshot{Tick: 9, BaselineTick: 3, Players: []*pb.PlayerSnap{player(1)}},
	}, {
		// Well formed, and both sides must still take it.
		name: "an ordinary delta",
		base: &pb.Snapshot{Tick: 3, Players: []*pb.PlayerSnap{player(1), player(2)},
			Projectiles: []*pb.ProjSnap{proj(1), proj(4)}},
		d: &pb.Snapshot{Tick: 4, BaselineTick: 3,
			Players:            []*pb.PlayerSnap{{Id: 2, X: 11, Changed: fieldX}},
			Projectiles:        []*pb.ProjSnap{{Id: 9, X: 1}},
			RemovedProjectiles: []uint32{1}},
	}, {
		name: "delta whose events run backwards in time",
		base: &pb.Snapshot{Tick: 3, Players: []*pb.PlayerSnap{player(1)}},
		d: &pb.Snapshot{Tick: 9, BaselineTick: 3, Players: []*pb.PlayerSnap{player(1)},
			Events: []*pb.GameEvent{
				{Tick: 8, Kind: pb.GameEventKind_GAME_EVENT_KIND_KILL, Actor: 1, Target: 2},
				{Tick: 5, Kind: pb.GameEventKind_GAME_EVENT_KIND_HIT, Actor: 1, Target: 2, Hp: 50},
			}},
	}, {
		// Several on one tick is ordinary — a hit and the kill it caused share
		// one — so the check is non-decreasing rather than ascending, on both
		// sides.
		name: "delta carrying a hit and the kill it caused on one tick",
		base: &pb.Snapshot{Tick: 3, Players: []*pb.PlayerSnap{player(1), player(2)}},
		d: &pb.Snapshot{Tick: 6, BaselineTick: 3, Players: []*pb.PlayerSnap{player(1), player(2)},
			Events: []*pb.GameEvent{
				{Tick: 4, Kind: pb.GameEventKind_GAME_EVENT_KIND_HIT, Actor: 1, Target: 2, Hp: 25},
				{Tick: 5, Kind: pb.GameEventKind_GAME_EVENT_KIND_HIT, Actor: 1, Target: 2, Hp: 0},
				{Tick: 5, Kind: pb.GameEventKind_GAME_EVENT_KIND_KILL, Actor: 1, Target: 2},
				{Tick: 6, Kind: pb.GameEventKind_GAME_EVENT_KIND_DEPART, Target: 2},
			}},
	}, {
		name: "full snapshot carrying its own tick's events",
		d: &pb.Snapshot{Tick: 4, Players: []*pb.PlayerSnap{player(1)},
			Events: []*pb.GameEvent{
				{Tick: 4, Kind: pb.GameEventKind_GAME_EVENT_KIND_HIT, Actor: 1, Target: 2, Hp: -5},
			}},
	}, {
		// The case the encoder documents but never produces: a player in the
		// delta and not in the baseline. Both have to put it in id order, or
		// the client's own state stops being a valid baseline for the next one.
		name: "a player the delta introduces below the baseline's lowest id",
		base: &pb.Snapshot{Tick: 3, Players: []*pb.PlayerSnap{player(5), player(6)}},
		d: &pb.Snapshot{Tick: 4, BaselineTick: 3,
			Players: []*pb.PlayerSnap{player(2), {Id: 6, X: 1, Changed: fieldX}}},
	}}

	names := make([]string, len(pairs))
	cases := make([]portCase, len(pairs))
	want := make([]applied, len(pairs))
	for i, p := range pairs {
		names[i] = p.name
		cases[i] = portCase{Name: p.name, Base: toJS(p.base), Delta: toJS(p.d)}
		want[i] = goApplied(p.base, p.d)
	}
	compare(t, names, want, runJS(t, cases))
}

// And on traffic the server actually produces: a whole match's deltas, applied
// tick by tick by both implementations from the same baselines.
func TestWebApplyDeltaMatchesGoOverARealMatch(t *testing.T) {
	snaps := busyMatch(t, 8, 60)

	var names []string
	var cases []portCase
	var want []applied
	for i := 1; i < len(snaps); i++ {
		d := deltaSnapshot(snaps[i-1], snaps[i])
		names = append(names, fmt.Sprintf("tick %d", i))
		cases = append(cases, portCase{Base: toJS(snaps[i-1]), Delta: toJS(d)})
		want = append(want, goApplied(snaps[i-1], d))
	}
	// A full snapshot as well: that is what a client gets on signon and after a
	// stall, and it is the path the repeated-id bug came in through.
	names = append(names, "full signon snapshot")
	cases = append(cases, portCase{Delta: toJS(snaps[0])})
	want = append(want, goApplied(nil, snaps[0]))

	for i, w := range want {
		if w.Refused {
			t.Fatalf("%s: go refused a snapshot it produced itself", names[i])
		}
	}
	compare(t, names, want, runJS(t, cases))
}

// pbDecode against the bytes the server actually puts on the wire.
//
// The two tests above hand web/pb.js snapshots that are already decoded, which
// is the right shape for comparing the merge but leaves the decoder itself
// untested — and the decoder is where a field goes missing. PlayerSnap.seq did:
// it is field 9, the scan read 1 through 8, and the omission was invisible
// because every consumer of it wrote `mine.seq || 0` and got a number.
//
// What that cost is worth stating, because it is not a rendering glitch.
// Predictor.reconcile drops acknowledged inputs with
// `while (pending[0].seq <= ackSeq) pending.shift()`. With ackSeq stuck at 0 and
// client sequence numbers starting at 1, nothing was ever dropped: the pending
// queue grew for the whole match and every snapshot replayed every input the
// player had ever sent on top of the authoritative position.
func TestWebDecodeMatchesTheWire(t *testing.T) {
	player := func(id uint32, seq uint32, changed uint32) *pb.PlayerSnap {
		return &pb.PlayerSnap{
			Id: id, X: int32(id) * 1000, Y: -int32(id) * 7, Aim: int32(id) * 13,
			Hp: 100 - int32(id), Score: id * 2, Bot: id%2 == 0, Seq: seq, Changed: changed,
		}
	}

	snaps := []struct {
		name string
		s    *pb.Snapshot
	}{{
		name: "full snapshot",
		s: &pb.Snapshot{
			Tick: 41, RoomId: "r-wire",
			Players:     []*pb.PlayerSnap{player(1, 17, fieldAll), player(2, 9, fieldAll)},
			Projectiles: []*pb.ProjSnap{{Id: 3, X: -12, Y: 44}, {Id: 8, X: 5, Y: -6}},
		},
	}, {
		name: "delta carrying only seq",
		s: &pb.Snapshot{
			Tick: 42, BaselineTick: 41, RoomId: "r-wire",
			Players: []*pb.PlayerSnap{{Id: 1, Seq: 18, Changed: fieldSeq}},
		},
	}, {
		// seq 0 with the bit set: the case the mask exists for, and the one a
		// decoder that drops the field cannot be distinguished from.
		name: "delta setting seq back to zero",
		s: &pb.Snapshot{
			Tick: 43, BaselineTick: 42, RoomId: "r-wire",
			Players:            []*pb.PlayerSnap{{Id: 1, Seq: 0, Changed: fieldSeq | fieldHP}},
			RemovedProjectiles: []uint32{3, 8},
		},
	}, {
		// Events are field 9 of Snapshot, which is exactly the shape the seq
		// bug came in through: a repeated field past the ones the scan already
		// knew about, whose absence every consumer papers over with `|| 0`.
		//
		// Negative HP is deliberate. The field is sint32 and zigzagged, so a
		// decoder reading it as a plain varint gets a large positive number
		// rather than an error, and a hitmarker drawn from it looks fine.
		name: "delta carrying events",
		s: &pb.Snapshot{
			Tick: 44, BaselineTick: 43, RoomId: "r-wire",
			Players: []*pb.PlayerSnap{{Id: 1, Hp: 50, Changed: fieldHP}},
			Events: []*pb.GameEvent{
				{Tick: 43, Kind: pb.GameEventKind_GAME_EVENT_KIND_HIT, Actor: 2, Target: 1, Hp: 75},
				{Tick: 44, Kind: pb.GameEventKind_GAME_EVENT_KIND_HIT, Actor: 2, Target: 1, Hp: -3},
				{Tick: 44, Kind: pb.GameEventKind_GAME_EVENT_KIND_KILL, Actor: 2, Target: 1},
				{Tick: 44, Kind: pb.GameEventKind_GAME_EVENT_KIND_DEPART, Target: 9},
			},
		},
	}, {
		name: "final snapshot",
		s: &pb.Snapshot{
			Tick: 45, BaselineTick: 44, RoomId: "r-wire", Ended: true, Winner: 2,
			Players: []*pb.PlayerSnap{player(2, 31, fieldScore|fieldSeq)},
			Events:  []*pb.GameEvent{},
		},
	}}

	type wireCase struct {
		Name string `json:"name"`
		Hex  string `json:"hex"`
	}
	cases := make([]wireCase, len(snaps))
	want := make([]*jsSnap, len(snaps))
	for i, c := range snaps {
		cases[i] = wireCase{Name: c.name, Hex: hex.EncodeToString(protocol.Snapshot(c.s))}
		want[i] = toJS(c.s)
	}

	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed; skipping JS parity check")
	}
	script, err := filepath.Abs("../../../web/pb.js")
	if err != nil {
		t.Fatal(err)
	}
	// Read here, and the contents deliberately thrown away.
	//
	// go test caches a result against the files the test binary opened, and
	// node opening pb.js is invisible to it — so editing the port and re-running
	// the suite returned a cached pass. A guard whose whole job is to catch that
	// file drifting must not be able to go stale when it drifts. Opening it is
	// what puts it in the cache key.
	if _, err := os.ReadFile(script); err != nil {
		t.Fatalf("reading the port under test: %v", err)
	}
	in, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	js := fmt.Sprintf(`
globalThis.window = globalThis;
require(%q);
const cases = %s;
const out = cases.map(c => {
  const m = globalThis.pbDecode(Buffer.from(c.hex, "hex"));
  return {
    tick: m.tick, baseline_tick: m.baseline_tick, room_id: m.room_id,
    ended: !!m.ended, winner: m.winner,
    players: m.players, projectiles: m.projectiles,
    removed_projectiles: m.removed_projectiles,
    events: m.events
  };
});
console.log(JSON.stringify(out));
`, script, string(in))

	raw, err := exec.Command(node, "-e", js).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got []*jsSnap
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(raw))), &got); err != nil {
		t.Fatalf("decoding node output %q: %v", raw, err)
	}
	if len(got) != len(cases) {
		t.Fatalf("node returned %d results, want %d", len(got), len(cases))
	}
	for i, c := range cases {
		if !reflect.DeepEqual(want[i], got[i]) {
			t.Errorf("%s:\n  go = %+v\n  js = %+v", c.Name, dump(want[i]), dump(got[i]))
		}
	}
}

func dump(s *jsSnap) string {
	b, _ := json.Marshal(s)
	return string(b)
}
