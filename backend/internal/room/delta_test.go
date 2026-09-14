package room

import (
	"sort"
	"sync"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
	"google.golang.org/protobuf/proto"
)

func snapAt(tick uint32, players []*pb.PlayerSnap, proj []*pb.ProjSnap) *pb.Snapshot {
	return &pb.Snapshot{Tick: tick, RoomId: "t", Players: players, Projectiles: proj}
}

// normalize sorts by id so a rebuilt snapshot can be compared to the original
// without depending on slice order.
func normalize(s *pb.Snapshot) *pb.Snapshot {
	out := cloneSnapshot(s)
	sort.Slice(out.Players, func(i, j int) bool { return out.Players[i].Id < out.Players[j].Id })
	sort.Slice(out.Projectiles, func(i, j int) bool { return out.Projectiles[i].Id < out.Projectiles[j].Id })
	return out
}

func assertRebuilds(t *testing.T, base, cur *pb.Snapshot) *pb.Snapshot {
	t.Helper()
	d := deltaSnapshot(base, cur)
	if d.BaselineTick != base.Tick {
		t.Fatalf("baseline_tick = %d, want %d", d.BaselineTick, base.Tick)
	}
	// Go through the wire, so anything proto3 elides as a zero value is
	// genuinely absent when the delta is applied.
	raw, err := proto.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var onWire pb.Snapshot
	if err := proto.Unmarshal(raw, &onWire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := ApplyDelta(base, &onWire)
	if got == nil {
		t.Fatal("ApplyDelta returned nil for a valid baseline")
	}
	if !proto.Equal(normalize(got), normalize(cur)) {
		t.Fatalf("rebuilt state mismatch\n got: %v\nwant: %v", normalize(got), normalize(cur))
	}
	return d
}

func TestDeltaRebuildsFullState(t *testing.T) {
	base := snapAt(100,
		[]*pb.PlayerSnap{
			{Id: 1, X: 10, Y: 20, Aim: 90, Hp: 100, Score: 0},
			{Id: 2, X: 30, Y: 40, Aim: 180, Hp: 75, Score: 3, Bot: true},
		},
		[]*pb.ProjSnap{{Id: 7, X: 5, Y: 5}, {Id: 8, X: 9, Y: 9}},
	)
	cur := snapAt(101,
		[]*pb.PlayerSnap{
			{Id: 1, X: 11, Y: 20, Aim: 90, Hp: 100, Score: 1},
			{Id: 2, X: 30, Y: 40, Aim: 180, Hp: 75, Score: 3, Bot: true},
		},
		[]*pb.ProjSnap{{Id: 8, X: 12, Y: 9}, {Id: 9, X: 1, Y: 1}},
	)

	d := assertRebuilds(t, base, cur)

	// Player 2 did not change at all and must not be on the wire.
	if len(d.Players) != 1 || d.Players[0].Id != 1 {
		t.Fatalf("expected only player 1 in the delta, got %v", d.Players)
	}
	// Player 1 moved in x and scored, but y/aim/hp did not change.
	if got := d.Players[0].Changed; got != fieldX|fieldScore {
		t.Fatalf("changed mask = %d, want %d", got, fieldX|fieldScore)
	}
	if len(d.RemovedProjectiles) != 1 || d.RemovedProjectiles[0] != 7 {
		t.Fatalf("removed_projectiles = %v, want [7]", d.RemovedProjectiles)
	}
}

// A field changing to zero is the case a naive "omit empty fields" encoder gets
// wrong: proto3 drops the zero from the wire, so only the mask can say whether
// it changed.
func TestDeltaCarriesChangeToZero(t *testing.T) {
	base := snapAt(10, []*pb.PlayerSnap{{Id: 1, X: 500, Y: 500, Hp: 100, Score: 4}}, nil)
	cur := snapAt(11, []*pb.PlayerSnap{{Id: 1, X: 0, Y: 500, Hp: 0, Score: 4}}, nil)

	d := assertRebuilds(t, base, cur)

	if got := d.Players[0].Changed; got != fieldX|fieldHP {
		t.Fatalf("changed mask = %d, want %d", got, fieldX|fieldHP)
	}
}

func TestDeltaIsSmallerThanFull(t *testing.T) {
	players := make([]*pb.PlayerSnap, 0, 8)
	for i := 1; i <= 8; i++ {
		players = append(players, &pb.PlayerSnap{
			Id: uint32(i), X: int32(i * 1000), Y: int32(i * 2000), Aim: int32(i * 7), Hp: 100,
		})
	}
	base := snapAt(200, players, nil)

	moved := cloneSnapshot(base)
	moved.Tick = 201
	moved.Players[0].X += 280 // one player took a step

	d := deltaSnapshot(base, moved)
	full, delta := protocol.Sizeof(moved), protocol.Sizeof(d)
	if delta >= full {
		t.Fatalf("delta (%dB) should be smaller than full (%dB)", delta, full)
	}
	t.Logf("full=%dB delta=%dB (%.0f%% of full)", full, delta, 100*float64(delta)/float64(full))
}

func TestApplyDeltaRejectsWrongBaseline(t *testing.T) {
	base := snapAt(100, []*pb.PlayerSnap{{Id: 1, X: 1}}, nil)
	cur := snapAt(101, []*pb.PlayerSnap{{Id: 1, X: 2}}, nil)
	d := deltaSnapshot(base, cur)

	stale := snapAt(99, []*pb.PlayerSnap{{Id: 1, X: 0}}, nil)
	if got := ApplyDelta(stale, d); got != nil {
		t.Fatal("applying a delta onto the wrong baseline must fail, not silently corrupt state")
	}
	if got := ApplyDelta(nil, d); got != nil {
		t.Fatal("applying a delta with no baseline must fail")
	}
}

func TestApplyDeltaAcceptsFullSnapshot(t *testing.T) {
	full := snapAt(5, []*pb.PlayerSnap{{Id: 1, X: 7, Hp: 90}}, []*pb.ProjSnap{{Id: 2, X: 3}})
	got := ApplyDelta(nil, full)
	if got == nil {
		t.Fatal("a full snapshot must apply with no baseline")
	}
	if !proto.Equal(normalize(got), normalize(full)) {
		t.Fatalf("full snapshot round-trip mismatch: %v vs %v", got, full)
	}
}

func TestBaselineWindow(t *testing.T) {
	r := &Room{}
	for tick := uint32(1); tick <= 80; tick++ {
		r.ring[tick%SnapshotHistory] = snapAt(tick, nil, nil)
	}
	now := uint32(80)

	if got := r.baselineFor(0, now); got != nil {
		t.Error("ack 0 means the client has applied nothing yet: must send full")
	}
	if got := r.baselineFor(now, now); got != nil {
		t.Error("ack equal to the current tick has no delta to express")
	}
	if got := r.baselineFor(10, now); got != nil {
		t.Error("ack older than the history window must fall back to full")
	}
	got := r.baselineFor(79, now)
	if got == nil || got.Tick != 79 {
		t.Fatalf("ack inside the window should give that exact tick, got %v", got)
	}
	// Tick 16 was overwritten by tick 80 (both map to slot 16); the guard must
	// notice the slot no longer holds the tick that was asked for.
	if got := r.baselineFor(16, now); got != nil {
		t.Errorf("stale ring slot must be rejected, got tick %d", got.Tick)
	}
}

func TestAckOnlyMovesForward(t *testing.T) {
	r := &Room{}
	// A room that has broadcast up to tick 80. recordAck refuses anything past
	// the tick actually sent, so the acks below have to name ticks this room
	// could really have produced — see TestAckAheadOfTheRoomIsIgnored.
	setTestClock(r, 80)
	s := &subscriber{send: func([]byte) {}, since: 50}
	r.setSub(1, s)

	r.recordAck(1, 49) // from a previous connection
	if got := s.ack.Load(); got != 0 {
		t.Fatalf("ack older than signon tick must be ignored, got %d", got)
	}
	r.recordAck(1, 60)
	r.recordAck(1, 55) // reordered duplicate
	if got := s.ack.Load(); got != 60 {
		t.Fatalf("ack = %d, want 60: a baseline must never slide backwards", got)
	}
	r.recordAck(1, 0) // unset field
	if got := s.ack.Load(); got != 60 {
		t.Fatalf("ack = %d, want 60", got)
	}
	r.recordAck(99, 70) // unknown subscriber
}

// End-to-end through the room: a fresh subscriber gets full snapshots, and the
// moment it acks a tick the server starts deltaing against it.
func TestRoomSendsDeltaOnceClientAcks(t *testing.T) {
	r, _, _ := startRoom(t, 2000)

	var mu sync.Mutex
	var got []*pb.Snapshot
	r.Subscribe(1, func(b []byte) {
		e, err := protocol.UnmarshalEnv(b)
		if err != nil {
			return
		}
		if s := e.GetSnapshot(); s != nil {
			mu.Lock()
			got = append(got, s)
			mu.Unlock()
		}
	})

	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			ok := cond()
			mu.Unlock()
			if ok {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}

	waitFor("first snapshots", func() bool { return len(got) >= 3 })

	mu.Lock()
	for _, s := range got {
		if s.BaselineTick != 0 {
			mu.Unlock()
			t.Fatalf("client that acked nothing must only get full snapshots, got baseline %d", s.BaselineTick)
		}
	}
	ack := got[len(got)-1].Tick
	got = nil
	mu.Unlock()

	r.SubmitInput(&pb.Input{PlayerId: 1, AckTick: ack})

	waitFor("a delta against the acked tick", func() bool {
		for _, s := range got {
			if s.BaselineTick == ack {
				return true
			}
		}
		return false
	})
}

// Honest bandwidth measurement: full vs delta encoded from the same tick of a
// real simulation, not full-at-spawn vs delta-at-midgame.
func TestDeltaBandwidthOverAMatch(t *testing.T) {
	roster := make([]sim.Player, 0, 8)
	for i := 1; i <= 8; i++ {
		roster = append(roster, sim.Player{ID: sim.PlayerID(i), Bot: i > 2})
	}
	r := New(Params{ID: "bw", Seed: 42, TickRate: 20, MatchTicks: 600, Roster: roster})

	pending := make(map[sim.PlayerID]sim.Input, 2)
	var prev *pb.Snapshot
	var fullBytes, deltaBytes, ticks int

	for tick := 0; tick < 600; tick++ {
		pending[1] = sim.Input{MX: int8(tick%3) - 1, MY: int8((tick/2)%3) - 1, Fire: tick%6 == 0, Aim: int16(tick % 360)}
		pending[2] = sim.Input{MX: 1, Fire: tick%9 == 0, Aim: int16((tick * 7) % 360)}
		snap := r.world.Step(pending)
		clear(pending)

		cur := r.encodeState(snap)
		if prev != nil {
			fullBytes += protocol.Sizeof(cur)
			deltaBytes += protocol.Sizeof(deltaSnapshot(prev, cur))
			ticks++
		}
		prev = cur
		if snap.Ended {
			break
		}
	}

	if ticks == 0 {
		t.Fatal("simulation produced no ticks")
	}
	ratio := 100 * float64(deltaBytes) / float64(fullBytes)
	t.Logf("%d ticks: full=%dB (%.0fB/tick) delta=%dB (%.0fB/tick) — delta is %.0f%% of full",
		ticks, fullBytes, float64(fullBytes)/float64(ticks),
		deltaBytes, float64(deltaBytes)/float64(ticks), ratio)

	if deltaBytes >= fullBytes {
		t.Fatalf("delta encoding did not reduce bytes: %dB vs %dB", deltaBytes, fullBytes)
	}
}
