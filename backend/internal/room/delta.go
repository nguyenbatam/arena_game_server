package room

import (
	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
)

// SnapshotHistory is how many past ticks a room keeps so a client ack can still
// serve as a delta baseline. Quake 3 kept 32, Source 64. At 20 Hz, 64 ticks is
// 3.2s of tolerated client silence before that client falls back to a full
// snapshot — long enough to ride out a stall, short enough to bound memory.
const SnapshotHistory = 64

const (
	fieldX     = uint32(pb.PlayerField_PLAYER_FIELD_X)
	fieldY     = uint32(pb.PlayerField_PLAYER_FIELD_Y)
	fieldAim   = uint32(pb.PlayerField_PLAYER_FIELD_AIM)
	fieldHP    = uint32(pb.PlayerField_PLAYER_FIELD_HP)
	fieldScore = uint32(pb.PlayerField_PLAYER_FIELD_SCORE)
	fieldBot   = uint32(pb.PlayerField_PLAYER_FIELD_BOT)
	fieldSeq   = uint32(pb.PlayerField_PLAYER_FIELD_SEQ)

	fieldAll = fieldX | fieldY | fieldAim | fieldHP | fieldScore | fieldBot | fieldSeq
)

// deltaSnapshot encodes cur against base. Only fields that actually changed go
// on the wire; a player that did not move at all is omitted entirely.
//
// The baseline is always a tick the client acknowledged, never merely one the
// server sent. That is the whole trick: losing packets makes the next delta
// bigger, it can never desync the client.
//
// Both lists are walked as sorted merges rather than looked up through maps.
// That is worth the extra care because this is the hottest thing in the tick:
// it runs once per distinct client baseline per tick per room, and the two maps
// it used to build cost more than the comparison work they were serving —
// measured at 8 players, 389 ns became 213 ns.
//
// What makes the merge sound is an invariant that belongs to internal/sim and
// is asserted here by TestEncodeStateEmitsIdsInAscendingOrder: the world keeps
// its players sorted by id for the life of a match, and projectiles are
// appended with strictly increasing ids and compacted in place, so both lists
// are always ascending in both snapshots. If that ever stops being true the
// test fails rather than the encoding quietly going wrong.
func deltaSnapshot(base, cur *pb.Snapshot) *pb.Snapshot {
	var b deltaBuf
	return b.encode(base, cur, cur.Events)
}

// deltaBuf is the tick path's reusable delta message.
//
// A delta is marshalled the instant it is built and never outlives the tick, so
// one of these per room serves every distinct baseline in a fanout and every
// tick of the match. That matters because this was the second half of the
// broadcast's allocation bill: measured at 8 players, 35% of every object
// allocated was playerDelta minting a PlayerSnap per changed player, per
// distinct baseline, per tick.
//
// players is the free list those come from. out.Players is a slice of pointers
// into it rather than an owner, so resetting the message to zero length never
// drops a message on the floor.
type deltaBuf struct {
	snap    *pb.Snapshot
	players []*pb.PlayerSnap
	used    int
}

// player hands out the next scratch PlayerSnap, growing the free list when a
// tick needs more than any tick before it.
func (b *deltaBuf) player() *pb.PlayerSnap {
	if b.used == len(b.players) {
		b.players = append(b.players, &pb.PlayerSnap{})
	}
	p := b.players[b.used]
	b.used++
	return p
}

// encode is deltaSnapshot writing into b's own message. The result is only
// valid until the next call on the same buf — the caller marshals it and drops
// it, which is what broadcast does.
//
// events is what happened between the baseline and now, which the caller
// gathers from the ring because only it can: a delta spans every tick since the
// client's last ack, and cur holds only the newest one. The slice is borrowed,
// not copied, for the same reason the rest of this is scratch — it is
// marshalled before anything can overwrite it. See Room.eventsSince.
func (b *deltaBuf) encode(base, cur *pb.Snapshot, events []*pb.GameEvent) *pb.Snapshot {
	if b.snap == nil {
		b.snap = &pb.Snapshot{}
	}
	b.used = 0
	out := b.snap
	out.Tick = cur.Tick
	out.RoomId = cur.RoomId
	out.Ended = cur.Ended
	out.Winner = cur.Winner
	out.BaselineTick = base.Tick
	out.Players = out.Players[:0]
	out.Projectiles = out.Projectiles[:0]
	out.RemovedProjectiles = out.RemovedProjectiles[:0]
	// Assigned rather than appended: cur is a ring entry and carries its own
	// tick's events, which are a subset of what this delta owes and must not be
	// mistaken for the whole span.
	out.Events = events

	// A player present in cur and absent from base is sent in full; one that is
	// in base and not in cur is simply not mentioned, which is what the map
	// version did too — the roster does not change mid-match, so neither case
	// arises today and neither is worth a wire field.
	i := 0
	for _, p := range cur.Players {
		for i < len(base.Players) && base.Players[i].Id < p.Id {
			i++
		}
		var bp *pb.PlayerSnap
		if i < len(base.Players) && base.Players[i].Id == p.Id {
			bp = base.Players[i]
		}
		if d := b.player(); playerDelta(d, bp, p) {
			out.Players = append(out.Players, d)
		} else {
			// Nothing changed for this player, so the scratch message is
			// handed straight back rather than being skipped over — otherwise
			// a room where most players stand still would grow the free list
			// to the roster size and then past it, one entry per quiet tick.
			b.used--
		}
	}

	// Projectiles come and go every tick, so this half does the real merging:
	// an id in the baseline that cur has walked past is spent and is named as
	// removed. Ascending order means the removals are produced sorted, which is
	// the property the shared-encode reuse in Room.broadcast depends on — two
	// clients on the same baseline are owed byte-identical messages, and the
	// map version had to sort at the end to get there.
	j := 0
	for _, q := range cur.Projectiles {
		for j < len(base.Projectiles) && base.Projectiles[j].Id < q.Id {
			out.RemovedProjectiles = append(out.RemovedProjectiles, base.Projectiles[j].Id)
			j++
		}
		if j < len(base.Projectiles) && base.Projectiles[j].Id == q.Id {
			b := base.Projectiles[j]
			j++
			if b.X == q.X && b.Y == q.Y {
				continue
			}
		}
		out.Projectiles = append(out.Projectiles, q)
	}
	for ; j < len(base.Projectiles); j++ {
		out.RemovedProjectiles = append(out.RemovedProjectiles, base.Projectiles[j].Id)
	}
	return out
}

// playerDelta fills d with what changed between base and cur, and reports
// whether anything did. When it returns false d is left in an unspecified state
// and must not be sent.
//
// A set bit means "this field is authoritative in this message" — including
// when the new value is zero and proto3 therefore elides it from the wire.
// Without the mask a client cannot tell "moved to x=0" from "did not move".
//
// It writes into a message the caller supplies rather than minting one, so the
// tick path can hand it the same scratch every time. See deltaBuf.
func playerDelta(d, base, cur *pb.PlayerSnap) bool {
	if base == nil {
		d.Id, d.X, d.Y, d.Aim = cur.Id, cur.X, cur.Y, cur.Aim
		d.Hp, d.Score, d.Bot, d.Seq = cur.Hp, cur.Score, cur.Bot, cur.Seq
		d.Changed = fieldAll
		return true
	}
	// Every field is assigned, changed or not.
	//
	// The message is scratch and may still hold an earlier tick's values, and
	// proto3 puts any non-zero scalar on the wire whether or not its Changed
	// bit is set. A stale value left behind would not confuse a client — it
	// reads the mask — but it would put the bytes of a full snapshot inside a
	// delta, which is the entire thing this encoding exists to avoid.
	// TestReusedDeltaBufDoesNotLeakStaleFields is what holds this.
	d.Id = cur.Id
	d.X, d.Y, d.Aim, d.Hp, d.Score, d.Seq = 0, 0, 0, 0, 0, 0
	d.Bot = false
	var mask uint32
	if cur.X != base.X {
		mask |= fieldX
		d.X = cur.X
	}
	if cur.Y != base.Y {
		mask |= fieldY
		d.Y = cur.Y
	}
	if cur.Aim != base.Aim {
		mask |= fieldAim
		d.Aim = cur.Aim
	}
	if cur.Hp != base.Hp {
		mask |= fieldHP
		d.Hp = cur.Hp
	}
	if cur.Score != base.Score {
		mask |= fieldScore
		d.Score = cur.Score
	}
	if cur.Bot != base.Bot {
		mask |= fieldBot
		d.Bot = cur.Bot
	}
	if cur.Seq != base.Seq {
		mask |= fieldSeq
		d.Seq = cur.Seq
	}
	if mask == 0 {
		return false
	}
	d.Changed = mask
	return true
}

// ApplyDelta rebuilds the complete state at d.Tick from the baseline d was
// encoded against. It is the reference client implementation — web/pb.js does
// the same thing in JavaScript, and cmd/loadtest uses this one directly.
//
// It returns nil when the delta cannot be applied, which is the client's signal
// to keep acking its last good tick and wait: the server will keep encoding
// from that older baseline until an ack moves it forward.
func ApplyDelta(base, d *pb.Snapshot) *pb.Snapshot {
	if d == nil {
		return nil
	}
	// Both halves have to be ordered before either is used. See wellOrdered:
	// this is the invariant every walk below is written against, and it is not
	// something a client can take on trust from the wire.
	if !wellOrdered(d) {
		return nil
	}
	if d.BaselineTick == 0 {
		return cloneSnapshot(d)
	}
	if base == nil || base.Tick != d.BaselineTick {
		return nil
	}
	if !wellOrdered(base) {
		return nil
	}

	out := &pb.Snapshot{
		Tick:   d.Tick,
		RoomId: d.RoomId,
		Ended:  d.Ended,
		Winner: d.Winner,
		// Carried through rather than merged. Events are not state: they
		// describe the span between the baseline and this tick, and the
		// baseline's own events belong to a span the client has already seen.
		// A rebuilt snapshot used as the next baseline therefore carries
		// events that nothing reads, which is why nothing here merges them.
		Events: cloneEvents(d.Events),
	}

	// Sorted merges, for the reason deltaSnapshot gives for encoding the same
	// way: every list involved is ascending by id, so the maps this used to
	// build were paying for a lookup the ordering already answers. It matters
	// more here than it looks, because this is not only the reference client —
	// cmd/loadtest runs it once per snapshot per bot, so at ten thousand bots
	// the map building was burning the load generator's own CPU and bending the
	// numbers it exists to measure.
	//
	// The invariant is the same one TestEncodeStateEmitsIdsInAscendingOrder
	// pins for the encoder, and it holds for a delta too: d.Players and
	// d.Projectiles are subsequences of an ascending list, and
	// d.RemovedProjectiles is produced by walking the baseline in order.
	i, j := 0, 0
	for i < len(base.Players) || j < len(d.Players) {
		switch {
		case j >= len(d.Players) || (i < len(base.Players) && base.Players[i].Id < d.Players[j].Id):
			// In the baseline and not mentioned in the delta: unchanged.
			out.Players = append(out.Players, applyPlayer(base.Players[i], nil))
			i++
		case i >= len(base.Players) || d.Players[j].Id < base.Players[i].Id:
			// In the delta and not in the baseline: sent in full. The roster
			// does not change mid-match, so this is the encoder's documented
			// but currently unreachable case.
			out.Players = append(out.Players, applyPlayer(nil, d.Players[j]))
			j++
		default:
			out.Players = append(out.Players, applyPlayer(base.Players[i], d.Players[j]))
			i++
			j++
		}
	}

	// Projectiles are a three-way merge: the baseline, the ids the delta says
	// are spent, and the ids it carries new values for.
	bi, pi, ri := 0, 0, 0
	for bi < len(base.Projectiles) {
		b := base.Projectiles[bi]
		// Anything the delta carries below this baseline id is a projectile
		// that did not exist at the baseline. Emitting it here rather than at
		// the end keeps the output ascending.
		for pi < len(d.Projectiles) && d.Projectiles[pi].Id < b.Id {
			out.Projectiles = append(out.Projectiles, cloneProj(d.Projectiles[pi]))
			pi++
		}
		for ri < len(d.RemovedProjectiles) && d.RemovedProjectiles[ri] < b.Id {
			ri++
		}
		if ri < len(d.RemovedProjectiles) && d.RemovedProjectiles[ri] == b.Id {
			ri++
			bi++
			continue
		}
		if pi < len(d.Projectiles) && d.Projectiles[pi].Id == b.Id {
			out.Projectiles = append(out.Projectiles, cloneProj(d.Projectiles[pi]))
			pi++
		} else {
			out.Projectiles = append(out.Projectiles, cloneProj(b))
		}
		bi++
	}
	for ; pi < len(d.Projectiles); pi++ {
		out.Projectiles = append(out.Projectiles, cloneProj(d.Projectiles[pi]))
	}
	return out
}

// wellOrdered reports whether every id list in a snapshot is strictly
// ascending, which is what the merges above are written against.
//
// The server cannot send anything else: internal/sim keeps players sorted by id
// for the life of a match, projectiles are appended with strictly increasing
// ids and compacted in place, and deltaSnapshot produces its removal list by
// walking the baseline in that order. TestEncodeStateEmitsIdsInAscendingOrder
// holds the encoder to it.
//
// The client is a different matter, because what reaches it is bytes. A
// snapshot carrying the same id twice used to walk straight through — cloneSnapshot
// copies a full snapshot verbatim, and the merge emits a baseline entry once per
// occurrence — and the client was left holding a state with one player in it
// twice, which makes its own lookups ambiguous and its prediction reconciliation
// pick whichever copy it happened to find. FuzzApplyDelta found it; nothing
// else would have, because no server produces it.
//
// Refusing is the honest answer rather than repairing the message: a snapshot
// this malformed is corrupt or hostile, and nil already means "cannot be
// applied — keep acking the last good tick and wait", which is a path the
// client and the load bot both handle.
func wellOrdered(s *pb.Snapshot) bool {
	for i := 1; i < len(s.Players); i++ {
		if s.Players[i].Id <= s.Players[i-1].Id {
			return false
		}
	}
	for i := 1; i < len(s.Projectiles); i++ {
		if s.Projectiles[i].Id <= s.Projectiles[i-1].Id {
			return false
		}
	}
	for i := 1; i < len(s.RemovedProjectiles); i++ {
		if s.RemovedProjectiles[i] <= s.RemovedProjectiles[i-1] {
			return false
		}
	}
	// Events are non-decreasing rather than strictly ascending: a tick can
	// produce several, and a hit and the kill it caused share one. What a
	// client cannot be handed is a feed that goes backwards — it renders these
	// in order, and an out-of-order pair puts a kill above the shot that made
	// it.
	for i := 1; i < len(s.Events); i++ {
		if s.Events[i] == nil || s.Events[i-1] == nil || s.Events[i].Tick < s.Events[i-1].Tick {
			return false
		}
	}
	return true
}

func cloneEvents(evs []*pb.GameEvent) []*pb.GameEvent {
	if len(evs) == 0 {
		return nil
	}
	out := make([]*pb.GameEvent, 0, len(evs))
	for _, e := range evs {
		out = append(out, &pb.GameEvent{
			Tick: e.Tick, Kind: e.Kind, Actor: e.Actor, Target: e.Target, Hp: e.Hp,
		})
	}
	return out
}

func cloneProj(q *pb.ProjSnap) *pb.ProjSnap {
	return &pb.ProjSnap{Id: q.Id, X: q.X, Y: q.Y}
}

func applyPlayer(base, d *pb.PlayerSnap) *pb.PlayerSnap {
	if d == nil {
		return &pb.PlayerSnap{
			Id: base.Id, X: base.X, Y: base.Y, Aim: base.Aim,
			Hp: base.Hp, Score: base.Score, Bot: base.Bot, Seq: base.Seq,
		}
	}
	out := &pb.PlayerSnap{Id: d.Id}
	if base != nil {
		out.X, out.Y, out.Aim = base.X, base.Y, base.Aim
		out.Hp, out.Score, out.Bot, out.Seq = base.Hp, base.Score, base.Bot, base.Seq
	}
	if d.Changed&fieldX != 0 {
		out.X = d.X
	}
	if d.Changed&fieldY != 0 {
		out.Y = d.Y
	}
	if d.Changed&fieldAim != 0 {
		out.Aim = d.Aim
	}
	if d.Changed&fieldHP != 0 {
		out.Hp = d.Hp
	}
	if d.Changed&fieldScore != 0 {
		out.Score = d.Score
	}
	if d.Changed&fieldBot != 0 {
		out.Bot = d.Bot
	}
	if d.Changed&fieldSeq != 0 {
		out.Seq = d.Seq
	}
	return out
}

func cloneSnapshot(s *pb.Snapshot) *pb.Snapshot {
	out := &pb.Snapshot{
		Tick:   s.Tick,
		RoomId: s.RoomId,
		Ended:  s.Ended,
		Winner: s.Winner,
	}
	for _, p := range s.Players {
		out.Players = append(out.Players, &pb.PlayerSnap{
			Id: p.Id, X: p.X, Y: p.Y, Aim: p.Aim,
			Hp: p.Hp, Score: p.Score, Bot: p.Bot, Seq: p.Seq,
		})
	}
	for _, q := range s.Projectiles {
		out.Projectiles = append(out.Projectiles, &pb.ProjSnap{Id: q.Id, X: q.X, Y: q.Y})
	}
	out.Events = cloneEvents(s.Events)
	return out
}
