package sim

import (
	"sort"
)

// Milli is a 0.001 world-unit fixed point. Integer math keeps simulation
// deterministic across CPU cores / machines (no float rounding drift).
type Milli int32

const (
	MapW Milli = 2000_000
	MapH Milli = 2000_000

	PlayerRadius     Milli  = 18_000
	ProjectileRadius Milli  = 6_000
	PlayerSpeed      Milli  = 280_000 // per second
	ProjectileSpeed  Milli  = 720_000
	MaxHP            int16  = 100
	Damage           int16  = 25
	FireCooldown     uint8  = 8
	RespawnTicks     uint8  = 40
	ProjectileTTL    uint16 = 30
	BotFireChance    uint32 = 12 // 1/N per tick

	// MaxLagCompTicks caps how far a shot may be compensated. Every rewind
	// mechanism needs a ceiling: without one, a client that stops acking buys
	// itself an unbounded advantage. 10 ticks = 500ms at 20 Hz.
	MaxLagCompTicks uint8 = 10

	// SpawnProtectTicks is how long a player is immune after respawning.
	//
	// Respawn points are a fixed ring, so without this the optimal strategy is
	// to stand on the point somebody is about to reappear on and shoot them the
	// instant they do — they are never alive long enough to react. Every
	// deathmatch ships some version of this; 10 ticks is 500 ms at 20 Hz, which
	// is about one human reaction time and well under the fire cooldown.
	//
	// It is cancelled the moment the protected player shoots, which is the
	// other half of the rule: protection is there to get out of the spawn, not
	// to take a free shot. Halo, Team Fortress and Quake's spawn rules all
	// break on the same event.
	SpawnProtectTicks uint8 = 10

	// DepartLingerTicks is how long a disconnected player's avatar stays in the
	// world before it despawns.
	//
	// It is not zero, because despawning instantly would make pulling the
	// network cable the cheapest dodge in the game. It is not unbounded either:
	// left standing for the rest of the match, the avatar is a motionless
	// target that pays full score to whoever shoots it, and that score is
	// written straight into the ladder. Three seconds is long enough that
	// quitting does not rescue you from a shot already in flight, short enough
	// that nobody farms it.
	DepartLingerTicks uint8 = 60
)

type PlayerID uint32

type Input struct {
	MX, MY int8
	Fire   bool
	Aim    int16
	Seq    uint32
	// LagTicks is how far behind the shooter was when this input was produced:
	// the round trip to the tick it acked, plus however far behind the newest
	// snapshot it draws other players. Derived server-side and clamped to
	// MaxLagCompTicks — never a tick count taken off the wire, which would hand
	// the client a dial to widen its own advantage. See room.lagFor.
	// It lives on Input so replaying the same inputs reproduces the same world.
	LagTicks uint8
}

type Player struct {
	ID       PlayerID
	X, Y     Milli
	Aim      int16
	HP       int16
	Score    uint16
	Cooldown uint8
	Respawn  uint8
	Alive    bool
	Bot      bool
	Seq      uint32

	// Protect counts down the immunity granted by a respawn. See
	// SpawnProtectTicks.
	Protect uint8

	// Left marks a player whose connection is gone. Their avatar lingers for
	// Linger ticks and is then despawned: no longer a target, no longer
	// respawned, and worth no score to anybody. Cleared if they reconnect.
	Left   bool
	Linger uint8
}

type Projectile struct {
	ID     uint32
	Owner  PlayerID
	X, Y   Milli
	VX, VY Milli
	TTL    uint16
}

// EventKind is something that happened on a tick, as opposed to something that
// is true at the end of it.
type EventKind uint8

const (
	EventHit EventKind = iota + 1
	EventKill
	EventDepart
)

// Event is one such fact.
//
// The simulation produces these because it is the only thing that knows them: a
// snapshot replicates state, and state cannot carry a discrete fact. Two hits
// inside one delta window are a single HP change, and a death followed by a
// respawn is no change at all — so a client watching PlayerSnap can see that HP
// fell, never that it fell twice or who did it. That is the difference between
// a health bar and a killfeed.
//
// They are an output of a tick, not part of the world: nothing downstream of
// this package reads them back, replaying the same inputs reproduces them, and
// they are deliberately absent from Checksum for that reason. What makes them
// deterministic is the same thing that makes the world deterministic — they are
// appended from passes that walk players in sorted order and projectiles in
// spawn order, never a map.
type Event struct {
	Tick   uint32
	Kind   EventKind
	Actor  PlayerID // who caused it; 0 when nobody did
	Target PlayerID
	HP     int16 // the target's HP afterwards
}

type Snapshot struct {
	Tick        uint32
	Players     []Player
	Projectiles []Projectile
	Ended       bool
	Winner      PlayerID
	// Events is what happened on this tick, in the order it happened. It
	// aliases the world under Advance, like Players and Projectiles.
	Events []Event
}

type World struct {
	TickRate    int
	MatchTicks  uint32
	Tick        uint32
	rng         RNG
	players     []Player
	projectiles []Projectile
	nextProj    uint32
	ended       bool
	winner      PlayerID
	// claimed is respawn's scratch: one bool per ring slot, reused every tick
	// rather than allocated, because respawn runs on the tick path.
	claimed []bool
	// events is this tick's output, truncated rather than reallocated at the
	// top of every step: the backing array settles at the busiest tick the
	// match has had and is never grown again.
	events    []Event
	speedTick Milli
	projTick  Milli
}

func NewWorld(seed int64, tickRate int, matchTicks uint32, roster []Player) *World {
	if tickRate <= 0 {
		tickRate = 20
	}
	ps := append([]Player(nil), roster...)
	sort.Slice(ps, func(i, j int) bool { return ps[i].ID < ps[j].ID })
	for i := range ps {
		if ps[i].HP == 0 {
			ps[i].HP = MaxHP
		}
		ps[i].Alive = true
		ps[i].X, ps[i].Y = spawnPoint(i, len(ps))
	}
	return &World{
		TickRate:   tickRate,
		MatchTicks: matchTicks,
		rng:        NewRNG(uint64(seed)),
		players:    ps,
		speedTick:  PlayerSpeed / Milli(tickRate),
		projTick:   ProjectileSpeed / Milli(tickRate),
		nextProj:   1,
	}
}

func spawnPoint(i, n int) (Milli, Milli) {
	// Even ring spawn — independent of map iteration order.
	cx, cy := MapW/2, MapH/2
	r := Milli(700_000)
	if n <= 1 {
		return cx, cy
	}
	angle := (360 * i) / n
	return cx + cosDeg(angle)*r/1000, cy + sinDeg(angle)*r/1000
}

// Step advances the world one tick and returns an owned copy of the result.
// The caller may keep it for as long as it likes; nothing the world does
// afterwards will change it.
func (w *World) Step(inputs map[PlayerID]Input) Snapshot {
	w.step(inputs)
	return w.snap()
}

// Advance is Step for a caller that reads the result and is finished with it
// before the next tick — which is the room's tick loop, and nothing else.
//
// The returned Snapshot *aliases* the world's own slices: Players and
// Projectiles are the live backing arrays, not copies, and the next Step or
// Advance overwrites them in place. That is the whole point — the copy Step
// makes is one allocation and a memcpy per tick per room, paid so a value can
// be kept that the tick path throws away before the tick is over.
//
// The rule for using it: read it, encode it, drop it, all before advancing
// again. Anything that outlives the tick — a value handed to another goroutine,
// a match result, a test comparing two ticks — must use Step or Snapshot
// instead. TestAdvanceAliasesTheWorldAndStepDoesNot pins the difference so this
// comment is not the only thing holding it.
func (w *World) Advance(inputs map[PlayerID]Input) Snapshot {
	w.step(inputs)
	return w.view()
}

// Snapshot is the current state as an owned copy, without advancing.
func (w *World) Snapshot() Snapshot { return w.snap() }

func (w *World) step(inputs map[PlayerID]Input) {
	if w.ended {
		return
	}
	w.Tick++
	// Truncated, not reallocated: see World.events. Anything the caller wanted
	// from last tick it has already copied — Step hands out an owned copy and
	// Advance documents that it does not.
	w.events = w.events[:0]

	w.timers()
	w.applyInputs(inputs)
	w.botThink(inputs)
	w.advanceProjectiles()
	w.respawn()

	if w.Tick >= w.MatchTicks || w.abandoned() {
		w.ended = true
		w.winner = w.leader()
	}
}

// abandoned reports that every player who could be watching has gone.
//
// A match whose humans have all left still has bots in it, and bots will happily
// play out the remaining minute and a half for an audience of nobody — on a
// node that is counting the room against MAX_ROOMS the whole time, and paying a
// tick loop for it. At a thousand rooms that is a real amount of a fleet spent
// on matches nobody is in.
//
// The test is despawned rather than merely departed, so the DepartLingerTicks
// grace applies here too: a player who drops and comes straight back finds the
// match still running. Past that there is nothing to come back to, and the
// result is recorded from the scores as they stand — leaving is not a way to
// avoid the rating.
//
// A roster with no human seats at all — which the matchmaker does not produce,
// but a test or a future mode might — is not abandoned. Nobody left it.
func (w *World) abandoned() bool {
	seats := 0
	for i := range w.players {
		p := &w.players[i]
		if p.Bot {
			continue
		}
		seats++
		if !p.Left || p.Alive {
			return false
		}
	}
	return seats > 0
}

func (w *World) applyInputs(inputs map[PlayerID]Input) {
	for i := range w.players {
		p := &w.players[i]
		if p.Cooldown > 0 {
			p.Cooldown--
		}
		in, ok := inputs[p.ID]
		if !ok || !p.Alive {
			continue
		}
		if in.Seq != 0 {
			if in.Seq <= p.Seq {
				continue
			}
			p.Seq = in.Seq
		}
		p.Aim = in.Aim
		dx, dy := clampDir(in.MX), clampDir(in.MY)
		if dx != 0 || dy != 0 {
			if dx != 0 && dy != 0 {
				p.X += w.speedTick * Milli(dx) * 707 / 1000
				p.Y += w.speedTick * Milli(dy) * 707 / 1000
			} else {
				p.X += w.speedTick * Milli(dx)
				p.Y += w.speedTick * Milli(dy)
			}
			p.X = clamp(p.X, PlayerRadius, MapW-PlayerRadius)
			p.Y = clamp(p.Y, PlayerRadius, MapH-PlayerRadius)
		}
		if in.Fire && p.Cooldown == 0 {
			// Taking a shot ends the immunity, before the shot is resolved so
			// there is no tick in which a player is both shooting and immune.
			p.Protect = 0
			w.spawnProjectile(p, in.LagTicks)
			p.Cooldown = FireCooldown
		}
	}
}

// timers advances the per-player countdowns that do not belong to any one
// subsystem, before anything this tick can read them.
//
// Spawn immunity and the departure linger both live here rather than beside the
// code that sets them, so that every one of them is charged exactly one tick per
// tick — a counter decremented inside a conditional path is a counter that runs
// at a different rate depending on what else happened.
func (w *World) timers() {
	for i := range w.players {
		p := &w.players[i]
		if p.Protect > 0 {
			p.Protect--
		}
		if !p.Left || p.Linger == 0 {
			// Not leaving, or long gone. The second half is what stops this
			// being a per-tick event: the branch below is reached on exactly
			// the tick the counter runs out, and a departed player sits at zero
			// for the rest of the match.
			//
			// It did not, and the end-to-end test is what found it — a single
			// disconnect put a DEPART on the wire every tick for the rest of
			// the match, which is a killfeed nobody can read and, worse, a
			// flood that pushed real hits out past MaxSnapshotEvents. The unit
			// test missed it by stopping at the first event it saw.
			continue
		}
		p.Linger--
		if p.Linger > 0 {
			continue
		}
		// The linger is spent: the avatar leaves the world. Alive is what every
		// other pass keys off — collision skips it, respawn is told to leave it
		// alone below — so this one flag is the whole despawn.
		p.Alive = false
		p.HP = 0
		p.Respawn = 0
		w.emit(Event{Kind: EventDepart, Target: p.ID})
	}
}

func (w *World) botThink(inputs map[PlayerID]Input) {
	for i := range w.players {
		p := &w.players[i]
		if !p.Bot || !p.Alive {
			continue
		}
		if _, human := inputs[p.ID]; human {
			continue
		}
		if w.rng.Uint32()%20 == 0 {
			p.Aim = int16(w.rng.Uint32() % 360)
		}
		dx, dy := cosDeg(int(p.Aim)), sinDeg(int(p.Aim))
		p.X += w.speedTick * dx / 1000
		p.Y += w.speedTick * dy / 1000
		p.X = clamp(p.X, PlayerRadius, MapW-PlayerRadius)
		p.Y = clamp(p.Y, PlayerRadius, MapH-PlayerRadius)
		if p.Cooldown == 0 && w.rng.Uint32()%BotFireChance == 0 {
			p.Protect = 0
			w.spawnProjectile(p, 0)
			p.Cooldown = FireCooldown
		}
	}
}

// spawnProjectile creates a shot and fast-forwards it by the shooter's lag.
//
// This is projectile catch-up, not hitbox rewind. Rewinding hitboxes is the
// right answer for hitscan weapons (CS2, Valorant): the server replays where
// targets were when the shooter pulled the trigger. It does not transfer to a
// weapon whose bullet travels for many ticks — there is no single instant to
// rewind to. So instead the bullet is advanced to where it would already be had
// it been fired when the client saw it, which is what Overwatch does for its
// projectile weapons.
//
// The catch-up is swept, not teleported: the shot is stepped one tick at a time
// and collides along the way. Jumping straight to the end would let a lagged
// shot pass through a target standing at the muzzle — the shooter would see a
// clean hit and the server would record a miss.
func (w *World) spawnProjectile(p *Player, lag uint8) {
	vx := w.projTick * cosDeg(int(p.Aim)) / 1000
	vy := w.projTick * sinDeg(int(p.Aim)) / 1000
	nx := p.X + cosDeg(int(p.Aim))*PlayerRadius/1000
	ny := p.Y + sinDeg(int(p.Aim))*PlayerRadius/1000

	if lag > MaxLagCompTicks {
		lag = MaxLagCompTicks
	}

	pr := Projectile{
		ID:    w.nextProj,
		Owner: p.ID,
		X:     nx,
		Y:     ny,
		VX:    vx,
		VY:    vy,
		TTL:   ProjectileTTL,
	}
	w.nextProj++

	for i := uint8(0); i < lag; i++ {
		fx, fy := pr.X, pr.Y
		pr.X += pr.VX
		pr.Y += pr.VY
		if pr.TTL > 0 {
			pr.TTL--
		}
		// Swept before the bounds check, not after: a target standing just
		// inside the edge of the map is still standing in the path, and the
		// old order threw the whole step away — hit included — the moment the
		// shot's end point crossed the line.
		if w.sweepHit(&pr, fx, fy) {
			return
		}
		if pr.TTL == 0 || pr.X < 0 || pr.Y < 0 || pr.X > MapW || pr.Y > MapH {
			// Spent before it caught up: nothing to add to the world.
			return
		}
	}

	w.projectiles = append(w.projectiles, pr)
}

// sweepHit resolves what a shot passed through on one step, and reports whether
// it was stopped.
//
// The test is the segment the projectile travelled, not the point it landed on,
// and that is the whole reason this function exists. At the default rate a
// projectile covers 36_000 units in a tick while the player and projectile radii
// add up to only 24_000 — the step is wider than the target — so sampling the
// end point alone lets a shot pass clean through somebody. Measured before this
// changed: of 432 shots fired straight at a target from inside the hit radius,
// 40 recorded a miss, some of them through the middle of the body.
//
// Discrete collision is fine only while the moving thing is slower than the
// thing it can hit is wide. Every engine that fires anything fast does this as a
// sweep — a raycast or a shapecast along the frame's motion — for exactly this
// reason, and it is why hit registration is described in terms of traces rather
// than positions.
//
// The nearest target along the path is taken, not the first one found in roster
// order: a shot that passes two players in one step has to stop at the one it
// reaches first, or the roster's ordering decides who gets hit.
func (w *World) sweepHit(pr *Projectile, fromX, fromY Milli) bool {
	reach := sq64(PlayerRadius + ProjectileRadius)
	var best *Player
	var bestAlong int64
	for j := range w.players {
		pl := &w.players[j]
		if !pl.Alive || pl.ID == pr.Owner {
			continue
		}
		if segDist2(pl.X, pl.Y, fromX, fromY, pr.X, pr.Y) > reach {
			continue
		}
		// How far down the segment this target sits, as the projection of the
		// target onto the direction of travel. Ordering by it is ordering by
		// what the shot reaches first.
		along := int64(pl.X-fromX)*int64(pr.X-fromX) + int64(pl.Y-fromY)*int64(pr.Y-fromY)
		if best == nil || along < bestAlong || (along == bestAlong && pl.ID < best.ID) {
			best, bestAlong = pl, along
		}
	}
	if best == nil {
		return false
	}
	return w.damage(best, pr.Owner)
}

// segDist2 is the squared distance from a point to the segment AB.
//
// Integer throughout, like everything else in this package: the closest point
// on the segment is found by projecting and then truncating to whole units
// before the distance is taken, which costs at most one unit of precision
// against a 24_000-unit hit radius and keeps the result identical on every
// machine. The projection is deliberately not carried into the distance
// algebraically — dot squared overflows int64 at map scale, which is the shape
// of bug that shows up as a desync months later rather than as a test failure.
func segDist2(px, py, ax, ay, bx, by Milli) int64 {
	dx := int64(bx - ax)
	dy := int64(by - ay)
	if dx == 0 && dy == 0 {
		return dist2(px, py, ax, ay)
	}
	wx := int64(px - ax)
	wy := int64(py - ay)
	dot := wx*dx + wy*dy
	if dot <= 0 {
		return dist2(px, py, ax, ay)
	}
	len2 := dx*dx + dy*dy
	if dot >= len2 {
		return dist2(px, py, bx, by)
	}
	cx := ax + Milli(dx*dot/len2)
	cy := ay + Milli(dy*dot/len2)
	return dist2(px, py, cx, cy)
}

// damage is the single place a hit is resolved, so the catch-up sweep and the
// per-tick collision pass can never drift apart on scoring or respawn rules.
//
// It reports whether the hit actually landed. A shot that meets a
// spawn-protected player is not absorbed by them — it carries on, because a
// bullet stopping dead on somebody who takes no damage from it is a shield, and
// a shield standing on the spawn point is worse than no protection at all.
func (w *World) damage(pl *Player, owner PlayerID) bool {
	if pl.Protect > 0 {
		return false
	}
	pl.HP -= Damage
	if pl.HP > 0 {
		w.emit(Event{Kind: EventHit, Actor: owner, Target: pl.ID, HP: pl.HP})
		return true
	}
	pl.HP = 0
	pl.Alive = false
	pl.Respawn = RespawnTicks
	if o := w.player(owner); o != nil {
		o.Score++
	}
	// Both, and in this order. The hit is what a shooter's crosshair reacts to
	// and the kill is what the feed reads; sending only the kill would leave
	// the shot that killed somebody as the one shot that produced no hitmarker.
	w.emit(Event{Kind: EventHit, Actor: owner, Target: pl.ID, HP: 0})
	w.emit(Event{Kind: EventKill, Actor: owner, Target: pl.ID})
	return true
}

// emit records something that happened this tick.
func (w *World) emit(e Event) {
	e.Tick = w.Tick
	w.events = append(w.events, e)
}

// advanceProjectiles moves every shot one tick and resolves what it crossed.
//
// Movement and collision are one pass rather than two because the collision
// test needs the step, not the destination: see sweepHit. Splitting them is
// what left the tick sampling positions a whole projectile-step apart.
func (w *World) advanceProjectiles() {
	dst := w.projectiles[:0]
	for i := range w.projectiles {
		pr := w.projectiles[i]
		fx, fy := pr.X, pr.Y
		pr.X += pr.VX
		pr.Y += pr.VY
		if pr.TTL > 0 {
			pr.TTL--
		}
		if pr.TTL != 0 && w.sweepHit(&pr, fx, fy) {
			// Spent on a target. Kept for this tick with no life left so the
			// snapshot drops it here rather than a tick later; the next pass
			// skips it on the same test the expiry below uses.
			pr.TTL = 0
		}
		if pr.TTL == 0 || pr.X < 0 || pr.Y < 0 || pr.X > MapW || pr.Y > MapH {
			continue
		}
		dst = append(dst, pr)
	}
	w.projectiles = dst
}

// respawn brings back everyone whose timer has run out, on the safest point the
// ring has free.
//
// Returning each player to the slot they opened on is what this used to do, and
// it is the one rule an opponent can exploit for free: the point is fixed for
// the whole match, the countdown is visible in the snapshot, and camping it
// costs nothing. Picking the point furthest from anybody alive is the
// "safest spawn" rule every arena shooter converges on, for the same reason.
//
// Deterministic, which is not automatic here: the slot is chosen by walking the
// ring in index order and the roster in its sorted order, never a map, and ties
// go to the lowest index. Two players coming back on the same tick take
// different slots because a slot is claimed as it is taken.
func (w *World) respawn() {
	n := len(w.players)
	if n == 0 {
		return
	}
	claimed := w.claimed[:0]
	for i := 0; i < n; i++ {
		claimed = append(claimed, false)
	}
	w.claimed = claimed

	for i := range w.players {
		p := &w.players[i]
		if p.Alive {
			continue
		}
		if p.Left {
			// Gone. Nothing counts down for a despawned avatar, and nothing
			// brings it back until its player reconnects — see Rejoin.
			continue
		}
		if p.Respawn > 0 {
			p.Respawn--
			continue
		}
		slot := w.safestSlot(p.ID, claimed)
		claimed[slot] = true
		p.Alive = true
		p.HP = MaxHP
		p.Protect = SpawnProtectTicks
		p.X, p.Y = spawnPoint(slot, n)
	}
}

// safestSlot is the free ring point furthest from the nearest living threat.
//
// "Nearest" rather than "average": what kills somebody on a spawn is the one
// player standing on it, and averaging lets a crowd on the far side of the map
// outvote them.
func (w *World) safestSlot(self PlayerID, claimed []bool) int {
	n := len(w.players)
	best, bestScore := -1, int64(-1)
	for slot := 0; slot < n; slot++ {
		if claimed[slot] {
			continue
		}
		sx, sy := spawnPoint(slot, n)
		nearest := int64(-1)
		for j := range w.players {
			pl := &w.players[j]
			if !pl.Alive || pl.ID == self {
				continue
			}
			d := dist2(sx, sy, pl.X, pl.Y)
			if nearest < 0 || d < nearest {
				nearest = d
			}
		}
		// An empty map leaves nearest at -1, which every slot shares, so the
		// tie-break below hands out slot 0, 1, 2 ... in order. That is the old
		// even ring, which is the right answer when there is nobody to avoid.
		if best < 0 || nearest > bestScore {
			best, bestScore = slot, nearest
		}
	}
	if best < 0 {
		// Every slot claimed, which cannot happen while there is one slot per
		// player — but a spawn with no point is worse than a crowded one.
		return 0
	}
	return best
}

// Depart takes a disconnected player's avatar out of play.
//
// Their connection is gone but the match is not, and doing nothing leaves a
// motionless body that pays full score to anybody who shoots it — score this
// server writes into the ladder. It lingers for DepartLingerTicks first, so
// disconnecting is not a way to dodge a shot already in the air.
//
// Called from the room's own goroutine, like every other mutation here.
func (w *World) Depart(id PlayerID) {
	p := w.player(id)
	if p == nil || p.Left {
		return
	}
	p.Left = true
	p.Linger = DepartLingerTicks
}

// Rejoin puts a player who came back inside the disconnect grace window back
// into the match. An avatar that has already despawned returns through the
// ordinary respawn path rather than reappearing where it stood.
func (w *World) Rejoin(id PlayerID) {
	p := w.player(id)
	if p == nil || !p.Left {
		return
	}
	p.Left = false
	p.Linger = 0
	if !p.Alive && p.Respawn == 0 {
		p.Respawn = RespawnTicks
	}
}

func (w *World) player(id PlayerID) *Player {
	for i := range w.players {
		if w.players[i].ID == id {
			return &w.players[i]
		}
	}
	return nil
}

// leader is who won, or 0 for a draw.
//
// The tie used to go to the lowest player id, which is not a tie-break so much
// as a coin permanently weighted towards seat one — and it disagreed with the
// ladder, where internal/rating scores equal results as a draw for both players.
// The scoreboard said one thing and the rating said another about the same
// match. Zero is the honest answer and is already a value no seat can hold.
func (w *World) leader() PlayerID {
	if len(w.players) == 0 {
		return 0
	}
	best := w.players[0]
	tied := false
	for i := 1; i < len(w.players); i++ {
		p := w.players[i]
		switch {
		case p.Score > best.Score:
			best, tied = p, false
		case p.Score == best.Score:
			tied = true
		}
	}
	if tied {
		return 0
	}
	return best.ID
}

func (w *World) snap() Snapshot {
	s := w.view()
	s.Players = append([]Player(nil), w.players...)
	s.Projectiles = append([]Projectile(nil), w.projectiles...)
	s.Events = append([]Event(nil), w.events...)
	return s
}

// view is the same fields without the copy. Callers hold the aliasing contract
// Advance documents.
func (w *World) view() Snapshot {
	return Snapshot{
		Tick:        w.Tick,
		Players:     w.players,
		Projectiles: w.projectiles,
		Ended:       w.ended,
		Winner:      w.winner,
		Events:      w.events,
	}
}

func (w *World) Players() []Player { return w.players }

func clampDir(v int8) int8 {
	if v < -1 {
		return -1
	}
	if v > 1 {
		return 1
	}
	return v
}

func clamp(v, lo, hi Milli) Milli {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func dist2(ax, ay, bx, by Milli) int64 {
	dx := int64(ax - bx)
	dy := int64(ay - by)
	return dx*dx + dy*dy
}

func sq64(v Milli) int64 {
	x := int64(v)
	return x * x
}

func sinDeg(d int) Milli {
	d = ((d % 360) + 360) % 360
	sign := Milli(1)
	if d > 180 {
		d -= 180
		sign = -1
	}
	x := Milli(d)
	// Bhaskara I: integer, exact at 0/90/180
	return sign * 1000 * (4 * x * (180 - x)) / (40500 - x*(180-x))
}

func cosDeg(d int) Milli { return sinDeg(d + 90) }

// Checksum fingerprints the whole world at the current tick.
//
// Determinism is claimed all over this package, and a checksum is how the claim
// gets checked rather than asserted: replay the same seed and the same inputs
// and you must land on the same number. That is what cmd/replay compares, and
// it is the same device lockstep games run every tick to catch a desync at the
// moment it happens instead of when the two views visibly disagree.
//
// FNV-1a over the fields that make up the state, in the order the simulation
// already keeps them — players sorted by id, projectiles in spawn order. Nothing
// here may be iterated from a map, or the number would differ between two runs
// of the same code.
func (w *World) Checksum() uint64 {
	const (
		offset uint64 = 1469598103934665603
		prime  uint64 = 1099511628211
	)
	h := offset
	mix := func(v uint64) {
		for i := 0; i < 8; i++ {
			h ^= v & 0xff
			h *= prime
			v >>= 8
		}
	}
	mix(uint64(w.Tick))
	for _, p := range w.players {
		mix(uint64(p.ID))
		mix(uint64(uint32(p.X)))
		mix(uint64(uint32(p.Y)))
		mix(uint64(uint16(p.Aim)))
		mix(uint64(uint16(p.HP)))
		mix(uint64(p.Score))
		mix(uint64(p.Seq))
		var flags uint64
		if p.Alive {
			flags |= 1
		}
		if p.Bot {
			flags |= 2
		}
		if p.Left {
			flags |= 4
		}
		mix(flags)
		mix(uint64(p.Cooldown))
		mix(uint64(p.Respawn))
		mix(uint64(p.Protect))
		mix(uint64(p.Linger))
	}
	for _, pr := range w.projectiles {
		mix(uint64(pr.ID))
		mix(uint64(pr.Owner))
		mix(uint64(uint32(pr.X)))
		mix(uint64(uint32(pr.Y)))
		mix(uint64(uint32(pr.VX)))
		mix(uint64(uint32(pr.VY)))
		mix(uint64(pr.TTL))
	}
	return h
}
