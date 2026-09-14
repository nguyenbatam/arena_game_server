package room

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/metrics"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/safe"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

type Subscriber func(msg []byte)

// Recorder is handed the inputs applied on each tick, and the world's final
// checksum when the match ends. An interface rather than the concrete type so
// the room does not have to know whether anything is listening — and so a test
// can assert on what a room recorded without touching a disk.
type Recorder interface {
	Frame(tick uint32, inputs map[sim.PlayerID]sim.Input)
	// Depart and Rejoin record a change to who is in the match. They are not
	// inputs and no input implies them, so a recording without them replays a
	// different world than the one that was played.
	Depart(id sim.PlayerID)
	Rejoin(id sim.PlayerID)
	Close(finalTick uint32, checksum uint64) error
}

// subscriber pairs a send function with the last tick that client confirmed it
// applied. The ack is an atomic because it is written from network goroutines
// and read by the tick goroutine; it is not world state, so it never needs the
// room lock and never blocks the simulation.
type subscriber struct {
	send Subscriber
	ack  atomic.Uint32
	// since is the tick this subscriber was handed its signon snapshot at.
	// An ack older than that belongs to a previous connection and names a tick
	// whose state this client has already thrown away — honouring it would
	// wedge the pair: server keeps deltaing from a baseline the client cannot
	// reconstruct, client never acks anything newer. Written once before the
	// subscriber is published to the map, read-only after.
	since uint32
}

type Room struct {
	ID     string
	Seed   int64
	closed atomic.Bool

	tickDur    time.Duration
	budget     time.Duration
	matchTicks uint32

	inputs chan arrival
	// cmds carries roster changes — a player left, a player came back — from
	// the gateway's goroutine to this room's.
	//
	// A separate channel from inputs rather than a field on arrival: these are
	// rare, they must not be dropped when a flood of input fills the inbound
	// queue, and the input path is written to throw its oldest message away
	// under pressure. Losing a departure is what leaves a motionless avatar
	// paying score for the rest of the match.
	cmds chan roomCmd
	// inboxes hold input that has arrived but not been simulated yet, one queue
	// per player. Touched only by the tick goroutine — both the drain and the
	// per-tick fill run there — so they need no lock.
	inboxes map[sim.PlayerID]*inbox
	// inputBuffer is how deep those queues are allowed to sit after a tick has
	// taken its input. Fixed for the life of the room, like the tick rate.
	inputBuffer int

	world *sim.World

	// subs is published whole and replaced on every join and leave. Both hot
	// readers — the tick's fanout and the ack on every arriving input — then
	// take a pointer and use it without a lock.
	//
	// The lock it replaces was not contended in the sense a profile shows, but
	// it was taken 20 times a second per room for the fanout and once per
	// input per player on top of that: at 10k CCU that is a quarter of a
	// million acquisitions a second of a RWMutex whose contents change only
	// when somebody joins or leaves a match. subsMu serialises the writers,
	// which is where the cost belongs.
	subsMu sync.Mutex
	subs   atomic.Pointer[subSet]

	// ring holds the last SnapshotHistory encoded world states, indexed by
	// tick % SnapshotHistory. Written and read only by the tick goroutine, so
	// it needs no lock. Shared across the whole room: every client sees the
	// same world, only their baselines differ.
	ring [SnapshotHistory]*pb.Snapshot

	// byAck is broadcast's scratch space, reused across ticks. Touched only by
	// the tick goroutine. A fresh map per tick is an allocation per room per
	// tick — 20k/s across a thousand rooms — for working memory that is dead
	// again before broadcast returns.
	byAck map[uint32][]byte

	// events is the gathered span handed to one delta, reused across every
	// baseline in a fanout and every tick. Touched only by the tick goroutine,
	// like byAck and delta. The messages it points at are owned by the ring.
	events []*pb.GameEvent

	// delta is the scratch the per-baseline delta messages are built in,
	// reused across every subscriber and every tick. Touched only by the tick
	// goroutine, like byAck.
	delta deltaBuf

	// enc is the outbound envelope, reused across ticks the way delta and
	// byAck are, and touched only by the tick goroutine. See
	// protocol.SnapshotEncoder.
	enc protocol.SnapshotEncoder

	// lagShots, lagTicks and lagCapped accumulate this tick's lag compensation
	// so the process-wide counters are touched once a tick instead of once per
	// shot. Plain integers, not atomics: only the tick goroutine writes them,
	// and only it reads them. See metrics.LagCompShots.
	lagShots, lagTicks, lagCapped uint64

	// arrived counts inputs taken in since the last tick, so the process-wide
	// counter is touched once a tick instead of once a message.
	//
	// The counter it feeds is one cache line shared by every room in the
	// process, and at 10k CCU it was being incremented 200k times a second
	// from as many different goroutines. This one is per room and written by
	// that room's own players, which is a line nobody else is fighting for.
	arrived atomic.Uint64

	// lastTick is the newest tick that has actually been sent to clients, and
	// lastWorld the newest the simulation has reached. They are the same
	// number only when the room broadcasts every tick.
	//
	// The distinction matters in both directions. An ack names a tick the
	// client received, so it is checked against lastTick; the input path asks
	// "which tick is being built", which is a question about the simulation and
	// is answered by lastWorld. Answering either with the other is a class of
	// bug that only appears once the two rates differ, which is precisely when
	// nobody is looking for it.
	lastTick  atomic.Uint32
	lastWorld atomic.Uint32

	// sendEvery is how many simulated ticks pass between broadcasts. 1 is every
	// tick. See Params.SnapshotEvery.
	sendEvery uint32

	warmup      time.Duration
	expectSeats int

	onEnd func(snap sim.Snapshot)
	rec   Recorder
	stop  chan struct{}
	seats map[string]uint32
}

// MaxSnapshotEvents bounds how many gameplay events one message may carry.
//
// A delta owes every event since the client's last ack, so a client that has
// been quiet for most of the history window can be owed a lot of them — and a
// snapshot that outgrows udp.MaxDatagram is dropped by the UDP transport rather
// than fragmented, which costs the state as well as the events. The cap is what
// keeps the events from being able to do that.
//
// The number is measured, not guessed: TestSnapshotWithAFullEventBurstFitsOneDatagram
// pins a full room at MaxRoomSize carrying a full burst against the datagram
// ceiling, and fails if either side moves.
//
// Overflow keeps the newest and drops the oldest. A killfeed missing its
// oldest lines is a killfeed; a message that does not arrive is neither.
//
// Eight, because that is what the measurement allows. At config.MaxRoomSize a
// mid-match full snapshot is 932 B against udp.MaxDatagram's 1200, which leaves
// 268 B, and an event costs at most 28 of them — every field at its longest
// varint. The delta is the message that can actually carry a span, and it has
// more room (757 B, so 443 left), but one constant held to the tighter of the
// two is worth more than two constants and a rule about which applies where.
//
// The 28 B is a true ceiling rather than a realistic size: it assumes a tick
// number past four billion and player ids to match. Real events cost about a
// third of that, so the cap bites later in practice than the arithmetic
// suggests — and it only bites at all for a client that has been silent for
// most of the history window, at the largest room the server will form.
const MaxSnapshotEvents = 8

// DefaultInputBuffer is how many ticks of input a room holds in reserve for a
// player.
//
// Two is enough to absorb the ordinary case — one input arriving late and its
// successor arriving in the same window — without putting a player meaningfully
// behind the world. It is a ceiling, not a delay: in the steady state the queue
// is drained to empty every tick and nothing is held at all.
const DefaultInputBuffer = 2

// inputQueueCap bounds what one player can pile up between two ticks, whatever
// the per-connection rate limiter let through. The per-tick trim does the real
// work; this only stops the slice growing while the tick is still running.
const inputQueueCap = 8

// idleTicks is how long a player's inbox survives with nothing arriving in it.
//
// It exists so that a player who has left stops being counted as suffering an
// underrun on every tick for the rest of the match, and so the map does not
// keep an entry per player who ever connected. Three seconds at 20 Hz — far
// longer than any gap a live connection produces, far shorter than a match.
const idleTicks = 60

// maxInterpMs bounds the render delay a client may declare on its inputs.
//
// A client that draws other players 100 ms in the past aimed at a world 100 ms
// older than the tick it acked, and is owed that on top of the round trip — so
// the figure has to come from the client, because only the client knows how it
// renders. That makes it the one client-supplied number on this path, and
// therefore a dial: claim a large delay, keep drawing fresh, and collect
// compensation for a handicap you never took.
//
// The clamp is what makes the dial not worth turning. Source does the same
// thing with sv_client_max_interp_ratio, and for the same reason. 150 ms leaves
// the 100 ms the web client actually uses a margin for a slower buffer without
// approaching MaxLagCompTicks, which remains the ceiling on the total.
const maxInterpMs = 150

// roomCmd is a roster change on its way to the tick goroutine.
type roomCmd struct {
	playerID uint32
	rejoin   bool
}

// cmdQueue is how many roster changes may be in flight. A room holds at most
// config.MaxRoomSize players, so this is several times more than one tick could
// possibly produce even if everybody disconnected and reconnected at once.
const cmdQueue = 64

// inbox is one player's queue of arrived-but-not-yet-simulated input.
type inbox struct {
	q []queued
	// lastRecv is the tick at which something last arrived. An inbox that has
	// been silent since long before now belongs to somebody who is gone.
	lastRecv uint32
}

// arrival is one input as it crosses from a connection's goroutine to the tick's.
//
// A value, and flattened off the wire message by the sender rather than by the
// tick. The channel used to carry *pb.Input, which kept the whole pb.Envelope
// that input was unmarshalled from reachable for as long as the message sat in
// the queue: a 1024-deep channel per room is, at a thousand rooms, a million
// pointer slots the collector walks on every cycle, each one anchoring an
// object graph the tick copies six fields out of and drops. This struct holds
// no pointers, so the channel's backing array is memory the collector never has
// to look inside — and the conversion now happens on the connection's own
// goroutine, which is the one with work to spare.
type arrival struct {
	playerID uint32
	ackTick  uint32
	// interpMs is the client's declared render delay, already clamped. Held in
	// milliseconds rather than ticks because the conversion needs the room's
	// tick duration, which the sender's goroutine has no business reaching for.
	interpMs uint16
	in       sim.Input
}

// arrivalFrom flattens a wire input. LagTicks is deliberately not taken from
// here: it is derived server-side at the moment the input is simulated. See
// lagFor.
func arrivalFrom(in *pb.Input) arrival {
	// Clamped here, at the one place the wire is read, so nothing downstream
	// ever holds a figure a client chose. The narrowing to uint16 is safe only
	// because of the clamp above it, and is why they sit together.
	interp := in.InterpMs
	if interp > maxInterpMs {
		interp = maxInterpMs
	}
	return arrival{
		playerID: in.PlayerId,
		ackTick:  in.AckTick,
		interpMs: uint16(interp),
		in: sim.Input{
			MX: int8(in.Mx), MY: int8(in.My), Fire: in.Fire,
			Aim: int16(in.Aim), Seq: in.Seq,
		},
	}
}

// queued is one input plus the tick its sender had applied when they produced
// it. The ack is kept rather than turned into a lag figure on arrival, because
// the lag that matters is measured when the input is *simulated*: an input held
// for a tick was produced one tick further in the past than it looks.
type queued struct {
	in       sim.Input
	ackTick  uint32
	interpMs uint16
}

// subSet is the published subscriber list, immutable once stored.
//
// Both shapes are kept because both hot paths want a different one: recordAck
// needs a lookup by seat, and broadcast needs to walk everyone. Deriving one
// from the other per tick is exactly the copy this is here to avoid.
type subSet struct {
	byID map[uint32]*subscriber
	list []*subscriber
}

var emptySubs = &subSet{byID: map[uint32]*subscriber{}}

func (r *Room) currentSubs() *subSet {
	if s := r.subs.Load(); s != nil {
		return s
	}
	return emptySubs
}

// setSub publishes a new subscriber list with playerID set to s, or removed
// when s is nil. Copy-on-write, under subsMu so two writers cannot each build
// a successor from the same predecessor and lose one of the changes.
func (r *Room) setSub(playerID uint32, s *subscriber) {
	r.subsMu.Lock()
	defer r.subsMu.Unlock()
	cur := r.currentSubs()
	if s == nil {
		if _, ok := cur.byID[playerID]; !ok {
			return
		}
	}
	next := &subSet{
		byID: make(map[uint32]*subscriber, len(cur.byID)+1),
		list: make([]*subscriber, 0, len(cur.list)+1),
	}
	for id, existing := range cur.byID {
		if id == playerID {
			continue
		}
		next.byID[id] = existing
		next.list = append(next.list, existing)
	}
	if s != nil {
		next.byID[playerID] = s
		next.list = append(next.list, s)
	}
	r.subs.Store(next)
}

type Params struct {
	ID         string
	Seed       int64
	TickRate   int
	MatchTicks uint32
	Roster     []sim.Player
	Seats      map[string]uint32
	OnEnd      func(snap sim.Snapshot)
	// Recorder is optional: matches are recorded by sample, not all of them.
	Recorder Recorder
	// InputBuffer is the queue depth; zero takes DefaultInputBuffer.
	InputBuffer int
	// Warmup is how long the room will hold the match at tick zero waiting for
	// its players to connect. Zero starts immediately, which is what every test
	// that builds Params by hand wants.
	Warmup time.Duration
	// ExpectSeats is how many connections the match is waiting for. Bots are
	// not counted — they are never going to join.
	ExpectSeats int
	// SnapshotEvery is how many simulated ticks pass between broadcasts. Zero
	// or one sends every tick, which is what this room did before the field
	// existed and is still the default.
	//
	// Simulation rate and send rate are separate numbers in every engine that
	// has had to pay a bandwidth bill: Source exposes them as tick and
	// sv_updaterate, Overwatch simulates at 60 and sends at 20. They are
	// separate because raising the tick rate buys hit resolution and input
	// latency, while raising the send rate buys nothing but egress — which
	// grows with the square of the room size (see the table in the README).
	// Without the split, pushing tick_rate from 20 to 60 through
	// /admin/config tripled every room's outbound traffic as a side effect of
	// asking for a better simulation.
	SnapshotEvery int
}

func New(p Params) *Room {
	if p.TickRate <= 0 {
		p.TickRate = 20
	}
	if p.InputBuffer <= 0 {
		p.InputBuffer = DefaultInputBuffer
	}
	if p.InputBuffer > inputQueueCap {
		p.InputBuffer = inputQueueCap
	}
	if p.SnapshotEvery < 1 {
		p.SnapshotEvery = 1
	}
	// A send interval at or past the history window would leave every client
	// without a usable baseline — baselineFor refuses an ack SnapshotHistory
	// ticks old, so every broadcast would be a full snapshot and the delta
	// encoding would be dead weight. Clamping rather than refusing: the number
	// comes from configuration, and a room that will not start is worse than
	// one that sends a little more often than asked.
	if p.SnapshotEvery >= SnapshotHistory {
		p.SnapshotEvery = SnapshotHistory - 1
	}
	return &Room{
		ID:          p.ID,
		Seed:        p.Seed,
		tickDur:     time.Second / time.Duration(p.TickRate),
		budget:      time.Second / time.Duration(p.TickRate),
		matchTicks:  p.MatchTicks,
		inputs:      make(chan arrival, 1024),
		cmds:        make(chan roomCmd, cmdQueue),
		inboxes:     make(map[sim.PlayerID]*inbox, 16),
		inputBuffer: p.InputBuffer,
		sendEvery:   uint32(p.SnapshotEvery),
		warmup:      p.Warmup,
		expectSeats: p.ExpectSeats,
		world:       sim.NewWorld(p.Seed, p.TickRate, p.MatchTicks, p.Roster),
		byAck:       make(map[uint32][]byte, 4),
		onEnd:       p.OnEnd,
		rec:         p.Recorder,
		stop:        make(chan struct{}),
		seats:       p.Seats,
	}
}

// Run drives the room until the match ends, the context is cancelled or the
// room is stopped.
//
// The tick loop runs under a recover because the room is the unit of failure
// here as well as the unit of parallelism. One panic in gameplay code would
// otherwise take down every other match in the process — at 8 players a room
// and 10k CCU, about 1250 of them — for a bug that belongs to one.
func (r *Room) Run(ctx context.Context) {
	metrics.Rooms.Inc()
	defer metrics.Rooms.Dec()
	// Closed however the room ends — finished, stopped, cancelled or panicked.
	// A recording that is never closed is a file that never lands and a sample
	// slot that is never given back.
	defer r.closeRecorder()
	if safe.Do("room.tick", func() { r.loop(ctx) }) {
		r.abort()
	}
}

// abort ends a room whose simulation panicked. The world is in an unknown
// state, so there is nothing to resume: it takes the normal match-ended path so
// the room directory, the lifecycle event and the clients all see what they see
// at any other finish. The players reconnect and queue again.
func (r *Room) abort() {
	r.closed.Store(true)
	if r.onEnd == nil {
		return
	}
	safe.Do("room.end", func() {
		r.onEnd(sim.Snapshot{Tick: r.lastWorld.Load(), Ended: true})
	})
}

// closeRecorder finishes the recording from the room's own goroutine, which is
// the only one allowed to read the world.
func (r *Room) closeRecorder() {
	if r.rec == nil {
		return
	}
	rec := r.rec
	r.rec = nil
	safe.Do("room.replay", func() {
		if err := rec.Close(r.lastWorld.Load(), r.world.Checksum()); err != nil {
			log.Printf("replay %s: %v", r.ID, err)
		}
	})
}

func (r *Room) loop(ctx context.Context) {
	r.awaitWarmup(ctx)
	next := time.Now().Add(r.tickDur)
	pending := make(map[sim.PlayerID]sim.Input, 16)

	// One timer for the life of the room, not one per tick. time.NewTimer
	// allocates a timer and its channel, and at a thousand rooms and 20 Hz that
	// is twenty thousand of each a second thrown away immediately.
	//
	// Reset is only safe to use this way because the module is Go 1.23 or
	// later: since then a timer's channel is unbuffered and Stop and Reset
	// guarantee that no value queued by an earlier expiry can still be
	// received. Under the old semantics this would need the drain-then-reset
	// dance, and getting it wrong is a tick that returns instantly forever.
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		if r.closed.Load() {
			return
		}
		now := time.Now()
		if sleep := next.Sub(now); sleep > 0 {
			if timer == nil {
				timer = time.NewTimer(sleep)
			} else {
				timer.Reset(sleep)
			}
			r.drainUntil(ctx, timer.C)
			timer.Stop()
		}

		if r.closed.Load() || ctx.Err() != nil {
			r.closed.Store(true)
			return
		}

		start := time.Now()
		r.drainNonblock()
		r.fillPending(pending)
		// Advance, not Step: this value is encoded and dropped inside the tick,
		// so it does not need the world copied out for it. See sim.Advance for
		// the aliasing contract that makes this safe here and nowhere else.
		snap := r.world.Advance(pending)
		r.lastWorld.Store(snap.Tick)
		if r.rec != nil {
			// Recorded before the map is cleared, and before the broadcast, so
			// what lands in the file is exactly what the simulation was given.
			// Roster events applied during the drain above ride along with it.
			r.rec.Frame(snap.Tick, pending)
		}
		clear(pending)
		// Inputs are counted every tick even when nothing goes out, or the
		// counter would report the send rate rather than the arrival rate.
		r.flushInputs()
		if r.shouldSend(snap) {
			r.broadcast(snap)
		}

		elapsed := time.Since(start)
		metrics.TickDuration.Observe(elapsed.Seconds())
		if elapsed > r.budget {
			metrics.TickOverruns.Inc()
		}

		if snap.Ended {
			r.closed.Store(true)
			if r.onEnd != nil {
				// An owned copy here and only here: OnEnd records the result
				// from another goroutine, long after this one has gone.
				r.onEnd(r.world.Snapshot())
			}
			return
		}
		next = next.Add(r.tickDur)
		if time.Now().After(next.Add(r.tickDur)) {
			next = time.Now().Add(r.tickDur)
		}
	}
}

// awaitWarmup holds the match at tick zero until its players are actually
// connected, or the warmup budget runs out.
//
// Without it the match clock starts when the placement job is taken and the
// players are told about the match afterwards. In one process that costs
// milliseconds. Across nodes it is a MATCH_FOUND, a socket closed, a dial to
// another host, a HELLO and a JOIN_ROOM — seconds, all of it deducted from a
// match that is already running, and deducted unevenly, so whoever reconnects
// first gets a free head start on an empty map.
//
// Every competitive game has this state under one name or another: warmup,
// ready-up, the loading screen that waits for the last player. It is bounded
// rather than unconditional, because a seat whose player has already closed the
// tab must not hold the other seven forever.
//
// Input that arrives early is drained rather than left to fill the channel, and
// roster changes are applied — a player can perfectly well disconnect during
// the warmup they are holding up.
func (r *Room) awaitWarmup(ctx context.Context) {
	if r.warmup <= 0 || r.expectSeats <= 0 {
		return
	}
	started := time.Now()
	deadline := started.Add(r.warmup)
	poll := time.NewTicker(2 * time.Millisecond)
	defer poll.Stop()
	for {
		if len(r.currentSubs().list) >= r.expectSeats {
			metrics.MatchWarmup.Observe(time.Since(started).Seconds())
			return
		}
		if r.closed.Load() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case in := <-r.inputs:
			r.enqueue(in)
		case c := <-r.cmds:
			r.applyCmd(c)
		case <-poll.C:
			if time.Now().After(deadline) {
				// Started short-handed. The match runs anyway: the alternative
				// is holding up everybody who did turn up.
				metrics.MatchWarmup.Observe(time.Since(started).Seconds())
				metrics.WarmupTimeouts.Inc()
				log.Printf("room %s: started with %d of %d seats after %s of warmup",
					r.ID, len(r.currentSubs().list), r.expectSeats, r.warmup)
				r.departNoShows()
				return
			}
		}
	}
}

// departNoShows takes out the seats that did not turn up for the warmup.
//
// A seat that is still empty after the whole warmup budget belongs to somebody
// who is not coming — a handoff that failed, a tab closed on the queue screen —
// and leaving it standing is the same motionless target a disconnect leaves,
// for a player who was never here to disconnect. It also defeats the
// abandonment check: sim.abandoned() asks whether every human has *left*, and a
// player who never arrived has not, so a match where nobody at all turned up
// would run its full length for an empty room.
//
// They are departed rather than removed, so arriving late still works: joinRoom
// calls Rejoin, and they come back through the ordinary respawn.
//
// Only reachable from the timeout path. A warmup that ended because everyone
// joined has no no-shows by definition, and a room with no warmup configured
// never gets here — which matters, because there every seat is empty at tick
// zero.
func (r *Room) departNoShows() {
	if len(r.seats) == 0 {
		return
	}
	subs := r.currentSubs().byID
	for _, seat := range r.seats {
		if _, joined := subs[seat]; joined {
			continue
		}
		r.applyCmd(roomCmd{playerID: seat})
	}
}

func (r *Room) drainUntil(ctx context.Context, ch <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case in := <-r.inputs:
			r.enqueue(in)
		case c := <-r.cmds:
			r.applyCmd(c)
		case <-ch:
			return
		}
	}
}

func (r *Room) drainNonblock() {
	for {
		select {
		case in := <-r.inputs:
			r.enqueue(in)
		case c := <-r.cmds:
			r.applyCmd(c)
		default:
			return
		}
	}
}

// lagFor derives how far behind a client was from the tick it last
// acknowledged, and it is worth being exact about what that does and does not
// buy, because the comments here used to overstate it.
//
// It is computed here rather than taken as a latency field, which removes one
// dial — a client cannot hand the server a millisecond count and have it
// believed. What it does not remove is the underlying one: ack_tick itself
// comes off the wire. recordAck refuses an ack ahead of what this room has
// broadcast, but nothing can refuse an ack *behind* it, because that is exactly
// what a client losing packets legitimately sends. A client that reports an old
// tick while rendering a fresh one is claiming a worse connection than it has
// and collecting the catch-up for the difference.
//
// So the cap is the defence, not the derivation. MaxLagCompTicks bounds the
// whole claim at 10 ticks, and metrics.LagCompTicks is where an implausible
// claim becomes visible — statistical review rather than a rule, because the
// server genuinely cannot tell a liar from a bad link with this information
// alone. Telling them apart needs a latency the server measures itself, which
// means a server-initiated ping the client echoes: a protocol change, not a
// change to this function.
//
// Capped here and again in sim.spawnProjectile.
//
// applyTick is the tick the input is about to be simulated at, not the last one
// the room finished — see fillPending. The distinction is a whole tick, 50 ms
// at the default rate, and it used to be measured against the finished tick
// instead. That scored a client acking the freshest snapshot in existence as
// zero lag, even though the world still advances once more before its input
// lands, and it cost every client one tick of the compensation it was owed:
// against a typical three-to-six ticks of real lag, a fifth to a third of it.
//
// Undercompensating is the safe direction — it can only ever favour the target,
// never sell advantage to the shooter — which is why it went unnoticed. But
// safe is not the same as correct, and correct here is the world the shooter
// was looking at when they pulled the trigger. Every engine that rewinds for
// shots targets that, not one frame short of it.
// The two terms are the two halves of what Source writes as
//
//	Command Execution Time = Current Server Time - Packet Latency - Client View Interpolation
//
// applyTick-ackTick is the round trip as the client reports it: the tick the
// input is about to be simulated at, minus the last one the shooter says they
// had applied. applyTick, not the last tick the room finished — see
// fillPending. That distinction is a whole
// tick, 50 ms at the default rate, and this used to measure against the
// finished tick: a client acking the freshest snapshot in existence scored zero
// lag, even though the world advances once more before its input lands. It cost
// every client one tick of what it was owed, a fifth to a third of a typical
// three-to-six tick round trip, and it cost the best connections the most.
//
// interp is the other half, and the one that is easy to forget because it is
// not latency at all: a client that draws other players in the past aimed at a
// world older than the tick it acked, and owes nothing for the privilege.
//
// Both are undercompensations when missing, which is the safe direction — it
// can only favour the target, never sell advantage to the shooter — which is
// why neither was noticed. But safe is not correct, and correct is the world
// the shooter was looking at when they pulled the trigger.
//
// MaxLagCompTicks caps the sum, not the terms: it is the security bound on how
// far into the past any shot may reach, and splitting it per-term would let the
// two add up past it.
func lagFor(ackTick, applyTick uint32, interp uint8) uint8 {
	if ackTick == 0 || ackTick >= applyTick {
		// Nothing acked, or an ack of a tick that has not happened. Neither is
		// a client with a rendered world to compensate against, so the declared
		// interpolation buys nothing either.
		return 0
	}
	d := applyTick - ackTick + uint32(interp)
	if d > uint32(sim.MaxLagCompTicks) {
		return sim.MaxLagCompTicks
	}
	return uint8(d)
}

// interpTicks converts a clamped client render delay into ticks of this room's
// rate. Rounded to nearest rather than truncated: at 20 Hz the web client's
// 100 ms is exactly two ticks, but nothing guarantees a client picks a delay
// that divides the rate, and truncation would quietly drop most of a tick from
// the ones that do not.
func (r *Room) interpTicks(ms uint16) uint8 {
	if ms == 0 || r.tickDur <= 0 {
		return 0
	}
	t := (time.Duration(ms)*time.Millisecond + r.tickDur/2) / r.tickDur
	if t > time.Duration(sim.MaxLagCompTicks) {
		return sim.MaxLagCompTicks
	}
	return uint8(t)
}

// enqueue files an arrived input behind whatever that player already has
// waiting.
//
// It used to overwrite instead — one slot per player, last write wins — which
// quietly threw away a frame of movement every time two inputs landed inside
// one tick window. That is not an unusual event: a client sending at the tick
// rate is producing one input per tick, so the slightest jitter gives one
// window two and the next window none. The player then stands still for a tick
// they were moving through, the client's prediction is corrected backwards, and
// it reads as the network being bad when it was the server discarding input it
// had already received.
func (r *Room) enqueue(a arrival) {
	id := sim.PlayerID(a.playerID)
	e := r.inboxes[id]
	if e == nil {
		e = &inbox{q: make([]queued, 0, r.inputBuffer+1)}
		r.inboxes[id] = e
	}
	e.lastRecv = r.lastWorld.Load()
	if len(e.q) >= inputQueueCap {
		// Nothing a client sends should be able to grow this without bound
		// between two ticks. The oldest goes, because a client this far ahead
		// is better served current than complete.
		copy(e.q, e.q[1:])
		e.q = e.q[:len(e.q)-1]
		metrics.InputsDropped.Inc()
	}
	e.q = append(e.q, queued{in: a.in, ackTick: a.ackTick, interpMs: a.interpMs})
}

// fillPending takes one input per player out of the queues and into the map the
// simulation is about to be handed.
//
// One per tick, no more and no less. That is the whole jitter buffer: a window
// that received two inputs spends one and keeps one, and the window that
// receives none spends what was kept. In the steady state — one input per tick,
// which is what a client at the tick rate sends — the queue is emptied every
// time and nothing is held back, so this costs no latency when there is no
// jitter to absorb.
func (r *Room) fillPending(pending map[sim.PlayerID]sim.Input) {
	now := r.lastWorld.Load()
	// The tick whatever comes out of these queues is headed for. fillPending
	// runs before Advance, and Advance increments the world before it applies
	// anything, so an input lifted here is simulated at lastWorld+1 — never at
	// lastWorld, which is the tick the simulation has already finished. Idle
	// eviction below still measures against lastWorld: that is a question about
	// the past, not about the tick being built.
	//
	// lastWorld, not lastTick: this is the simulation's clock, and the two part
	// company as soon as the room sends less often than it ticks.
	applyTick := now + 1
	for id, e := range r.inboxes {
		if len(e.q) == 0 {
			// Silent for long enough that this is not a gap, it is a player who
			// is gone. Dropping the entry keeps the map bounded by who is
			// actually playing, and stops them being counted as underrunning on
			// every tick for the rest of the match.
			if now-e.lastRecv > idleTicks {
				delete(r.inboxes, id)
				continue
			}
			metrics.InputUnderruns.Inc()
			continue
		}
		// A client producing input faster than the server consumes it would
		// otherwise build a backlog that never drains, and every input it sends
		// would be simulated further and further in the past. Trimming to the
		// target keeps them at most that far behind: they lose the frames, but
		// they stay in the present, which is the trade a player would choose.
		if drop := len(e.q) - r.inputBuffer; drop > 0 {
			copy(e.q, e.q[drop:])
			e.q = e.q[:len(e.q)-drop]
			metrics.InputsDropped.Add(float64(drop))
		}
		next := e.q[0]
		copy(e.q, e.q[1:])
		e.q = e.q[:len(e.q)-1]

		in := next.in
		in.LagTicks = lagFor(next.ackTick, applyTick, r.interpTicks(next.interpMs))
		if in.Fire && in.LagTicks > 0 {
			// Accumulated here and reported once at the end of the tick. Only
			// shots count: compensation is only ever spent on a projectile, and
			// charging every movement frame would bury the signal as well as
			// the budget. See metrics.LagCompShots.
			r.lagShots++
			r.lagTicks += uint64(in.LagTicks)
			if in.LagTicks >= sim.MaxLagCompTicks {
				r.lagCapped++
			}
		}
		pending[id] = in
	}
}

func (r *Room) SubmitInput(in *pb.Input) {
	if r.closed.Load() || in == nil {
		return
	}
	r.recordAck(in.PlayerId, in.AckTick)
	r.arrived.Add(1)
	msg := arrivalFrom(in)
	select {
	case r.inputs <- msg:
	default:
		// The room's shared inbound channel is full, which the per-tick drain
		// makes a rare thing: it takes a burst arriving faster than the tick
		// loop can lift 1024 messages out. Newest wins, as everywhere else on
		// this path.
		//
		// Note what is dropped here, because it differs from the per-player
		// policy in enqueue: this channel is shared by the whole room, so the
		// message evicted to make space belongs to whoever happened to be at
		// the head — not necessarily to the client that overran. That is the
		// price of a single queue, and the counter is how the asymmetry becomes
		// visible rather than being inferred from players reporting lost input.
		select {
		case <-r.inputs:
			metrics.InputsDropped.Inc()
		default:
		}
		select {
		case r.inputs <- msg:
		default:
			// Drained and refilled by someone else in between. This input is
			// the one that goes.
			metrics.InputsDropped.Inc()
		}
	}
}

// recordAck moves a client baseline forward. Acks can arrive out of order or
// duplicated, so the baseline only ever advances — never let it slide back onto
// a tick the client has already moved past.
//
// The tick comes off the wire, so it is checked against one this room has
// actually broadcast. A client cannot have applied a tick that was never sent,
// and accepting a larger one was not merely meaningless — it was permanent and
// self-inflicted in the client's favour: baselineFor rejects any ack at or past
// the current tick, so a single `ack_tick: 4000000000` pinned that connection
// to a full snapshot every tick for the rest of the match, and the ack only
// ever moves forward so nothing could walk it back. Measured at four players
// with no projectiles in flight, a 12-byte delta became a 72-byte full
// snapshot, on every tick, for one field that costs the client nothing to send.
//
// Clamping rather than rejecting the connection: an ack this far ahead is
// either a hostile client or a broken one, and neither is worth a disconnect
// when the honest reading — "you cannot be ahead of me" — is a comparison.
func (r *Room) recordAck(playerID, tick uint32) {
	if tick == 0 || tick > r.lastTick.Load() {
		return
	}
	s := r.currentSubs().byID[playerID]
	if s == nil {
		return
	}
	if tick < s.since {
		return
	}
	for {
		cur := s.ack.Load()
		if tick <= cur {
			return
		}
		if s.ack.CompareAndSwap(cur, tick) {
			return
		}
	}
}

// Subscribe always starts the client at ack 0, so the first message it gets is
// a full snapshot. A reconnecting client may claim a last_ack_tick, but its
// snapshot history died with the old connection — this is the signon state.
//
// It sends nothing itself. It used to push the last broadcast's bytes from the
// caller's goroutine, and that was two things at once: a duplicate — the next
// broadcast owes an ack-0 subscriber a full snapshot anyway, so the client got
// the same state twice — and a reordering, because the tick goroutine could
// broadcast tick T+1 to this subscriber in the window between it being
// published into the map and the signon copy of tick T going out behind it.
// The client then applied an older world over a newer one and only recovered on
// its next ack.
//
// The cost of not sending here is that the first snapshot arrives on the next
// tick instead of at once: 50 ms at 20 Hz, against a match measured in minutes.
// What it buys is that every byte a client receives is produced in tick order
// by one goroutine, which is also what lets broadcast encode lazily — see
// there. A room that is already closed will never broadcast again, so the
// gateway does not seat anyone into one; see App.joinRoom.
func (r *Room) Subscribe(playerID uint32, sub Subscriber) {
	r.setSub(playerID, &subscriber{send: sub, since: r.lastTick.Load()})
}

// Leave takes a disconnected player out of the match.
//
// Unsubscribe stops sending to them; this stops the match paying them out. An
// avatar left standing is a motionless target worth full score to whoever
// shoots it, and that score is written straight into the ladder when the match
// ends — so "one disconnect does not kill the room" quietly meant "one
// disconnect hands everyone else a free kill every RespawnTicks". The
// simulation lingers before despawning, so this is not a way to dodge a shot
// already in flight. See sim.World.Depart.
//
// Safe to call from any goroutine and for a seat that is not in this room.
func (r *Room) Leave(playerID uint32) { r.command(roomCmd{playerID: playerID}) }

// Rejoin puts a player who reconnected back into the match. The counterpart to
// Leave, and idempotent in the same way.
func (r *Room) Rejoin(playerID uint32) { r.command(roomCmd{playerID: playerID, rejoin: true}) }

// command hands a roster change to the tick goroutine.
//
// Non-blocking, like everything else crossing this boundary: a gateway
// goroutine must never wait on a room. Unlike the input path it does not evict
// anything to make space — every message here is a distinct fact and there is
// no "newest wins" reading of them — so a full queue is counted and dropped,
// which at cmdQueue deep means the room has stopped ticking entirely.
func (r *Room) command(c roomCmd) {
	if r.closed.Load() {
		return
	}
	select {
	case r.cmds <- c:
	default:
		metrics.RosterEventsDropped.Inc()
	}
}

// applyCmd runs a roster change against the world. Tick goroutine only.
//
// The recorder is told in the same breath, and before the world advances: a
// departure is a change no input implies, so a recording that leaves it out
// re-simulates a different match and reports a desync that never happened.
func (r *Room) applyCmd(c roomCmd) {
	id := sim.PlayerID(c.playerID)
	if c.rejoin {
		r.world.Rejoin(id)
		if r.rec != nil {
			r.rec.Rejoin(id)
		}
		return
	}
	r.world.Depart(id)
	if r.rec != nil {
		r.rec.Depart(id)
	}
}

// shouldSend decides whether this tick goes on the wire.
//
// The final tick always does, whatever the interval: Ended is the one piece of
// state a client cannot wait for the next snapshot to learn, because there is
// no next snapshot. Everything else is a world that the following broadcast
// describes at least as well.
func (r *Room) shouldSend(snap sim.Snapshot) bool {
	if r.sendEvery <= 1 || snap.Ended {
		return true
	}
	return snap.Tick%r.sendEvery == 0
}

func (r *Room) Unsubscribe(playerID uint32) {
	r.setSub(playerID, nil)
}

func (r *Room) LastTick() uint32 { return r.lastTick.Load() }

func (r *Room) Closed() bool { return r.closed.Load() }

func (r *Room) BindSeat(connID string, claimed uint32) (uint32, bool) {
	if len(r.seats) == 0 {
		return claimed, claimed != 0
	}
	if id, ok := r.seats[connID]; ok {
		return id, true
	}
	return 0, false
}

func (r *Room) Stop() {
	if r.closed.CompareAndSwap(false, true) {
		close(r.stop)
	}
}

func (r *Room) broadcast(snap sim.Snapshot) {
	cur := r.ringSlot(snap.Tick)
	r.encodeInto(cur, snap)

	r.lastTick.Store(snap.Tick)
	metrics.Snapshots.Inc()

	// The full encode is built at most once per tick and only if somebody is
	// actually owed one — a subscriber that has just joined, or one whose ack
	// has aged out of the ring. In the steady state that is nobody: every
	// client acked the previous tick and is owed a delta.
	//
	// It used to be unconditional, because the bytes were also cached for
	// Subscribe to hand to the next arrival. Nobody reads that cache any more
	// (see Subscribe), and marshalling a whole world that is then discarded was
	// more than half the cost of the entire broadcast: measured at 8 players,
	// with everything else in this file unchanged, 1300 ns a tick became
	// 587 ns and 25 allocations became 22.
	var full []byte
	fullBytes := func() []byte {
		if full == nil {
			full = r.enc.Marshal(cur)
		}
		return full
	}

	// No lock and no copy: the list is immutable once published, so the tick
	// reads the pointer and walks it. See subSet.
	subs := r.currentSubs().list

	// A delta is a pure function of (baseline, current), and current is shared,
	// so two clients sitting on the same acked tick are owed byte-identical
	// messages. In the steady state that is every client in the room: they all
	// ack the previous tick. Encoding per client instead of per distinct
	// baseline makes the tick cost quadratic in room size for no benefit —
	// measurably so, since each encode also builds its own lookup maps.
	//
	// Clients only diverge here transiently, after one of them drops a packet,
	// and the map then holds exactly as many encodes as there are distinct
	// baselines. deltaSnapshot sorts its removal list precisely so this reuse
	// is sound.
	byAck := r.byAck
	clear(byAck)
	delta := &r.delta
	// Accumulated across the fanout and reported once, rather than four atomic
	// increments per subscriber per tick on counters every room in the process
	// shares. At 8 players and 10k CCU that was 800k read-modify-writes a
	// second over four cache lines, which is a cost that grows with the cores
	// available instead of shrinking: measured on an M4, the parallel
	// broadcast benchmark went from 647 ns/op to 464 ns/op at -cpu 10 with
	// this change alone, and did not move at all at -cpu 1.
	var nFull, nDelta, bFull, bDelta int
	for _, s := range subs {
		ack := s.ack.Load()
		base := r.baselineFor(ack, cur.Tick)
		if base == nil {
			msg := fullBytes()
			nFull++
			bFull += len(msg)
			s.send(msg)
			continue
		}
		msg, ok := byAck[ack]
		if !ok {
			// Built into the room's own scratch and marshalled at once: the
			// bytes are what gets cached and sent, so the message itself is
			// dead before the next baseline is encoded. See deltaBuf.
			msg = r.enc.Marshal(delta.encode(base, cur, r.eventsSince(ack, cur.Tick)))
			byAck[ack] = msg
		}
		nDelta++
		bDelta += len(msg)
		s.send(msg)
	}
	r.flushCounters(nFull, bFull, nDelta, bDelta)
}

// flushInputs reports the inputs taken in since the last tick.
//
// Separate from flushCounters because it is charged every tick while the
// snapshot counters are charged only when something is sent: folding it into
// the fanout made the inbound counter report the send rate.
func (r *Room) flushInputs() {
	if n := r.arrived.Swap(0); n > 0 {
		metrics.Inputs.Add(float64(n))
	}
	if r.lagShots > 0 {
		metrics.LagCompShots.Add(float64(r.lagShots))
		metrics.LagCompTicks.Add(float64(r.lagTicks))
		if r.lagCapped > 0 {
			metrics.LagCompCapped.Add(float64(r.lagCapped))
		}
		r.lagShots, r.lagTicks, r.lagCapped = 0, 0, 0
	}
}

// flushCounters reports one tick's fanout to Prometheus in one pass.
func (r *Room) flushCounters(nFull, bFull, nDelta, bDelta int) {
	if nFull > 0 {
		metrics.SnapshotsFull.Add(float64(nFull))
		metrics.SnapshotBytesFull.Add(float64(bFull))
	}
	if nDelta > 0 {
		metrics.SnapshotsDelta.Add(float64(nDelta))
		metrics.SnapshotBytesDelta.Add(float64(bDelta))
	}
}

// ringSlot is the entry this tick owns, recycled rather than replaced.
//
// The recycling is sound because of the guard in baselineFor, and only because
// of it. The slot a tick lands in last held the state from SnapshotHistory
// ticks ago, and baselineFor refuses any ack that old — `now-ack >=
// SnapshotHistory` — so the message being overwritten here is one no client can
// still be deltaed against. It is written before any delta is computed, which
// is safe for the same reason: every baseline a delta can legitimately use is
// in one of the other 63 slots.
//
// Shrink SnapshotHistory in one place and not the other and this turns into a
// client being deltaed against a world that has since been overwritten, which
// is the quietest possible desync. TestRingSlotIsOnlyRecycledOnceItIsUnusable
// holds the two together.
func (r *Room) ringSlot(tick uint32) *pb.Snapshot {
	slot := tick % SnapshotHistory
	if r.ring[slot] == nil {
		r.ring[slot] = &pb.Snapshot{}
	}
	return r.ring[slot]
}

// eventsSince gathers what happened between the tick a client acknowledged and
// the one it is about to be sent.
//
// This is what makes gameplay events reliable without a channel or an ack
// scheme of their own. The delta is already encoded against a tick the client
// confirmed it holds, so the same window that decides which state to resend
// decides which events it has not seen: losing a packet makes the next message
// carry more of both. A client that acks every tick is owed exactly the newest
// tick's events, which is the steady state.
//
// A tick missing from the ring — overwritten, or never broadcast because the
// room sends less often than it ticks — contributes nothing rather than
// aborting the walk. Its events are genuinely unrecoverable, and the state
// around them is not.
//
// The returned slice is the room's own scratch and is valid until the next
// call; broadcast marshals it before anything can move it.
func (r *Room) eventsSince(baseTick, curTick uint32) []*pb.GameEvent {
	out := r.events[:0]
	if baseTick >= curTick {
		r.events = out
		return out
	}
	for t := baseTick + 1; t <= curTick; t++ {
		s := r.ring[t%SnapshotHistory]
		if s == nil || s.Tick != t {
			continue
		}
		out = append(out, s.Events...)
	}
	if len(out) > MaxSnapshotEvents {
		// Keep the newest. See MaxSnapshotEvents.
		out = append(out[:0], out[len(out)-MaxSnapshotEvents:]...)
	}
	r.events = out
	return out
}

// baselineFor returns the acked snapshot to delta against, or nil when the
// client must be sent a full snapshot: it has acked nothing yet, or its ack has
// aged out of the ring.
func (r *Room) baselineFor(ack, now uint32) *pb.Snapshot {
	if ack == 0 || ack >= now || now-ack >= SnapshotHistory {
		return nil
	}
	s := r.ring[ack%SnapshotHistory]
	if s == nil || s.Tick != ack {
		return nil
	}
	return s
}

// encodeState is encodeInto with a message of its own. For callers that want to
// keep the result — a test comparing two ticks, anything outside the tick loop.
func (r *Room) encodeState(snap sim.Snapshot) *pb.Snapshot {
	out := &pb.Snapshot{}
	r.encodeInto(out, snap)
	return out
}

// encodeInto writes the world state into an existing message, reusing the
// per-player and per-projectile messages it already holds.
//
// This is the tick path's version, and the reuse is most of why it exists: the
// allocation profile of a room is dominated by this function and by the delta
// encode that follows it — measured at 8 players, 46% of every object the
// broadcast allocates was this one building a fresh Snapshot, a fresh slice and
// a fresh PlayerSnap per player, 20 times a second, per room. At a thousand
// rooms that is 200k messages a second whose only distinguishing feature is
// eight integers.
//
// What makes the reuse safe is stated at the call site rather than here: see
// ringSlot.
func (r *Room) encodeInto(out *pb.Snapshot, snap sim.Snapshot) {
	out.Tick = snap.Tick
	out.RoomId = r.ID
	out.Ended = snap.Ended
	out.Winner = uint32(snap.Winner)
	// A ring entry is only ever a baseline, never a delta, so these two stay
	// clear — but the message may have been one before it was recycled.
	out.BaselineTick = 0
	out.RemovedProjectiles = out.RemovedProjectiles[:0]

	// A ring entry carries its own tick's events, which is both what a full
	// snapshot owes a client with no baseline and the unit eventsSince walks.
	// Capped here as well: one tick cannot realistically produce this many, but
	// the ceiling belongs on every path that can put events on the wire.
	evs := snap.Events
	if len(evs) > MaxSnapshotEvents {
		evs = evs[len(evs)-MaxSnapshotEvents:]
	}
	out.Events = reuseEvents(out.Events, len(evs))
	for i, e := range evs {
		d := out.Events[i]
		d.Tick, d.Kind = e.Tick, eventKind(e.Kind)
		d.Actor, d.Target, d.Hp = uint32(e.Actor), uint32(e.Target), int32(e.HP)
	}

	out.Players = reusePlayers(out.Players, len(snap.Players))
	for i, p := range snap.Players {
		hp := p.HP
		if !p.Alive {
			hp = 0
		}
		d := out.Players[i]
		d.Id, d.X, d.Y = uint32(p.ID), int32(p.X), int32(p.Y)
		d.Aim, d.Hp, d.Score = int32(p.Aim), int32(hp), uint32(p.Score)
		d.Bot, d.Seq, d.Changed = p.Bot, p.Seq, 0
	}

	live := 0
	for _, p := range snap.Projectiles {
		if p.TTL != 0 {
			live++
		}
	}
	out.Projectiles = reuseProjs(out.Projectiles, live)
	i := 0
	for _, p := range snap.Projectiles {
		if p.TTL == 0 {
			continue
		}
		q := out.Projectiles[i]
		q.Id, q.X, q.Y = p.ID, int32(p.X), int32(p.Y)
		i++
	}
}

// reusePlayers and reuseProjs resize a slice of messages to n, keeping every
// message already in the backing array so it is overwritten rather than
// replaced.
//
// Deliberately not append. Append writes at len(dst), and in a slice that has
// been shrunk that slot still holds a perfectly good message from an earlier
// tick — so growing with append would allocate a replacement for an object
// that was already sitting there, which is exactly the cost being removed. The
// backing array is resliced to its capacity instead, and only a genuinely
// larger request allocates.
func reusePlayers(dst []*pb.PlayerSnap, n int) []*pb.PlayerSnap {
	if n > cap(dst) {
		grown := make([]*pb.PlayerSnap, n)
		copy(grown, dst[:cap(dst)])
		dst = grown
	}
	dst = dst[:n]
	for i := range dst {
		if dst[i] == nil {
			dst[i] = &pb.PlayerSnap{}
		}
	}
	return dst
}

// eventKind maps the simulation's kind onto the wire's. Written out rather than
// cast so that adding one to internal/sim without adding it here is a compile
// error rather than a zero on the wire.
func eventKind(k sim.EventKind) pb.GameEventKind {
	switch k {
	case sim.EventHit:
		return pb.GameEventKind_GAME_EVENT_KIND_HIT
	case sim.EventKill:
		return pb.GameEventKind_GAME_EVENT_KIND_KILL
	case sim.EventDepart:
		return pb.GameEventKind_GAME_EVENT_KIND_DEPART
	default:
		return pb.GameEventKind_GAME_EVENT_KIND_UNSPECIFIED
	}
}

func reuseEvents(dst []*pb.GameEvent, n int) []*pb.GameEvent {
	if n > cap(dst) {
		grown := make([]*pb.GameEvent, n)
		copy(grown, dst[:cap(dst)])
		dst = grown
	}
	dst = dst[:n]
	for i := range dst {
		if dst[i] == nil {
			dst[i] = &pb.GameEvent{}
		}
	}
	return dst
}

func reuseProjs(dst []*pb.ProjSnap, n int) []*pb.ProjSnap {
	if n > cap(dst) {
		grown := make([]*pb.ProjSnap, n)
		copy(grown, dst[:cap(dst)])
		dst = grown
	}
	dst = dst[:n]
	for i := range dst {
		if dst[i] == nil {
			dst[i] = &pb.ProjSnap{}
		}
	}
	return dst
}
