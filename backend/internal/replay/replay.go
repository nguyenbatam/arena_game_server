// Package replay records a match as the only two things needed to play it back:
// the seed it started from and the inputs that went into it.
//
// The simulation is already deterministic — fixed-point arithmetic, a seeded
// RNG, players sorted by id — and a deterministic simulation gets replays for
// almost nothing. There is no need to store positions, hits or scores: given
// the same seed and the same inputs the server reproduces all of them exactly,
// which is also what makes the recording worth having. A replay that merely
// showed what the server said happened could not be used to check the server.
//
// What they are for, in the order they get used:
//
//   - A bug report with a repro. "It desynced at tick 900" is a file, not a
//     story, and cmd/replay runs it.
//   - Cheat review. A suspicious match can be re-simulated from the inputs the
//     server actually accepted, and the checksum says whether the world it
//     produced is the world that was played.
//   - The thing every game eventually wants: spectating, highlights, esports.
//
// A match is around 200 KB at 20 Hz for 90 seconds, which at a thousand
// concurrent rooms is more memory than the simulation itself uses. So recording
// is sampled: a bounded number of matches record at once and the rest run
// untouched. Sampling is how this is done in practice — nobody keeps every
// match of a live game either.
package replay

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/nguyenbatam/arena_game_server/internal/metrics"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

// Magic and Format identify the file. A version byte costs nothing now and is
// the difference between "old replays stop loading" and "old replays load" —
// and v2 is the first time that mattered.
//
// v2 adds roster events to each frame. A match is no longer reproducible from
// its inputs alone: a player who disconnects has their avatar taken out of the
// world, which is a change to the simulation that no input carries. Without it
// recorded, every replay of a match somebody left would re-simulate a different
// world and report a desync that never happened.
//
// v1 files still load. They have no event lists, which is exactly right for
// them: they were written by a build where leaving changed nothing.
const (
	Magic     = "ARNR"
	Format    = uint16(2)
	minFormat = uint16(1)
)

// EventKind is a change to who is in the match, as opposed to what they did.
type EventKind uint8

const (
	// EventDepart takes a disconnected player's avatar out of play after a
	// short linger. See sim.World.Depart.
	EventDepart EventKind = 1
	// EventRejoin puts a player who reconnected back in. See sim.World.Rejoin.
	EventRejoin EventKind = 2
)

// RosterEvent is one such change, applied before the tick it is recorded on.
type RosterEvent struct {
	PlayerID sim.PlayerID
	Kind     EventKind
}

// Header is everything needed to rebuild the starting world.
type Header struct {
	RoomID     string
	Seed       int64
	TickRate   int
	MatchTicks uint32
	Roster     []sim.Player
}

// Store hands out recorders, up to a limit.
type Store struct {
	dir    string
	max    int
	active atomic.Int32
}

// NewStore prepares the output directory. An empty dir disables recording, and
// every method on a nil Store is a no-op, so callers do not have to branch.
func NewStore(dir string, max int) (*Store, error) {
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("replay dir: %w", err)
	}
	if max <= 0 {
		max = 1
	}
	return &Store{dir: dir, max: max}, nil
}

// Begin starts recording a match, or returns nil when the sample is already
// full. A nil *Recorder is usable — its methods do nothing — so the room does
// not need to know whether it is being recorded.
func (s *Store) Begin(h Header) *Recorder {
	if s == nil {
		return nil
	}
	if s.active.Add(1) > int32(s.max) {
		s.active.Add(-1)
		return nil
	}
	r := &Recorder{store: s, header: h}
	r.writeHeader()
	return r
}

// Recorder accumulates one match in memory and writes it out at the end.
//
// In memory, because this is called from the tick goroutine: a write syscall
// there would put the filesystem inside the tick budget, and a slow disk would
// show up to players as the simulation stuttering. The whole match is a couple
// of hundred kilobytes, and it is written once, off the tick path.
type Recorder struct {
	store  *Store
	header Header
	buf    []byte
	frames uint32
	closed bool
	// pending holds the roster events that arrived since the last frame. They
	// are written with the tick they precede, because that is when the
	// simulation saw them — the room applies them while draining, before it
	// advances the world.
	pending []RosterEvent
	// ids is Frame's scratch space for sorting the input map's keys, reused
	// across ticks. A recorder belongs to exactly one room and Frame is only
	// ever called from that room's tick goroutine, so there is no one to share
	// it with — and a fresh slice per tick is an allocation inside the tick
	// budget, which is the one place this package has promised not to spend.
	ids []sim.PlayerID
}

func (r *Recorder) writeHeader() {
	r.buf = append(r.buf, Magic...)
	r.buf = binary.BigEndian.AppendUint16(r.buf, Format)
	r.buf = binary.BigEndian.AppendUint16(r.buf, uint16(r.header.TickRate))
	r.buf = binary.BigEndian.AppendUint32(r.buf, r.header.MatchTicks)
	r.buf = binary.BigEndian.AppendUint64(r.buf, uint64(r.header.Seed))
	r.buf = binary.BigEndian.AppendUint16(r.buf, uint16(len(r.header.Roster)))
	for _, p := range r.header.Roster {
		r.buf = binary.BigEndian.AppendUint32(r.buf, uint32(p.ID))
		var bot byte
		if p.Bot {
			bot = 1
		}
		r.buf = append(r.buf, bot)
	}
}

// Frame records the inputs applied on one tick.
//
// The map is iterated in sorted order. Go randomises map iteration, and a
// replay whose frames come back in a different order than they went in is a
// replay that does not reproduce the match — the one bug this file format could
// have that would be invisible until someone relied on it.
// Depart and Rejoin record a change to who is in the match. They are buffered
// until the next Frame, which stamps them with the tick they take effect on.
//
// The room calls these from its own goroutine while draining, before the tick
// they belong to is simulated, which is the same order Verify replays them in.
func (r *Recorder) Depart(id sim.PlayerID) { r.event(id, EventDepart) }

// Rejoin is Depart's counterpart, for a player who reconnected in time.
func (r *Recorder) Rejoin(id sim.PlayerID) { r.event(id, EventRejoin) }

func (r *Recorder) event(id sim.PlayerID, kind EventKind) {
	if r == nil || r.closed {
		return
	}
	r.pending = append(r.pending, RosterEvent{PlayerID: id, Kind: kind})
}

func (r *Recorder) Frame(tick uint32, inputs map[sim.PlayerID]sim.Input) {
	if r == nil || r.closed {
		return
	}
	// A tick with nothing in it is not written, which is what keeps an idle
	// match cheap — but a roster event is something, even on a tick where
	// nobody moved.
	if len(inputs) == 0 && len(r.pending) == 0 {
		return
	}
	ids := r.ids[:0]
	for id := range inputs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	r.ids = ids

	r.buf = binary.BigEndian.AppendUint32(r.buf, tick)
	r.buf = binary.BigEndian.AppendUint16(r.buf, uint16(len(ids)))
	for _, id := range ids {
		in := inputs[id]
		r.buf = binary.BigEndian.AppendUint32(r.buf, uint32(id))
		r.buf = append(r.buf, byte(in.MX), byte(in.MY))
		var fire byte
		if in.Fire {
			fire = 1
		}
		r.buf = append(r.buf, fire)
		r.buf = binary.BigEndian.AppendUint16(r.buf, uint16(in.Aim))
		r.buf = binary.BigEndian.AppendUint32(r.buf, in.Seq)
		r.buf = append(r.buf, in.LagTicks)
	}
	r.buf = binary.BigEndian.AppendUint16(r.buf, uint16(len(r.pending)))
	for _, e := range r.pending {
		r.buf = binary.BigEndian.AppendUint32(r.buf, uint32(e.PlayerID))
		r.buf = append(r.buf, byte(e.Kind))
	}
	r.pending = r.pending[:0]
	r.frames++
}

// endMarker cannot collide with a tick: ticks are counted from 1 and a match
// that reached four billion of them has other problems.
const endMarker = uint32(0xFFFFFFFF)

// Close writes the recording out, stamped with the tick it ended on and the
// checksum of the world it ended in. Those two numbers are the assertion the
// replay is checked against.
func (r *Recorder) Close(finalTick uint32, checksum uint64) error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	defer r.store.active.Add(-1)

	r.buf = binary.BigEndian.AppendUint32(r.buf, endMarker)
	r.buf = binary.BigEndian.AppendUint32(r.buf, finalTick)
	r.buf = binary.BigEndian.AppendUint64(r.buf, checksum)

	path := filepath.Join(r.store.dir, safeName(r.header.RoomID)+".arnr")
	if err := os.WriteFile(path, r.buf, 0o644); err != nil {
		metrics.ReplayErrors.Inc()
		return err
	}
	metrics.ReplaysWritten.Inc()
	return nil
}

// safeName keeps a room id from reaching outside the replay directory. Room ids
// are server-generated today, which is exactly the assumption that quietly
// stops being true.
func safeName(id string) string {
	if id == "" {
		return "room"
	}
	clean := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, id)
	if len(clean) > 96 {
		clean = clean[:96]
	}
	return clean
}

// Recording is a replay read back from disk.
type Recording struct {
	// Version is the format the file was written in, so a caller can tell a
	// recording that had no roster events from one that happened not to
	// contain any.
	Version   uint16
	Header    Header
	Frames    []Frame
	FinalTick uint32
	Checksum  uint64
}

type Frame struct {
	Tick   uint32
	Inputs map[sim.PlayerID]sim.Input
	// Events are applied before the tick is simulated, in the order they were
	// recorded. Always empty for a v1 recording.
	Events []RosterEvent
}

// Load reads a recording.
func Load(path string) (*Recording, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Read(bufio.NewReader(f))
}

type reader interface {
	Read(p []byte) (int, error)
}

// Read parses a recording from r.
func Read(src reader) (*Recording, error) {
	r := &parser{src: src}
	magic := r.bytes(4)
	if r.err != nil {
		return nil, r.err
	}
	if string(magic) != Magic {
		return nil, fmt.Errorf("not a replay file (magic %q)", magic)
	}
	version := r.u16()
	if version < minFormat || version > Format {
		return nil, fmt.Errorf("replay format v%d, this build reads v%d..v%d", version, minFormat, Format)
	}
	rec := &Recording{Version: version}
	rec.Header.TickRate = int(r.u16())
	rec.Header.MatchTicks = r.u32()
	rec.Header.Seed = int64(r.u64())
	n := int(r.u16())
	for i := 0; i < n && r.err == nil; i++ {
		id := r.u32()
		bot := r.byte1()
		rec.Header.Roster = append(rec.Header.Roster, sim.Player{ID: sim.PlayerID(id), Bot: bot == 1})
	}
	for r.err == nil {
		tick := r.u32()
		if tick == endMarker {
			rec.FinalTick = r.u32()
			rec.Checksum = r.u64()
			break
		}
		count := int(r.u16())
		f := Frame{Tick: tick, Inputs: make(map[sim.PlayerID]sim.Input, count)}
		for i := 0; i < count && r.err == nil; i++ {
			id := sim.PlayerID(r.u32())
			mx := int8(r.byte1())
			my := int8(r.byte1())
			fire := r.byte1() == 1
			aim := int16(r.u16())
			seq := r.u32()
			lag := r.byte1()
			f.Inputs[id] = sim.Input{MX: mx, MY: my, Fire: fire, Aim: aim, Seq: seq, LagTicks: lag}
		}
		// v1 frames end after the inputs. Reading an event count there would
		// consume the next frame's tick, so the field is only present — and
		// only read — from v2 on.
		if version >= 2 {
			events := int(r.u16())
			for i := 0; i < events && r.err == nil; i++ {
				id := sim.PlayerID(r.u32())
				f.Events = append(f.Events, RosterEvent{PlayerID: id, Kind: EventKind(r.byte1())})
			}
		}
		rec.Frames = append(rec.Frames, f)
	}
	if r.err != nil {
		return nil, r.err
	}
	return rec, nil
}

// Verify replays the recording and reports the checksum it reaches.
//
// This is the whole point of storing inputs rather than outcomes: the replay is
// re-simulated, not re-read, so a mismatch means the build that is running now
// does not reproduce the match that was played. That is either a desync bug or
// a gameplay change — both worth knowing about, and neither visible from a
// recording of positions.
func (rec *Recording) Verify() (checksum uint64, ok bool) {
	w := sim.NewWorld(rec.Header.Seed, rec.Header.TickRate, rec.Header.MatchTicks, rec.Header.Roster)
	next := 0
	for tick := uint32(1); tick <= rec.finalTick(); tick++ {
		var inputs map[sim.PlayerID]sim.Input
		if next < len(rec.Frames) && rec.Frames[next].Tick == tick {
			f := rec.Frames[next]
			inputs = f.Inputs
			// Before the step, because that is where the room applied them:
			// events are drained and applied, then the world advances.
			for _, e := range f.Events {
				switch e.Kind {
				case EventDepart:
					w.Depart(e.PlayerID)
				case EventRejoin:
					w.Rejoin(e.PlayerID)
				}
			}
			next++
		}
		w.Step(inputs)
	}
	got := w.Checksum()
	return got, got == rec.Checksum
}

// maxReplayTicks bounds a replay at a day of simulation — far past any match
// this server forms, and near enough that a corrupt or hostile final tick
// cannot turn Verify into a four-billion-step loop. The file says how many
// ticks to replay, and a file is not a thing to be trusted about how much work
// to do.
const maxReplayTicks = uint32(24 * 60 * 60 * 60)

// finalTick is the recorded end, clamped to what the header says the match
// could possibly have run for.
func (rec *Recording) finalTick() uint32 {
	limit := maxReplayTicks
	if rec.Header.MatchTicks > 0 && rec.Header.MatchTicks < limit {
		limit = rec.Header.MatchTicks
	}
	if rec.FinalTick > limit {
		return limit
	}
	return rec.FinalTick
}

type parser struct {
	src reader
	err error
}

func (p *parser) bytes(n int) []byte {
	if p.err != nil {
		return nil
	}
	out := make([]byte, n)
	read := 0
	for read < n {
		got, err := p.src.Read(out[read:])
		read += got
		if err != nil {
			p.err = err
			return nil
		}
	}
	return out
}

func (p *parser) byte1() byte {
	b := p.bytes(1)
	if b == nil {
		return 0
	}
	return b[0]
}

func (p *parser) u16() uint16 {
	b := p.bytes(2)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}

func (p *parser) u32() uint32 {
	b := p.bytes(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

func (p *parser) u64() uint64 {
	b := p.bytes(8)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
