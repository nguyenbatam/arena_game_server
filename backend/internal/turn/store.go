package turn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// LogWindow is how many events a match keeps. Past this the cursor cannot be
// diffed from and the client is told to resync — the escape hatch every
// cursor-based scheme needs. An unbounded log is a memory leak wearing a
// feature's clothes.
const LogWindow = 500

// A match is ephemeral. Without an expiry every match ever played would sit in
// Redis forever — the kind of leak that looks fine for a month and then fills
// the instance. Live matches get the long window so a disconnected player can
// still come back; finished ones drain quickly, keeping just enough time for a
// last sync and a post-game screen.
//
// These are the defaults, not the law: a deployment sizes them against its own
// Redis. Both are refreshed on every write, so each is measured from the last
// move rather than from the deal — and since a match that ends sets the shorter
// one on its final commit, finished matches are almost all of what is stored at
// any moment. A measured match costs ~5.6 KB across its three keys, so the
// steady-state bill is roughly matches-per-second × EndedTTL × 5.6 KB: at a
// thousand matches a second, every extra minute of EndedTTL is another 336 MB.
//
// EndedTTL is the one worth tuning. LiveTTL is a backstop that a running
// matchmaker keeps from ever mattering — the deadline sweeper auto-plays an
// abandoned match to its end within a few turn limits, dropping it onto the
// shorter clock. Without a matchmaker role running, abandoned matches really do
// sit for the whole of LiveTTL.
const (
	DefaultLiveTTL  = 24 * time.Hour
	DefaultEndedTTL = 30 * time.Minute
)

// TTL is how long a match's keys survive, carried by the store rather than set
// on a package variable: two stores in one process (a test, a migration) must
// be able to disagree without racing each other.
type TTL struct {
	Live  time.Duration
	Ended time.Duration
}

func (t TTL) orDefaults() TTL {
	if t.Live <= 0 {
		t.Live = DefaultLiveTTL
	}
	if t.Ended <= 0 {
		t.Ended = DefaultEndedTTL
	}
	return t
}

// forState picks the expiry a state should carry.
func (t TTL) forState(s *State) time.Duration {
	if s.Ended {
		return t.Ended
	}
	return t.Live
}

var (
	// ErrVersionConflict means someone else wrote first. The caller must
	// re-read and redo its work on top of the newer state, never overwrite.
	ErrVersionConflict = errors.New("turn: version conflict")
	ErrNoMatch         = errors.New("turn: no such match")
)

// Store holds a match: its state and its event log, together.
//
// Keeping them in one component is the whole point. State and log are two
// halves of one fact — "this move happened" — and writing them separately
// leaves a window where a crash advances the state without logging the event,
// so a client diffing from its cursor would never learn about it. Commit writes
// both or neither.
//
// In the arena the world lives in one goroutine's memory and dies with the
// process. Here state is durable and any node can serve any match, which is
// what lets the turn-based role scale out without sticky routing.
type Store interface {
	// Create writes a fresh match together with its opening events.
	Create(ctx context.Context, s *State, events []Event) ([]Event, error)
	Get(ctx context.Context, matchID string) (*State, uint64, error)
	// Commit advances the state and appends its events atomically, but only if
	// the stored version still matches expect.
	Commit(ctx context.Context, s *State, expect uint64, events []Event) ([]Event, error)
	// Since returns events after the cursor. gap reports that the cursor has
	// fallen out of the retained window, so the caller must send full state.
	Since(ctx context.Context, matchID string, since uint64) (evs []Event, current uint64, gap bool, err error)
}

// ---------------------------------------------------------------------------

type matchRecord struct {
	state     *State
	version   uint64
	events    []Event
	nextSeq   uint64
	expiresAt time.Time
}

type MemoryStore struct {
	mu  sync.Mutex
	m   map[string]*matchRecord
	ttl TTL
	now func() time.Time
	// writes counts commits since the last sweep. See evictExpired.
	writes int
}

func NewMemoryStore(ttl TTL) *MemoryStore {
	return &MemoryStore{m: map[string]*matchRecord{}, ttl: ttl.orDefaults(), now: time.Now}
}

// sweepEvery and sweepBatch amortise eviction over writes, the same shape
// ratelimit.Window uses and for the same reason — and the same invariant holds
// here:
//
//	sweepBatch / sweepEvery  >  1
//
// Each write admits at most one match, so a sweep has to inspect strictly more
// than one entry per write or eviction loses the race and the map grows anyway.
// At 64 per 8 writes the margin is 8x. TestExpiredMatchesAreEvicted holds it.
const (
	sweepEvery = 8
	sweepBatch = 64
)

// evictExpired drops finished matches. Done inline on writes rather than from a
// janitor goroutine: no lifecycle to manage, and nothing for goleak to find.
//
// Bounded, and only every sweepEvery-th write. It used to walk the whole map on
// every Create and every Commit, with s.mu held — so the cost of one player's
// move was proportional to how many matches the process was holding, and the
// total cost of running N matches to completion was quadratic in N. Every other
// mover waits behind that walk, which makes it a latency floor on the mode's
// only write path rather than merely wasted work.
//
// Go randomises map iteration, which is the property that makes a partial sweep
// sound: each pass starts somewhere else, so no entry can hide behind the batch
// limit the way it could under a stable order. Anything the sweep misses is
// still reclaimed the moment somebody looks it up — see live.
//
// Measured by BenchmarkCommitWithManyLiveMatches on an M4, one commit against a
// store holding this many other matches:
//
//	live      full walk      bounded
//	100        1925 ns        725 ns
//	1000      10956 ns        649 ns
//	10000     58355 ns        467 ns
func (s *MemoryStore) evictExpired() {
	s.writes++
	if s.writes < sweepEvery {
		return
	}
	s.writes = 0
	now := s.now()
	seen := 0
	for id, rec := range s.m {
		if !rec.expiresAt.IsZero() && now.After(rec.expiresAt) {
			delete(s.m, id)
		}
		if seen++; seen >= sweepBatch {
			return
		}
	}
}

func (s *MemoryStore) live(matchID string) (*matchRecord, bool) {
	rec, ok := s.m[matchID]
	if !ok {
		return nil, false
	}
	if !rec.expiresAt.IsZero() && s.now().After(rec.expiresAt) {
		delete(s.m, matchID)
		return nil, false
	}
	return rec, true
}

func (s *MemoryStore) Create(_ context.Context, st *State, events []Event) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpired()
	rec := &matchRecord{version: 1}
	stored := rec.append(events)
	if n := len(stored); n > 0 {
		st.Seq = stored[n-1].Seq
	}
	rec.state = st.Clone()
	rec.expiresAt = s.now().Add(s.ttl.forState(st))
	s.m[st.MatchID] = rec
	return stored, nil
}

func (s *MemoryStore) Get(_ context.Context, matchID string) (*State, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.live(matchID)
	if !ok {
		return nil, 0, ErrNoMatch
	}
	return rec.state.Clone(), rec.version, nil
}

func (s *MemoryStore) Commit(_ context.Context, st *State, expect uint64, events []Event) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpired()
	rec, ok := s.live(st.MatchID)
	if !ok {
		return nil, ErrNoMatch
	}
	if rec.version != expect {
		return nil, ErrVersionConflict
	}
	stored := rec.append(events)
	if n := len(stored); n > 0 {
		st.Seq = stored[n-1].Seq
	}
	rec.state = st.Clone()
	rec.version = expect + 1
	rec.expiresAt = s.now().Add(s.ttl.forState(st))
	return stored, nil
}

func (s *MemoryStore) Since(_ context.Context, matchID string, since uint64) ([]Event, uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.live(matchID)
	if !ok {
		return nil, 0, false, ErrNoMatch
	}
	current := rec.nextSeq
	if len(rec.events) == 0 {
		return nil, current, since != current, nil
	}
	oldest := rec.events[0].Seq
	// since+1 is the first event we owe them; if that is already trimmed away
	// the diff cannot be built.
	if since+1 < oldest {
		return nil, current, true, nil
	}
	out := make([]Event, 0, len(rec.events))
	for _, e := range rec.events {
		if e.Seq > since {
			out = append(out, e)
		}
	}
	return out, current, false, nil
}

func (r *matchRecord) append(events []Event) []Event {
	out := make([]Event, 0, len(events))
	for _, e := range events {
		r.nextSeq++
		e.Seq = r.nextSeq
		r.events = append(r.events, e)
		out = append(out, e)
	}
	if over := len(r.events) - LogWindow; over > 0 {
		r.events = append([]Event(nil), r.events[over:]...)
	}
	return out
}

// ---------------------------------------------------------------------------

// RedisStore keeps the state as a JSON blob and the log as a Redis Stream, and
// writes both inside one Lua script.
//
// Redis runs a script to completion with nothing interleaved, so "compare the
// version, write the state, append the events" is one indivisible step — the
// dual-write window closes without needing a transactional outbox.
//
// JSON for the state is a deliberate choice here and would be the wrong one in
// the arena: it is small, written a handful of times per match rather than 20
// times a second, and being able to read a stuck match with redis-cli is worth
// more than the bytes. Snapshots on the tick path stay protobuf.
type RedisStore struct {
	rdb *redis.Client
	ttl TTL
}

func NewRedisStore(rdb *redis.Client, ttl TTL) *RedisStore {
	return &RedisStore{rdb: rdb, ttl: ttl.orDefaults()}
}

func stateKey(matchID string) string { return "turn:state:" + matchID }
func seqKey(matchID string) string   { return "turn:seq:" + matchID }
func logKey(matchID string) string   { return "turn:log:" + matchID }

// streamSeq reads the sequence number out of a Redis stream id ("<seq>-<n>").
// Every id in the log is written by the commit script as "<seq>-0", so the
// sequence half is the cursor value the client acks against.
func streamSeq(id string) (uint64, bool) {
	if i := strings.IndexByte(id, '-'); i >= 0 {
		id = id[:i]
	}
	n, err := strconv.ParseUint(id, 10, 64)
	return n, err == nil
}

// commitLua returns {0, seq...} on success, {-1} when the match is missing, and
// {currentVersion} when the CAS lost. Version starts at 1, so 0 can never be a
// real version and stays unambiguous as the success marker.
var commitLua = redis.NewScript(`
local statek, seqk, logk = KEYS[1], KEYS[2], KEYS[3]
local expect  = tonumber(ARGV[1])
local window  = tonumber(ARGV[2])
local ttl     = tonumber(ARGV[3])
local blob    = ARGV[4]

local cur = redis.call('HGET', statek, 'v')
if cur == false then return {-1} end
if tonumber(cur) ~= expect then return {tonumber(cur)} end

redis.call('HSET', statek, 'v', expect + 1, 'd', blob)

local out = {0}
for i = 5, #ARGV do
  local n = redis.call('INCR', seqk)
  redis.call('XADD', logk, 'MAXLEN', '~', window, n .. '-0', 'e', ARGV[i])
  out[#out+1] = n
end

-- Refresh every key together: state, cursor and log must die at the same time,
-- or a surviving cursor would point into a log that no longer exists.
redis.call('EXPIRE', statek, ttl)
redis.call('EXPIRE', seqk, ttl)
redis.call('EXPIRE', logk, ttl)
return out
`)

var createLua = redis.NewScript(`
local statek, seqk, logk = KEYS[1], KEYS[2], KEYS[3]
local window = tonumber(ARGV[1])
local ttl    = tonumber(ARGV[2])
local blob   = ARGV[3]

-- Create means a fresh match, so anything already filed under these keys is a
-- previous one that has not aged out yet. Leaving it would splice two matches
-- together: HSET resets the state to version 1 while the old cursor keeps
-- counting and the old events stay in the log, so a client syncing from zero
-- replays a game that is already over.
redis.call('DEL', statek, seqk, logk)
redis.call('HSET', statek, 'v', 1, 'd', blob)

local out = {}
for i = 4, #ARGV do
  local n = redis.call('INCR', seqk)
  redis.call('XADD', logk, 'MAXLEN', '~', window, n .. '-0', 'e', ARGV[i])
  out[#out+1] = n
end

redis.call('EXPIRE', statek, ttl)
redis.call('EXPIRE', seqk, ttl)
redis.call('EXPIRE', logk, ttl)
return out
`)

func (s *RedisStore) keys(matchID string) []string {
	return []string{stateKey(matchID), seqKey(matchID), logKey(matchID)}
}

func encodeArgs(head []any, events []Event) ([]any, error) {
	args := head
	for _, e := range events {
		blob, err := encodeEvent(e)
		if err != nil {
			return nil, err
		}
		args = append(args, blob)
	}
	return args, nil
}

func seqsFrom(res any, events []Event, skip int) ([]Event, error) {
	raw, ok := res.([]any)
	if !ok || len(raw)-skip != len(events) {
		return nil, fmt.Errorf("turn: unexpected script reply %v", res)
	}
	out := make([]Event, 0, len(events))
	for i, e := range events {
		n, _ := raw[i+skip].(int64)
		e.Seq = uint64(n)
		out = append(out, e)
	}
	return out, nil
}

func (s *RedisStore) Create(ctx context.Context, st *State, events []Event) ([]Event, error) {
	blob, err := json.Marshal(st)
	if err != nil {
		return nil, err
	}
	args, err := encodeArgs([]any{LogWindow, int(s.ttl.forState(st).Seconds()), blob}, events)
	if err != nil {
		return nil, err
	}
	res, err := createLua.Run(ctx, s.rdb, s.keys(st.MatchID), args...).Result()
	if err != nil {
		return nil, err
	}
	stored, err := seqsFrom(res, events, 0)
	if err != nil {
		return nil, err
	}
	if n := len(stored); n > 0 {
		st.Seq = stored[n-1].Seq
	}
	return stored, nil
}

func (s *RedisStore) Commit(ctx context.Context, st *State, expect uint64, events []Event) ([]Event, error) {
	blob, err := json.Marshal(st)
	if err != nil {
		return nil, err
	}
	args, err := encodeArgs([]any{expect, LogWindow, int(s.ttl.forState(st).Seconds()), blob}, events)
	if err != nil {
		return nil, err
	}
	res, err := commitLua.Run(ctx, s.rdb, s.keys(st.MatchID), args...).Result()
	if err != nil {
		return nil, err
	}
	raw, ok := res.([]any)
	if !ok || len(raw) == 0 {
		return nil, fmt.Errorf("turn: unexpected commit reply %v", res)
	}
	switch head, _ := raw[0].(int64); {
	case head == -1:
		return nil, ErrNoMatch
	case head != 0:
		return nil, ErrVersionConflict
	}
	stored, err := seqsFrom(res, events, 1)
	if err != nil {
		return nil, err
	}
	if n := len(stored); n > 0 {
		st.Seq = stored[n-1].Seq
	}
	return stored, nil
}

func (s *RedisStore) Get(ctx context.Context, matchID string) (*State, uint64, error) {
	vals, err := s.rdb.HMGet(ctx, stateKey(matchID), "v", "d").Result()
	if err != nil {
		return nil, 0, err
	}
	if len(vals) != 2 || vals[0] == nil || vals[1] == nil {
		return nil, 0, ErrNoMatch
	}
	verStr, _ := vals[0].(string)
	version, err := strconv.ParseUint(verStr, 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("turn: bad version %q", verStr)
	}
	blob, _ := vals[1].(string)
	var st State
	if err := json.Unmarshal([]byte(blob), &st); err != nil {
		return nil, 0, err
	}
	if st.AppliedIdem == nil {
		st.AppliedIdem = map[string]uint32{}
	}
	return &st, version, nil
}

// sinceLua answers a cursor in one round trip.
//
// It used to be three sequential ones — read the cursor, read the oldest event
// to find out whether a diff is even possible, then read the range — and every
// TURN_SYNC pays them: a reconnect, a client that missed a push, a tab coming
// back from the background. Sync's own state read is the only other one left.
//
// The round trips are the smaller half of what this fixes. Read separately, the
// cursor is a moment older than the log, so a move committed between the two
// reads produced a reply whose events ran past the current_seq sent with them —
// the client acked the lower number and was handed the same events again on its
// next sync. Redis runs a script to completion with nothing interleaved, so the
// two now describe the same instant, which is the property the whole cursor
// scheme rests on.
//
// The reply is {current, gap, id, payload, id, payload, ...}. The gap decision
// is made here rather than in Go so that a cursor which has fallen out of the
// window costs no range read at all — that is exactly the case where the range
// would be the entire retained log, fetched only to be thrown away.
var sinceLua = redis.NewScript(`
local seqk, logk = KEYS[1], KEYS[2]
local sinceStr = ARGV[1]
local since    = tonumber(sinceStr)

local current = tonumber(redis.call('GET', seqk)) or 0

local oldest = redis.call('XRANGE', logk, '-', '+', 'COUNT', 1)
if #oldest == 0 then
  -- No log at all. A cursor that already names the current sequence is simply
  -- up to date; anything else cannot be diffed from.
  if since == current then return {current, 0} end
  return {current, 1}
end

local oldestSeq = tonumber(string.match(oldest[1][1], '^(%d+)'))
if oldestSeq == nil or since + 1 < oldestSeq then
  -- The window has moved past this cursor: no diff is possible.
  return {current, 1}
end

local msgs = redis.call('XRANGE', logk, '(' .. sinceStr .. '-0', '+')
local out = {current, 0}
for i = 1, #msgs do
  out[#out+1] = msgs[i][1]
  -- Values come back flat as {field, value, ...}; the writer only ever sets 'e'.
  out[#out+1] = msgs[i][2][2]
end
return out
`)

func (s *RedisStore) Since(ctx context.Context, matchID string, since uint64) ([]Event, uint64, bool, error) {
	raw, err := sinceLua.Run(ctx, s.rdb,
		[]string{seqKey(matchID), logKey(matchID)}, strconv.FormatUint(since, 10)).Slice()
	if err != nil {
		return nil, 0, false, err
	}
	if len(raw) < 2 {
		return nil, 0, false, fmt.Errorf("turn: unexpected since reply %v", raw)
	}
	current := uint64(0)
	if n, ok := raw[0].(int64); ok && n > 0 {
		current = uint64(n)
	}
	gapFlag, _ := raw[1].(int64)
	if gapFlag != 0 {
		return nil, current, true, nil
	}

	// Whatever follows is (id, payload) pairs. An odd tail would mean the
	// script and this loop disagree, which is a bug rather than a bad match.
	rest := raw[2:]
	if len(rest)%2 != 0 {
		return nil, current, false, fmt.Errorf("turn: since reply has %d trailing values", len(rest))
	}
	out := make([]Event, 0, len(rest)/2)
	for i := 0; i < len(rest); i += 2 {
		id, _ := rest[i].(string)
		payload, _ := rest[i+1].(string)
		e, derr := decodeEvent([]byte(payload))
		if derr != nil {
			return nil, current, false, derr
		}
		// A stream id this loop cannot parse is refused rather than defaulted.
		// The zero it used to fall back to is not a harmless placeholder: Seq is
		// the cursor the client acks, so an unparsed id handed it a sequence
		// number *below* every event it already had, and its next sync asked
		// for the whole log again — for the rest of the match, since nothing
		// would ever move the cursor forward past that event. Only this package
		// writes these ids, and it writes them as "<seq>-0", so failing here
		// means the log has been written by something else.
		seq, ok := streamSeq(id)
		if !ok {
			return nil, current, false, fmt.Errorf("turn: unparsable stream id %q in log for %s", id, matchID)
		}
		e.Seq = seq
		out = append(out, e)
	}
	return out, current, false, nil
}
