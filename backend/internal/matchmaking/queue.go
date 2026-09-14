package matchmaking

import (
	"context"
	"sort"
	"sync"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type Player = pb.QueuePlayer

type Match struct {
	Players []*pb.QueuePlayer
	Bots    int
}

// Rules is what a match has to satisfy to be formed.
//
// It is a struct rather than four arguments because the skill half arrived
// late: the queue used to be pure FIFO, with every player handed the same
// Skill: 1000, which made "skill-based matchmaking" a field on the wire and
// nothing else. The names now have to carry their meaning to two
// implementations and a Lua script.
type Rules struct {
	RoomSize   int
	MinPlayers int
	// Timeout is how long the player at the front of the queue waits before a
	// short match is formed around them and bots fill the rest.
	//
	// Zero — or negative — switches that off: matches then only form when they
	// are full, and nobody is ever dropped into a bot game. It has to mean
	// that rather than be taken literally, because "wait >= 0" is true on the
	// very first pass: a queue configured with no timeout would have formed an
	// undersized bot-filled match for every arrival, 20 times a second, and
	// the real players in it would never have been given the chance to meet
	// each other. QUEUE_TIMEOUT defaults to 2s so this is one env var away,
	// not a hypothetical.
	Timeout time.Duration

	// SkillWindow is the rating spread a match may open with. Zero disables
	// skill entirely and the queue is pure FIFO again, which is what the
	// in-memory default and most tests want.
	//
	// A fixed window is not usable on its own: set it tight and a player at the
	// edge of the distribution waits forever, set it loose and it may as well
	// not exist. So it is a starting width, and Widen is the rest of the rule.
	SkillWindow int
	// Widen is how much the window grows for each second the anchor has been
	// waiting. This is the part that actually makes skill matchmaking work —
	// every real implementation trades match quality for queue time as the wait
	// runs on, because a perfect match nobody is in is worth nothing.
	Widen int
	// MaxWindow caps the widening. Past some spread the match is not a match
	// any more and the honest answer is bots, which Timeout already provides.
	// Zero means the window grows without limit.
	MaxWindow int
}

// scanDepth is how far down the queue one pass looks for company, as a
// multiple of the room size.
//
// The scan is bounded because this runs on a 50 ms ticker and, in Redis, inside
// a script that blocks the server for as long as it runs — a pass that walked
// ten thousand queued players would be a pass that hurt everything else. The
// cost of the bound is that a match may not form while in-window players sit
// deeper in the queue than this; the window widens with the anchor's wait, so
// it forms shortly afterwards instead.
//
// Both implementations use the same depth, in the same order, for the same
// reason every other pair in this repo does: a difference between them only
// ever surfaces in production.
const scanDepth = 8

func (r Rules) scan() int { return r.RoomSize * scanDepth }

func (r Rules) sane() Rules {
	if r.RoomSize <= 0 {
		r.RoomSize = 1
	}
	if r.MinPlayers <= 0 {
		r.MinPlayers = 1
	}
	if r.MinPlayers > r.RoomSize {
		r.MinPlayers = r.RoomSize
	}
	return r
}

// windowAt is the spread allowed for someone who has been waiting this long.
// A return of 0 means "no limit", which is also what a zero SkillWindow asks
// for — the two cases behave identically and there is no reason to tell them
// apart downstream.
func (r Rules) windowAt(wait time.Duration) int {
	if r.SkillWindow <= 0 {
		return 0
	}
	w := r.SkillWindow
	if r.Widen > 0 && wait > 0 {
		w += int(wait.Seconds() * float64(r.Widen))
	}
	if r.MaxWindow > 0 && w > r.MaxWindow {
		w = r.MaxWindow
	}
	return w
}

type Queue interface {
	Enqueue(ctx context.Context, p *pb.QueuePlayer) error
	Remove(ctx context.Context, connID string) error
	TryForm(ctx context.Context, r Rules) (*Match, error)
	Depth(ctx context.Context) (int64, error)
}

type MemoryQueue struct {
	mu   sync.Mutex
	list []*pb.QueuePlayer
	// queued is the same conn ids as list, as a set. It exists only so Enqueue
	// can reject a duplicate without walking the queue.
	//
	// The walk it replaces made Enqueue linear in the queue depth, and Enqueue
	// runs on a player's JOIN_QUEUE and again on every requeue a refused
	// placement produces — so filling the queue was quadratic in the number of
	// players filling it. Measured on this machine: 100 players 44us, 1k
	// players 1.2ms, 10k players 71ms, all of it under the lock. Redis never
	// had the problem; its twin is an HEXISTS.
	queued map[string]struct{}
}

func NewMemory() *MemoryQueue {
	return &MemoryQueue{queued: make(map[string]struct{})}
}

// Enqueue keeps the list ordered by (queued_at, conn_id).
//
// The order matters more than it looks: Redis holds the queue in a sorted set,
// where two players who queued in the same millisecond are ordered by id, and
// the anchor this queue forms a match around is whoever comes first. Appending
// in arrival order instead would pick a different anchor from the Redis twin
// whenever timestamps tie — which they do constantly, since a burst of arrivals
// all lands in one millisecond. Sorting on the way in keeps TryForm a linear
// scan and the two implementations one behaviour.
func (q *MemoryQueue) Enqueue(_ context.Context, p *pb.QueuePlayer) error {
	if p.QueuedAt == 0 {
		p.QueuedAt = time.Now().UnixMilli()
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.queued == nil {
		q.queued = make(map[string]struct{})
	}
	if _, dup := q.queued[p.ConnId]; dup {
		return nil
	}
	q.queued[p.ConnId] = struct{}{}
	i := sort.Search(len(q.list), func(i int) bool { return !before(q.list[i], p) })
	q.list = append(q.list, nil)
	copy(q.list[i+1:], q.list[i:])
	q.list[i] = p
	return nil
}

// before is the queue's order, and the same comparison Redis makes between two
// sorted-set members with equal scores.
func before(a, b *pb.QueuePlayer) bool {
	if a.QueuedAt != b.QueuedAt {
		return a.QueuedAt < b.QueuedAt
	}
	return a.ConnId < b.ConnId
}

// drop removes every player the predicate names, keeping list and queued in
// step. Callers hold q.mu.
//
// The tail is cleared rather than left behind the new length. Compacting into
// q.list[:0] leaves the evicted pointers live in the backing array, so a queue
// that once held ten thousand players keeps ten thousand QueuePlayer messages
// reachable no matter how short it gets afterwards.
func (q *MemoryQueue) drop(match func(*pb.QueuePlayer) bool) {
	dst := q.list[:0]
	for _, p := range q.list {
		if match(p) {
			delete(q.queued, p.ConnId)
			continue
		}
		dst = append(dst, p)
	}
	for i := len(dst); i < len(q.list); i++ {
		q.list[i] = nil
	}
	q.list = dst
}

func (q *MemoryQueue) Remove(_ context.Context, connID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Every disconnect calls this, and almost none of them are of a player who
	// was actually queued — they were in a match, or never queued at all. The
	// set answers that without touching the list.
	if _, ok := q.queued[connID]; !ok {
		return nil
	}
	q.drop(func(p *pb.QueuePlayer) bool { return p.ConnId == connID })
	return nil
}

func (q *MemoryQueue) Depth(_ context.Context) (int64, error) {
	q.mu.Lock()
	n := len(q.list)
	q.mu.Unlock()
	return int64(n), nil
}

// TryForm groups the player who has waited longest with the closest-rated
// company the window allows.
//
// The longest wait is the anchor rather than, say, the densest cluster of
// ratings, because the anchor is the person the queue is failing. Everything
// else in here follows from that: the window is measured around their rating
// and widens with their wait, and when the timeout finally runs out it is their
// match that gets formed with bots.
func (q *MemoryQueue) TryForm(_ context.Context, r Rules) (*Match, error) {
	r = r.sane()
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.list) == 0 {
		return nil, nil
	}

	anchor := q.list[0]
	wait := time.Since(time.UnixMilli(anchor.QueuedAt))
	window := r.windowAt(wait)

	// The anchor is always in, and the rest are the longest waiting inside the
	// window, looked for no deeper than scan(). Ordering the candidates by
	// rating instead would let a crowd of players at one end of the window keep
	// jumping ahead of the very player the window was drawn around.
	picked := []*pb.QueuePlayer{anchor}
	rest := q.list[1:]
	if depth := r.scan() - 1; len(rest) > depth {
		rest = rest[:depth]
	}
	for _, p := range rest {
		if len(picked) == r.RoomSize {
			break
		}
		if window > 0 && abs(int(p.Skill)-int(anchor.Skill)) > window {
			continue
		}
		picked = append(picked, p)
	}

	full := len(picked) >= r.RoomSize
	timedOut := r.Timeout > 0 && wait >= r.Timeout && len(picked) >= r.MinPlayers
	if !full && !timedOut {
		return nil, nil
	}

	taken := make(map[string]bool, len(picked))
	for _, p := range picked {
		taken[p.ConnId] = true
	}
	q.drop(func(p *pb.QueuePlayer) bool { return taken[p.ConnId] })

	m := &Match{Players: picked}
	if len(picked) < r.RoomSize {
		m.Bots = r.RoomSize - len(picked)
	}
	return m, nil
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

const (
	zkey = "mm:z" // queued_at, so the front of the queue is the oldest wait
	hkey = "mm:h" // the QueuePlayer blob
	skey = "mm:s" // rating, so a skill window is a range query
)

var enqueueLua = redis.NewScript(`
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('ZADD', KEYS[3], ARGV[4], ARGV[1])
return 1
`)

// formMatchLua is the Redis twin of MemoryQueue.TryForm, and the two are held
// to the same assertions by the tests.
//
// It has to be one script for the reason it always did: two matchmaker replicas
// polling the same queue must not both claim the same player. Redis runs a
// script to completion with nothing interleaved, so "pick the anchor, find its
// company, remove them all" either happens or does not.
var formMatchLua = redis.NewScript(`
local zkey, hkey, skey = KEYS[1], KEYS[2], KEYS[3]
local roomSize   = tonumber(ARGV[1])
local minPlayers = tonumber(ARGV[2])
local timeout    = tonumber(ARGV[3])
local now        = tonumber(ARGV[4])
local baseWindow = tonumber(ARGV[5])
local widen      = tonumber(ARGV[6])
local maxWindow  = tonumber(ARGV[7])
local scan       = tonumber(ARGV[8])

local oldest = redis.call('ZRANGE', zkey, 0, 0, 'WITHSCORES')
if #oldest == 0 then
  return {}
end
local anchor     = oldest[1]
local anchorWait = now - tonumber(oldest[2])

-- The window belongs to the anchor's wait, and only Redis knows who the anchor
-- is, so the widening is done here rather than handed in already computed.
local window = 0
if baseWindow > 0 then
  window = baseWindow
  if widen > 0 and anchorWait > 0 then
    window = window + math.floor(anchorWait / 1000 * widen)
  end
  if maxWindow > 0 and window > maxWindow then
    window = maxWindow
  end
end

-- Candidates are taken from the front of the queue — oldest wait first, which
-- is the order the sorted set already holds them in — and filtered by the
-- window drawn around the anchor's rating.
--
-- Scanning by time rather than by rating is deliberate. Reading a range out of
-- the rating index instead would return the lowest-rated players inside the
-- window, which is neither the fairest set nor the set the in-memory twin
-- picks, and the two would then disagree only under a deep queue — in
-- production, at load, where nothing is watching.
local ids = redis.call('ZRANGE', zkey, 0, scan - 1)
local anchorSkill = 0
if window > 0 then
  anchorSkill = tonumber(redis.call('ZSCORE', skey, anchor)) or 0
end

local picked = {anchor}
for i = 2, #ids do
  if #picked >= roomSize then break end
  local id = ids[i]
  if window <= 0 then
    picked[#picked+1] = id
  else
    local sk = tonumber(redis.call('ZSCORE', skey, id)) or 0
    local gap = sk - anchorSkill
    if gap < 0 then gap = -gap end
    if gap <= window then
      picked[#picked+1] = id
    end
  end
end

-- A non-positive timeout means the rule is off, not that it has already
-- elapsed. Kept identical to the Go twin's reading of the same field.
local full     = #picked >= roomSize
local timedOut = timeout > 0 and anchorWait >= timeout and #picked >= minPlayers
if not (full or timedOut) then
  return {}
end

local out = {}
for _, id in ipairs(picked) do
  local blob = redis.call('HGET', hkey, id)
  redis.call('ZREM', zkey, id)
  redis.call('ZREM', skey, id)
  redis.call('HDEL', hkey, id)
  if blob then
    out[#out+1] = blob
  end
end
return out
`)

type RedisQueue struct {
	rdb *redis.Client
}

func NewRedis(rdb *redis.Client) *RedisQueue { return &RedisQueue{rdb: rdb} }

func (q *RedisQueue) Enqueue(ctx context.Context, p *pb.QueuePlayer) error {
	if p.QueuedAt == 0 {
		p.QueuedAt = time.Now().UnixMilli()
	}
	b, err := proto.Marshal(p)
	if err != nil {
		return err
	}
	return enqueueLua.Run(ctx, q.rdb, []string{zkey, hkey, skey}, p.ConnId, p.QueuedAt, b, p.Skill).Err()
}

func (q *RedisQueue) Remove(ctx context.Context, connID string) error {
	pipe := q.rdb.Pipeline()
	pipe.ZRem(ctx, zkey, connID)
	pipe.ZRem(ctx, skey, connID)
	pipe.HDel(ctx, hkey, connID)
	_, err := pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) Depth(ctx context.Context) (int64, error) {
	return q.rdb.ZCard(ctx, zkey).Result()
}

func (q *RedisQueue) TryForm(ctx context.Context, r Rules) (*Match, error) {
	r = r.sane()
	// The window is evaluated against the anchor's wait, which only Redis knows
	// — so the widening rate goes in and the script does the arithmetic. Sending
	// a window computed here would be measuring it against the wrong player.
	raw, err := formMatchLua.Run(ctx, q.rdb, []string{zkey, hkey, skey},
		r.RoomSize, r.MinPlayers, r.Timeout.Milliseconds(), time.Now().UnixMilli(),
		r.SkillWindow, r.Widen, r.MaxWindow, r.scan(),
	).Slice()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	players := make([]*pb.QueuePlayer, 0, len(raw))
	for _, v := range raw {
		var b []byte
		switch t := v.(type) {
		case string:
			b = []byte(t)
		case []byte:
			b = t
		default:
			continue
		}
		var p pb.QueuePlayer
		if proto.Unmarshal(b, &p) != nil {
			continue
		}
		players = append(players, &p)
	}
	if len(players) == 0 {
		return nil, nil
	}
	sort.SliceStable(players, func(i, j int) bool { return players[i].QueuedAt < players[j].QueuedAt })
	bots := 0
	if len(players) < r.RoomSize {
		bots = r.RoomSize - len(players)
	}
	return &Match{Players: players, Bots: bots}, nil
}
