package turn

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Deadlines is the scheduler that replaces the arena's tick counter.
//
// With no tick loop there is nothing counting down, so a turn timer needs an
// external queue of "this match owes a move by then" and a worker that sweeps
// it. The pop must be atomic for the same reason matchmaking's pop is: two
// workers seeing the same expired match would each auto-play, and the player
// would lose two turns to one timeout.
type Deadlines interface {
	// Arm schedules the deadline for one specific turn.
	Arm(ctx context.Context, matchID string, turnNumber uint32, at time.Time) error
	// PopDue atomically claims deadlines that have passed.
	PopDue(ctx context.Context, now time.Time, limit int) ([]Due, error)
}

// Due is an expired deadline. It names the turn it was armed for, not just the
// match — without that the sweeper cannot tell "this turn ran out of time" from
// "this turn was played and the next one is now on the clock", and would spend
// a turn whose timer has barely started.
type Due struct {
	MatchID    string
	TurnNumber uint32
}

func dueKey(matchID string, turn uint32) string {
	return matchID + "#" + strconv.FormatUint(uint64(turn), 10)
}

func parseDue(s string) (Due, bool) {
	i := strings.LastIndexByte(s, '#')
	if i < 0 {
		return Due{}, false
	}
	n, err := strconv.ParseUint(s[i+1:], 10, 32)
	if err != nil {
		return Due{}, false
	}
	return Due{MatchID: s[:i], TurnNumber: uint32(n)}, true
}

// ---------------------------------------------------------------------------

type MemoryDeadlines struct {
	mu sync.Mutex
	at map[string]int64
}

func NewMemoryDeadlines() *MemoryDeadlines { return &MemoryDeadlines{at: map[string]int64{}} }

func (d *MemoryDeadlines) Arm(_ context.Context, matchID string, turn uint32, at time.Time) error {
	d.mu.Lock()
	d.at[dueKey(matchID, turn)] = at.UnixMilli()
	d.mu.Unlock()
	return nil
}

// PopDue claims expired deadlines oldest-first, up to limit.
//
// The ordering is the part worth being careful about. This used to range over
// the map and break once it had enough, and Go randomises map iteration: with
// more expired deadlines than the limit, which matches got auto-played was
// decided by the hash seed rather than by whose clock ran out first. The Redis
// twin pops by score — ZRANGEBYSCORE returns the earliest — so the two
// disagreed under exactly the backlog that makes the order matter, which is the
// kind of divergence between paired implementations this package exists to
// avoid. A player whose turn expired first should be the first one served.
func (d *MemoryDeadlines) PopDue(_ context.Context, now time.Time, limit int) ([]Due, error) {
	cutoff := now.UnixMilli()
	d.mu.Lock()
	defer d.mu.Unlock()

	type expired struct {
		key string
		at  int64
	}
	var ready []expired
	for key, at := range d.at {
		if at <= cutoff {
			ready = append(ready, expired{key: key, at: at})
		}
	}
	// Ties broken by key so a pass is reproducible: two deadlines armed in the
	// same millisecond must not swap places between runs.
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].at != ready[j].at {
			return ready[i].at < ready[j].at
		}
		return ready[i].key < ready[j].key
	})
	if limit > 0 && len(ready) > limit {
		ready = ready[:limit]
	}

	out := make([]Due, 0, len(ready))
	for _, e := range ready {
		// Deleted only for what is actually being handed out. Anything past the
		// limit stays armed and is claimed by the next sweep — dropping it here
		// would lose the deadline outright.
		delete(d.at, e.key)
		if due, ok := parseDue(e.key); ok {
			out = append(out, due)
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------

type RedisDeadlines struct{ rdb *redis.Client }

func NewRedisDeadlines(rdb *redis.Client) *RedisDeadlines { return &RedisDeadlines{rdb: rdb} }

const deadlineKey = "turn:deadlines"

// popDueLua is the same shape as the matchmaking pop: read and remove inside
// one script so no two workers can claim the same expired match.
var popDueLua = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #due > 0 then
  redis.call('ZREM', KEYS[1], unpack(due))
end
return due
`)

// Arm adds a member per turn. An entry for a turn that has since been played is
// left to expire and be rejected on pop rather than chased down and deleted —
// self-cleaning, and it keeps Arm a single round trip.
func (d *RedisDeadlines) Arm(ctx context.Context, matchID string, turn uint32, at time.Time) error {
	return d.rdb.ZAdd(ctx, deadlineKey, redis.Z{
		Score: float64(at.UnixMilli()), Member: dueKey(matchID, turn),
	}).Err()
}

func (d *RedisDeadlines) PopDue(ctx context.Context, now time.Time, limit int) ([]Due, error) {
	if limit <= 0 {
		limit = 64
	}
	res, err := popDueLua.Run(ctx, d.rdb, []string{deadlineKey}, now.UnixMilli(), limit).Result()
	if err != nil {
		return nil, err
	}
	raw, _ := res.([]any)
	out := make([]Due, 0, len(raw))
	for _, v := range raw {
		s, ok := v.(string)
		if !ok {
			continue
		}
		if due, ok := parseDue(s); ok {
			out = append(out, due)
		}
	}
	return out, nil
}
