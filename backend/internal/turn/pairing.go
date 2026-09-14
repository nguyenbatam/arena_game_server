package turn

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultPairTTL bounds how long a parked player stays claimable.
//
// The park survives the node that made it, so nothing local can clean it up
// when that player walks away — a browser tab closed between the two halves of
// a pairing leaves an entry no close handler will ever see. The TTL is the
// backstop, long enough that a slow reconnect still finds its opponent.
const DefaultPairTTL = 2 * time.Minute

// Pairing parks one player until an opponent shows up. The arena's matchmaker
// is overkill for a two-player game, but the parking slot still has to be
// shared: kept per-node, two players who land on different gateways never see
// each other and both sit on a queue screen that never resolves.
//
// Claim is atomic for the same reason every other pop in this repo is. Two
// arrivals racing for one parked player must not both be told they matched
// with them — that is three people in a two-seat game.
type Pairing interface {
	// Claim parks playerID and returns "" when nobody was waiting, or the
	// opponent's id — unparking them — when somebody was.
	Claim(ctx context.Context, playerID string, ttl time.Duration) (string, error)
	// Park puts a player back in the slot, but only if it is free, and reports
	// whether it did.
	//
	// It exists for one caller: a claim that succeeded and then could not be
	// turned into a match. Claim has already consumed the park by then, so the
	// opponent is neither playing nor waiting, and nobody is holding their
	// connection to tell them — they sit on a queue screen that will never
	// resolve. Putting them back is the repair.
	//
	// Claim cannot do this job. Called on a slot that somebody else has taken
	// it hands that person over as an opponent, which is exactly wrong here:
	// this caller has no connection to either of them and would unpark a second
	// player it cannot seat. So the free-slot case is its own operation.
	Park(ctx context.Context, playerID string, ttl time.Duration) (bool, error)
	// Cancel unparks a player, and is a no-op if somebody else is parked.
	Cancel(ctx context.Context, playerID string) error
}

// ---------------------------------------------------------------------------

type MemoryPairing struct {
	mu      sync.Mutex
	waiting string
	until   time.Time
	now     func() time.Time
}

func NewMemoryPairing() *MemoryPairing { return &MemoryPairing{now: time.Now} }

func (p *MemoryPairing) Claim(_ context.Context, playerID string, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		ttl = DefaultPairTTL
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	waiting := p.waiting
	if waiting != "" && p.now().After(p.until) {
		waiting = ""
	}
	// Parking again over yourself is a repeated JOIN, not a match.
	if waiting == "" || waiting == playerID {
		p.waiting, p.until = playerID, p.now().Add(ttl)
		return "", nil
	}
	p.waiting, p.until = "", time.Time{}
	return waiting, nil
}

func (p *MemoryPairing) Park(_ context.Context, playerID string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = DefaultPairTTL
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.waiting != "" && !p.now().After(p.until) && p.waiting != playerID {
		return false, nil
	}
	p.waiting, p.until = playerID, p.now().Add(ttl)
	return true, nil
}

func (p *MemoryPairing) Cancel(_ context.Context, playerID string) error {
	p.mu.Lock()
	if p.waiting == playerID {
		p.waiting, p.until = "", time.Time{}
	}
	p.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------

const pairKey = "turn:waiting"

type RedisPairing struct{ rdb *redis.Client }

func NewRedisPairing(rdb *redis.Client) *RedisPairing { return &RedisPairing{rdb: rdb} }

// claimLua reads and clears the slot in one step, so of two players arriving at
// the same instant exactly one is handed the other.
var claimLua = redis.NewScript(`
local waiting = redis.call('GET', KEYS[1])
if waiting == false or waiting == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
  return ''
end
redis.call('DEL', KEYS[1])
return waiting
`)

// parkLua fills the slot only when it is free, or already holds this player.
// Unlike claimLua it never hands back an occupant: the caller has nobody to
// seat them with. See Pairing.Park.
var parkLua = redis.NewScript(`
local waiting = redis.call('GET', KEYS[1])
if waiting == false or waiting == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
  return 1
end
return 0
`)

// cancelLua clears the slot only if this player is still the one in it. A bare
// DEL would let a departing player evict whoever took the slot after them.
var cancelLua = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
end
return 1
`)

// pairSeconds is the TTL as Redis wants it: whole seconds, never zero, so a
// short one expires rather than being written without an expiry at all.
func pairSeconds(ttl time.Duration) int {
	if ttl <= 0 {
		ttl = DefaultPairTTL
	}
	if secs := int(ttl.Seconds()); secs >= 1 {
		return secs
	}
	return 1
}

func (p *RedisPairing) Claim(ctx context.Context, playerID string, ttl time.Duration) (string, error) {
	res, err := claimLua.Run(ctx, p.rdb, []string{pairKey}, playerID, pairSeconds(ttl)).Result()
	if err != nil {
		return "", err
	}
	s, _ := res.(string)
	return s, nil
}

func (p *RedisPairing) Park(ctx context.Context, playerID string, ttl time.Duration) (bool, error) {
	res, err := parkLua.Run(ctx, p.rdb, []string{pairKey}, playerID, pairSeconds(ttl)).Result()
	if err != nil {
		return false, err
	}
	n, _ := res.(int64)
	return n == 1, nil
}

func (p *RedisPairing) Cancel(ctx context.Context, playerID string) error {
	return cancelLua.Run(ctx, p.rdb, []string{pairKey}, playerID).Err()
}
