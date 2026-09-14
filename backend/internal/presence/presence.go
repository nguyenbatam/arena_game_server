package presence

import (
	"context"
	"errors"
	"sync"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// OnlineTTL and DisconnectTTL are how long a presence record survives.
//
// A record is a cache of where somebody was, not a fact about them, so both are
// short. The disconnected one is the longer of the two because it is the one a
// reconnect reads: it has to outlive the grace window, or the window is
// decided by this number rather than by DISCONNECT_GRACE — which is a knob that
// would then quietly do nothing above two minutes. config.MaxDisconnectGrace
// holds the grace below it, and TestDisconnectTTLCoversTheConfiguredGrace pins
// the two together.
const (
	OnlineTTL     = 45 * time.Second
	DisconnectTTL = 2 * time.Minute
)

// ttlFor is the expiry a record should carry, in one place so the two stores
// cannot drift. A difference between paired implementations only ever surfaces
// in production, and this one would surface as reconnects that work in
// development and not in the fleet.
func ttlFor(s *pb.Presence) time.Duration {
	if s.Status == pb.PresenceStatus_PRESENCE_STATUS_DISCONNECTED {
		return DisconnectTTL
	}
	return OnlineTTL
}

type Store interface {
	Set(ctx context.Context, s *pb.Presence) error
	Get(ctx context.Context, id string) (*pb.Presence, error)
	Delete(ctx context.Context, id string) error
}

type Memory struct {
	mu sync.Mutex
	m  map[string]record
	// now is swappable so a test can age records without sleeping.
	now func() time.Time
	// writes counts sets since the last sweep. See evictExpired.
	writes int
}

// record is a presence plus the moment it stops counting.
type record struct {
	p         *pb.Presence
	expiresAt time.Time
}

func NewMemory() *Memory { return &Memory{m: map[string]record{}, now: time.Now} }

// sweepEvery and sweepBatch amortise expiry over writes, the shape
// turn.MemoryStore and ratelimit.Window already use — and with the same
// invariant: each write admits at most one record, so a sweep must inspect
// strictly more than one entry per write or the map grows anyway. At 32 per 8
// writes the margin is 4x.
const (
	sweepEvery = 8
	sweepBatch = 32
)

// evictExpired drops records whose time is up.
//
// The memory store used to have no expiry at all, which made it two things at
// once: a leak, and a disagreement with its Redis twin. Every player who
// dropped out of a match left a DISCONNECTED record that nothing ever removed —
// onClose deletes only the records of players who were not in one — so a
// process running a busy fleet accumulated one entry per player who ever
// disconnected, for its whole life. And because Redis expires the same record
// in two minutes, a reconnect hours later was greeted as a returning player in
// development and as a new arrival in production.
//
// Bounded and amortised rather than a janitor goroutine: no lifecycle to
// manage, nothing for goleak to find, and Go's randomised map iteration means
// no entry can hide behind the batch limit. Anything a sweep misses is still
// refused by Get, which checks the expiry it reads.
func (s *Memory) evictExpired() {
	s.writes++
	if s.writes < sweepEvery {
		return
	}
	s.writes = 0
	now := s.now()
	seen := 0
	for id, rec := range s.m {
		if now.After(rec.expiresAt) {
			delete(s.m, id)
		}
		if seen++; seen >= sweepBatch {
			return
		}
	}
}

// stamp fills in Seen when the caller did not supply one. The grace window is
// measured from this field, so a caller that knows when the event actually
// happened — a replay, a handover, a test aging a record past the window — must
// be able to say so rather than have every write reset the clock to now.
func stamp(sess *pb.Presence, now time.Time) {
	if sess.Seen == 0 {
		sess.Seen = now.Unix()
	}
}

func (s *Memory) Set(_ context.Context, sess *pb.Presence) error {
	cp := proto.Clone(sess).(*pb.Presence)
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	stamp(cp, now)
	// Measured from the write, not from Seen: that is what Redis does with a
	// SET plus an expiry, and Seen is deliberately allowed to name an older
	// moment than the write that carried it.
	s.m[cp.PlayerId] = record{p: cp, expiresAt: now.Add(ttlFor(cp))}
	s.evictExpired()
	return nil
}

func (s *Memory) Get(_ context.Context, id string) (*pb.Presence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.m[id]
	if !ok {
		return nil, nil
	}
	if s.now().After(rec.expiresAt) {
		// Reclaimed on the look-up that found it, so a record the bounded sweep
		// has not reached is still never served.
		delete(s.m, id)
		return nil, nil
	}
	return proto.Clone(rec.p).(*pb.Presence), nil
}

func (s *Memory) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	delete(s.m, id)
	s.mu.Unlock()
	return nil
}

type Redis struct {
	rdb *redis.Client
}

func NewRedis(rdb *redis.Client) *Redis { return &Redis{rdb: rdb} }

func (s *Redis) key(id string) string { return "presence:" + id }

func (s *Redis) Set(ctx context.Context, sess *pb.Presence) error {
	// Stamped on a copy: the caller reuses its Presence and is not expecting
	// this call to write into it. The memory twin already worked this way.
	cp := proto.Clone(sess).(*pb.Presence)
	stamp(cp, time.Now())
	b, err := proto.Marshal(cp)
	if err != nil {
		return err
	}
	ttl := ttlFor(cp)
	// One key, one command. There used to be a "presence:online" set maintained
	// alongside it — SADD on every online write, SREM on disconnect — and it
	// was a leak with nothing on the other end of it: no code in this repo ever
	// read the set, and set members carry no TTL of their own, so every record
	// whose key expired without a clean disconnect (a node killed, a gateway
	// that crashed) left its id in there for the life of the Redis instance.
	//
	// The expiring key is the whole answer to "is this player around": it is
	// what Get reads and what every caller already trusts. A real online roster
	// would want a sorted set scored by last-seen and trimmed on read, which is
	// self-cleaning — not a plain set that only shrinks when someone remembers
	// to remove from it.
	return s.rdb.Set(ctx, s.key(cp.PlayerId), b, ttl).Err()
}

func (s *Redis) Get(ctx context.Context, id string) (*pb.Presence, error) {
	b, err := s.rdb.Get(ctx, s.key(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var p pb.Presence
	if err := proto.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Redis) Delete(ctx context.Context, id string) error {
	return s.rdb.Del(ctx, s.key(id)).Err()
}
