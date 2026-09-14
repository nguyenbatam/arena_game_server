// Package rating keeps one number per player and moves it when a match ends.
//
// It exists because matchmaking without it is a lie: the queue was handing
// every player Skill: 1000, so "skill-based" grouping had nothing to group by.
// A rating is the smallest thing that makes a match mean something — it is what
// the queue widens its search around, and what a result is written back into.
//
// Elo rather than Glicko or TrueSkill, deliberately. Elo needs one number per
// player and a dozen lines of arithmetic; the others need a deviation and a
// volatility per player and a good deal more care, and they earn that only once
// there is a real population with real inactivity to model. The shape of the
// plumbing — read before the queue, write after the match — is the same either
// way, so swapping the formula later touches this file and nothing else.
package rating

import (
	"context"
	"errors"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Default is where an unrated player starts.
const Default = 1000

// K is how far one match can move a rating. 32 is the classic chess value:
// fast enough that a new player reaches roughly the right bracket inside a
// handful of matches, slow enough that one bad match is not a demotion.
const K = 32

// Floor stops a losing streak from running a rating into the ground, where the
// numbers stop meaning anything and the player can never be matched with
// anybody. Every ladder has one.
const Floor = 100

// ttl is how long a rating survives without being touched. With no account
// store behind it, this is the whole retention policy: ratings are a cache of
// recent play, not a permanent record. Set PLATFORM_DSN and there is a record —
// internal/platform implements this same Store against it and the gateway uses
// that instead, so nothing here is read and this hash goes cold.
const ttl = 30 * 24 * time.Hour

// Store reads and writes player ratings.
type Store interface {
	// Get returns a player's rating, or Default if they have none.
	Get(ctx context.Context, playerID string) (int, error)
	// GetMany is Get for a whole match, and exists for the same reason Put
	// takes a map: a match ends for everyone in it at the same instant, and one
	// round trip per seat is a round trip per seat. The write side had this
	// from the start and the read side did not, so recording a result cost one
	// sequential lookup per player — on the room's own goroutine, before the
	// room could be counted as finished.
	//
	// The result is positional: one entry per id, in the order they were asked
	// for. An id with no rating behind it reads as Default, which is the same
	// answer Get gives and is never an error — a bot has no rating by
	// construction, and in development neither does a player.
	//
	// On failure it returns a slice of Defaults alongside the error, so a
	// caller that would rather guess than fail can use the values without
	// checking. Get behaves the same way, and for the reason ratingOf gives:
	// guessing costs one match at the wrong skill, failing costs a player who
	// does not get to play.
	GetMany(ctx context.Context, playerIDs []string) ([]int, error)
	// Put writes several ratings at once — a match ends for everyone in it at
	// the same instant, and one round trip per seat is a round trip per seat.
	Put(ctx context.Context, ratings map[string]int) error
}

// defaults is the answer when a store cannot produce one.
func defaults(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = Default
	}
	return out
}

// Update returns the new ratings for one finished match.
//
// Elo is defined for two players, and a deathmatch is not two players. The
// standard generalisation — the one racing games and free-for-all ladders use —
// is to score every pair separately and average: each player plays n-1 notional
// matches, one against each opponent, winning the pairs they out-scored and
// drawing the pairs they tied. Averaging keeps one match worth one match's
// movement regardless of how many people were in it.
//
// Scores are whatever the game counts — kills here — and only their order
// matters, so this does not need to know anything about the game.
func Update(ratings, scores []int) []int {
	n := len(ratings)
	out := make([]int, n)
	copy(out, ratings)
	if n < 2 || len(scores) != n {
		return out
	}
	for i := range ratings {
		delta := 0.0
		for j := range ratings {
			if i == j {
				continue
			}
			actual := 0.5
			switch {
			case scores[i] > scores[j]:
				actual = 1
			case scores[i] < scores[j]:
				actual = 0
			}
			delta += K * (actual - expected(ratings[i], ratings[j]))
		}
		v := out[i] + int(math.Round(delta/float64(n-1)))
		if v < Floor {
			v = Floor
		}
		out[i] = v
	}
	return out
}

// expected is the logistic curve at the heart of Elo: 400 points of gap is
// roughly a 10-to-1 favourite.
func expected(a, b int) float64 {
	return 1 / (1 + math.Pow(10, float64(b-a)/400))
}

type Memory struct {
	mu sync.RWMutex
	m  map[string]int
}

func NewMemory() *Memory { return &Memory{m: map[string]int{}} }

func (s *Memory) Get(_ context.Context, playerID string) (int, error) {
	s.mu.RLock()
	v, ok := s.m[playerID]
	s.mu.RUnlock()
	if !ok {
		return Default, nil
	}
	return v, nil
}

func (s *Memory) GetMany(_ context.Context, playerIDs []string) ([]int, error) {
	out := defaults(len(playerIDs))
	s.mu.RLock()
	for i, id := range playerIDs {
		if v, ok := s.m[id]; ok {
			out[i] = v
		}
	}
	s.mu.RUnlock()
	return out, nil
}

func (s *Memory) Put(_ context.Context, ratings map[string]int) error {
	s.mu.Lock()
	for id, v := range ratings {
		s.m[id] = v
	}
	s.mu.Unlock()
	return nil
}

// Top returns the highest-rated players, for a leaderboard or a test. Memory
// only: the Redis twin would need a sorted set to answer this cheaply, and
// nothing asks it to yet.
func (s *Memory) Top(n int) []string {
	s.mu.RLock()
	ids := make([]string, 0, len(s.m))
	for id := range s.m {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	sort.Slice(ids, func(i, j int) bool {
		if s.m[ids[i]] != s.m[ids[j]] {
			return s.m[ids[i]] > s.m[ids[j]]
		}
		return ids[i] < ids[j]
	})
	if n > 0 && len(ids) > n {
		ids = ids[:n]
	}
	return ids
}

const key = "mm:rating"

type Redis struct{ rdb *redis.Client }

func NewRedis(rdb *redis.Client) *Redis { return &Redis{rdb: rdb} }

func (s *Redis) Get(ctx context.Context, playerID string) (int, error) {
	v, err := s.rdb.HGet(ctx, key, playerID).Int()
	if errors.Is(err, redis.Nil) {
		return Default, nil
	}
	if err != nil {
		// An unreachable rating store must not stop anyone queueing: the cost
		// of guessing is one match at the wrong skill, and the cost of failing
		// is a player who cannot play at all.
		return Default, err
	}
	return v, nil
}

// GetMany is one HMGET rather than a pipeline of HGETs. Both are a single round
// trip, but HMGET is also a single command: the hash is read once, and a Redis
// that is doing anything else is not asked to schedule eight of them.
//
// Redis returns nil for a field that is not set, and a field that is set to
// something unparseable is treated the same way — a rating store is not worth
// failing a match write over, and Default is the answer Get already gives.
func (s *Redis) GetMany(ctx context.Context, playerIDs []string) ([]int, error) {
	out := defaults(len(playerIDs))
	if len(playerIDs) == 0 {
		return out, nil
	}
	vals, err := s.rdb.HMGet(ctx, key, playerIDs...).Result()
	if err != nil {
		return out, err
	}
	for i := range out {
		if i >= len(vals) {
			break
		}
		str, ok := vals[i].(string)
		if !ok {
			continue
		}
		if v, convErr := strconv.Atoi(str); convErr == nil {
			out[i] = v
		}
	}
	return out, nil
}

func (s *Redis) Put(ctx context.Context, ratings map[string]int) error {
	if len(ratings) == 0 {
		return nil
	}
	pairs := make([]any, 0, len(ratings)*2)
	for id, v := range ratings {
		pairs = append(pairs, id, strconv.Itoa(v))
	}
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, key, pairs...)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}
