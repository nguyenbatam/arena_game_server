package platform

import (
	"context"
	"errors"

	"github.com/nguyenbatam/arena_game_server/internal/rating"
)

// Ratings adapts the platform to rating.Store, which is the interface the
// matchmaker already reads through.
//
// This is the seam the README promised and then did not use: "rating.Store is
// an interface with two implementations, and a third backed by a real database
// changes one constructor." This is the third, and it is one constructor.
//
// Get is the only method with a cost worth stating: it runs on every queue
// join, so skill-based matchmaking now costs one primary-key read per join
// instead of one Redis HGET. That is off the tick path and inside the same
// deadline every other coordination call carries, and a queue join is not a
// frequent event per player. It would still be the first thing to cache if the
// join rate ever justified it — Redis in front, the database as the record —
// and the shape of that cache is exactly the rating.Store this replaces.
type Ratings struct{ store Store }

// Ratings returns the rating.Store view of this service.
func (s *Service) Ratings() *Ratings { return &Ratings{store: s.store} }

var _ rating.Store = (*Ratings)(nil)

// Get returns the account's rating, or rating.Default for an id with no account
// behind it.
//
// An unknown id is ordinary rather than exceptional: with ENV=development the
// gateway accepts a hello carrying no token at all, and a bot filling out a
// room has no account by construction. Neither should be a reason a queue join
// fails — matchmaking's own fallback says the same thing, that guessing a
// rating costs one match at the wrong skill and failing costs a player who does
// not get to play.
func (r *Ratings) Get(ctx context.Context, playerID string) (int, error) {
	v, err := r.store.Rating(ctx, playerID)
	if errors.Is(err, ErrNoAccount) {
		return rating.Default, nil
	}
	if err != nil {
		return rating.Default, err
	}
	return v, nil
}

// GetMany reads a whole match's ratings in one statement.
//
// The single-row Get runs on every queue join, which is one player at a time
// and is why that one is a primary-key read. This runs when a match ends, where
// the caller has every seat in hand at once — so the choice is one statement or
// one per player, and the per-player version was on the room's own goroutine
// holding the room open until the last of them came back.
//
// Ids with no account behind them are filled with rating.Default rather than
// refused, the same answer Get gives and for the same reason: a bot has no
// account by construction, and neither does a development session.
func (r *Ratings) GetMany(ctx context.Context, playerIDs []string) ([]int, error) {
	out := make([]int, len(playerIDs))
	for i := range out {
		out[i] = rating.Default
	}
	if len(playerIDs) == 0 {
		return out, nil
	}
	found, err := r.store.RatingsFor(ctx, playerIDs)
	if err != nil {
		return out, err
	}
	for i, id := range playerIDs {
		if v, ok := found[id]; ok {
			out[i] = v
		}
	}
	return out, nil
}

// Put exists to satisfy the interface and deliberately does nothing.
//
// Ratings are written by RecordMatch, in the same transaction as the record,
// the reward and the history line, because they are one fact about one match.
// A second writer that could move a rating on its own would be a way for the
// number and the history explaining it to drift apart — so the write path is
// closed here rather than left open and merely unused.
func (r *Ratings) Put(context.Context, map[string]int) error { return nil }
