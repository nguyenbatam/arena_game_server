// Package platform is the half of a game backend that is not realtime.
//
// The rest of this repo is the realtime tier: a connection, a queue, a tick, a
// snapshot. Nothing in it outlives the process, and nothing is supposed to.
// A room dies with the node that ran it, presence expires in 45 seconds and the
// Elo hash is a 30-day cache. That is correct for those things — none of them
// is a record of anything.
//
// An account is. So is what a player paid for, what they were awarded, and
// where they stand on a ladder. Those cannot expire, cannot be recomputed from
// the world, and cannot be lost when a pod restarts, which rules out every
// store the realtime tier uses. This package is where they live: one relational
// schema, one transaction per fact, one durable identity that the gateway then
// carries around in a token.
//
// # The seam
//
// The realtime tier touches this package in exactly three places, and that is
// the point of drawing it as a package rather than sprinkling SQL through the
// gateway:
//
//   - login mints a token for a durable account id instead of a fresh random
//     one, so the same person is the same player tomorrow;
//   - the matchmaker reads that account's rating on the way into the queue;
//   - a finished match is written back — rating, record, currency, history — in
//     one transaction on the way out.
//
// Everything else here is served over HTTP off the tick path entirely.
//
// # Idempotency
//
// Every write is retryable, because every caller can crash between sending a
// request and learning what happened to it. Two kinds of key do that work:
//
//   - A client-chosen key on a purchase, scoped to the account. Scoping is not
//     optional — clients pick keys independently, so stored bare one player's
//     key collides with another's and the second player's purchase is swallowed
//     as a duplicate. internal/turn learned this the same way.
//   - The server-chosen match id on a result. The gateway that ends a match can
//     die mid-write, the job can be redelivered, and a turn match can be ended
//     by a matchmaker that then retries. Recording is keyed on
//     (match_id, account_id) so the second attempt writes nothing and says so.
//
// # Why not Redis
//
// The realtime tier already has Redis and it would hold all of this. It should
// not. Every structure in there carries a TTL because everything in there is
// derived — and an account is not derived from anything. The properties this
// needs are the ones a relational store is for: a transaction spanning the
// wallet and the inventory, a uniqueness constraint on a username, a check
// constraint that a balance cannot go negative, and an index that answers a
// leaderboard without walking the player base.
package platform

import (
	"errors"
	"time"
)

var (
	// ErrInvalid marks a caller's mistake rather than the system's — a username
	// that cannot be a key, a password too short to be one, a missing
	// idempotency key. Every validation error wraps it, which is what lets the
	// HTTP layer answer 400 without matching on message text or maintaining a
	// second list of which errors are the client's fault.
	ErrInvalid = errors.New("invalid request")
	// ErrUsernameTaken is the uniqueness constraint reported as an error rather
	// than as a 500. Registration races are ordinary, not exceptional.
	ErrUsernameTaken = errors.New("platform: username taken")
	ErrNoAccount     = errors.New("platform: no such account")
	// ErrBadCredentials covers both an unknown username and a wrong password,
	// deliberately: telling them apart tells an attacker which usernames exist.
	ErrBadCredentials = errors.New("platform: bad credentials")
	ErrNoItem         = errors.New("platform: no such item")
	// ErrInsufficientFunds is returned instead of writing a negative balance.
	// The check is part of the same UPDATE that spends, never a read followed
	// by a write — two clients spending the last coin at once is a race the
	// application layer cannot win.
	ErrInsufficientFunds = errors.New("platform: insufficient funds")
	// ErrItemLimit is a unique cosmetic bought twice.
	ErrItemLimit = errors.New("platform: already at the limit for this item")
	// ErrBadDSN is a connection string that could not be parsed, as opposed to
	// one that could not be reached. The difference is the whole reason it is a
	// sentinel: a database that is not up yet is worth waiting for, and a typo
	// never will be. A caller that retries both spends its startup budget on a
	// string that was never going to work, then reports the timeout instead of
	// the typo.
	ErrBadDSN = errors.New("platform: malformed connection string")
)

// Account is one durable identity. ID is what goes into a JWT and therefore
// what every other package in the repo sees as the player id.
type Account struct {
	ID          string    `json:"id"`
	Username    string    `json:"username"`
	DisplayName string    `json:"display_name"`
	CreatedAt   time.Time `json:"created_at"`
	// omitzero rather than a zero timestamp: an account that has never logged
	// in has no last-login time, and "0001-01-01T00:00:00Z" is a date a client
	// has to know to special-case.
	LastLoginAt time.Time `json:"last_login_at,omitzero"`
}

// Credential is a password at rest: a salted PBKDF2 digest plus the cost it was
// computed at.
//
// The cost is stored per row rather than fixed in code on purpose. It is the
// one parameter that has to change over a schema's life — what is expensive
// enough today is cheap in five years — and a column makes that a rehash on
// next login instead of a migration that cannot read the old rows.
type Credential struct {
	Hash  []byte
	Salt  []byte
	Iters int
}

// Profile is the mutable half of an account: what play has done to it.
type Profile struct {
	AccountID string `json:"account_id"`
	Rating    int    `json:"rating"`
	Matches   int    `json:"matches"`
	Wins      int    `json:"wins"`
	Kills     int    `json:"kills"`
	Currency  int64  `json:"currency"`
}

// Stack is one line of an inventory.
type Stack struct {
	ItemID     string    `json:"item_id"`
	Qty        int       `json:"qty"`
	AcquiredAt time.Time `json:"acquired_at"`
}

// Entry is one movement of currency, items, or both, applied exactly once per
// (AccountID, Key).
//
// Grant and purchase are the same operation with the sign of Delta flipped, so
// they are one primitive rather than two code paths that have to be kept
// honest about the same transaction boundary.
type Entry struct {
	AccountID string
	// Key is the idempotency key, scoped to AccountID. A purchase carries one
	// the client chose; a grant carries one the server did.
	Key    string
	Kind   string // "purchase" | "grant"
	ItemID string // empty for a pure currency movement
	Qty    int
	// Delta is the currency movement, signed: a purchase is negative. The store
	// refuses the whole entry rather than writing a negative balance.
	Delta int64
	// Max caps how many of ItemID one account may hold. Zero means unlimited;
	// a unique cosmetic sets 1, and a second purchase fails with ErrItemLimit
	// rather than silently stacking.
	Max int
	At  time.Time
}

// Receipt is what an Entry did.
type Receipt struct {
	Profile Profile `json:"profile"`
	Item    Stack   `json:"item"`
	// Replay is true when this key had already been applied and nothing moved.
	// The fields above are then the current state rather than a replay of the
	// original response — enough for a client to reconcile, and a good deal
	// cheaper than storing every response body for the life of the account.
	Replay bool `json:"replay"`
}

// Result is one player's line in a finished match.
type Result struct {
	AccountID string
	Score     int
	// Placement is 1 for the winner. Draws share a placement.
	Placement    int
	RatingBefore int
	RatingAfter  int
}

// Report is a finished match as the platform records it: mode-agnostic on
// purpose, because the arena and the turn-based game agree on exactly this much
// and nothing else.
type Report struct {
	MatchID string
	Mode    string // "arena" | "turn"
	EndedAt time.Time
	Results []Result
}

// MatchRow is one recorded line of match history.
type MatchRow struct {
	MatchID      string    `json:"match_id"`
	AccountID    string    `json:"account_id"`
	Mode         string    `json:"mode"`
	Score        int       `json:"score"`
	Placement    int       `json:"placement"`
	RatingBefore int       `json:"rating_before"`
	RatingAfter  int       `json:"rating_after"`
	Reward       int64     `json:"reward"`
	EndedAt      time.Time `json:"ended_at"`
}

// Won reports whether this line is a win, which is the only thing the profile
// counters need from a placement.
func (r MatchRow) Won() bool { return r.Placement == 1 }

// RatingDelta is how far this match moved the player, and it is what the store
// applies — relatively, never by assignment.
//
// Two reasons, and the second is the one that matters. A rating written as
// "SET rating = 1016" is a read-modify-write with the read done somewhere else
// entirely: the number was read when the match started and is applied when it
// ends, minutes later, so a second match that finished in between is silently
// undone. Adding a delta composes instead. And it lets a mode that does not
// rank — the turn-based game, which has no ladder of its own — record a match
// with a delta of zero and leave the arena's ladder alone, rather than writing
// back a number it read for no reason.
func (r MatchRow) RatingDelta() int { return r.RatingAfter - r.RatingBefore }

// Rank is one row of a leaderboard.
type Rank struct {
	Rank        int    `json:"rank"`
	AccountID   string `json:"account_id"`
	DisplayName string `json:"display_name"`
	Rating      int    `json:"rating"`
	Matches     int    `json:"matches"`
	Wins        int    `json:"wins"`
}
