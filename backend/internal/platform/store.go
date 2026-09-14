package platform

import (
	"context"
	"time"
)

// Store is the persistence seam.
//
// Two implementations, the same rule the rest of the repo follows for Redis:
// Memory for tests and for a demo somebody wants to run without a database, and
// Postgres for anything that has to survive the process. Behavioural
// differences between the two only ever surface in production, so store_test.go
// runs one suite against both — see TestStoreConformance.
//
// The methods are deliberately coarse. There is no BeginTx here and no way for
// a caller to compose two of these into one transaction, because a caller that
// could do that would eventually do it across a network boundary and get a
// half-written wallet. Every method below is one atomic fact.
type Store interface {
	// CreateAccount fails with ErrUsernameTaken rather than overwriting.
	CreateAccount(ctx context.Context, a Account, c Credential) error
	// AccountByUsername is the login path; the username is matched
	// case-insensitively, since a display name is not a key and a key should
	// not depend on shift.
	AccountByUsername(ctx context.Context, username string) (Account, Credential, error)
	AccountByID(ctx context.Context, id string) (Account, error)
	// NoteLogin stamps last_login_at. It is not part of the login transaction:
	// a write failure here must not cost somebody their session.
	NoteLogin(ctx context.Context, id string, at time.Time) error

	Profile(ctx context.Context, accountID string) (Profile, error)
	// Rating is the single-column read the matchmaker does on every queue join,
	// split out from Profile so that path costs one indexed lookup rather than
	// a row it throws away. ErrNoAccount for an id with no account behind it —
	// which happens in development, where the gateway accepts a hello with no
	// token at all.
	Rating(ctx context.Context, accountID string) (int, error)
	// RatingsFor is Rating for a whole match, and answers with a map rather
	// than a positional slice because the caller's ids are not all accounts:
	// bots and unauthenticated development sessions have no row, and an absent
	// key says so without needing a sentinel. It is not an error for an id to
	// be missing — the single-row Rating returns ErrNoAccount because a caller
	// asking about one player wants to know, and a caller recording a match
	// does not.
	//
	// Recording a result used to read these one at a time, which is a round
	// trip per seat against a database, run on the room's own goroutine before
	// the room could be counted as finished.
	RatingsFor(ctx context.Context, accountIDs []string) (map[string]int, error)

	// Inventory and History answer for an id with no account behind it with an
	// empty list rather than ErrNoAccount. A list endpoint returning a list is
	// the simpler contract, and the alternative costs an existence query on
	// every call to say something the caller's Profile read already said.
	Inventory(ctx context.Context, accountID string) ([]Stack, error)
	// Apply moves currency and items together, at most once per (account, key).
	// A replayed key reports Receipt.Replay and moves nothing.
	Apply(ctx context.Context, e Entry) (Receipt, error)

	// RecordMatch writes one finished match for every player in it and returns
	// how many lines were new. Keyed on (match_id, account_id): a redelivered
	// job, a retried write or a matchmaker finishing a match a gateway already
	// finished writes nothing the second time and returns 0.
	//
	// Partial redelivery is the case worth stating, because it is the one a
	// naive "have we seen this match?" guard gets wrong: a crash halfway
	// through can leave some players recorded and some not, and the retry has
	// to write exactly the missing ones.
	RecordMatch(ctx context.Context, rows []MatchRow) (int, error)
	History(ctx context.Context, accountID string, limit int) ([]MatchRow, error)
	// Leaderboard ranks only accounts that have played. Ties break by account
	// id so the order is total — a board that reshuffles equal ratings between
	// two reads shows a paginating client the same row twice and skips another.
	Leaderboard(ctx context.Context, limit int) ([]Rank, error)

	Close() error
}
