package platform

import (
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode"
)

// DefaultIters is the PBKDF2-HMAC-SHA256 work factor.
//
// OWASP's current floor for this construction is 600k. Nothing here is holding
// a real password yet, and a demo whose login takes a third of a second on a
// laptop is a demo nobody finishes, so this sits an order below it — and says
// so out loud rather than pretending the number was chosen for security. The
// cost is stored per credential, so raising it is a rehash on next login.
//
// PBKDF2 at all, rather than argon2id or bcrypt, because it is in the standard
// library as of Go 1.24 and the alternatives are a dependency. That trade is
// only defensible at demo scale; a product picks argon2id and pays the module.
const DefaultIters = 64_000

const (
	saltLen = 16
	keyLen  = 32
)

// Service is the platform's business logic: everything that has to be true
// before a Store call, and everything that has to be decided after one.
type Service struct {
	store   Store
	catalog Catalog
	rewards Rewards
	iters   int
	now     func() time.Time
	newID   func() (string, error)
}

type Options struct {
	Store   Store
	Catalog Catalog
	Rewards Rewards
	// Iters overrides DefaultIters. Tests set it low: the conformance suite
	// registers a few dozen accounts, and at the real cost that is most of the
	// run time of the package.
	Iters int
	Now   func() time.Time
	NewID func() (string, error)
}

func NewService(o Options) *Service {
	s := &Service{
		store: o.Store, catalog: o.Catalog, rewards: o.Rewards,
		iters: o.Iters, now: o.Now, newID: o.NewID,
	}
	if s.catalog == nil {
		s.catalog = DefaultCatalog()
	}
	if s.rewards == (Rewards{}) {
		s.rewards = DefaultRewards
	}
	if s.iters <= 0 {
		s.iters = DefaultIters
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.newID == nil {
		s.newID = NewAccountID
	}
	return s
}

func (s *Service) Catalog() Catalog { return s.catalog }
func (s *Service) Close() error     { return s.store.Close() }

// NewAccountID mints a durable player id.
//
// The "a-" prefix is not decoration. Guest tokens from the no-database path
// carry "p-" ids, and the two are not the same kind of thing: one is an
// identity that will still mean this person next month, the other is a label on
// a session. They end up in the same field of the same token and the same
// string column, so the only place the difference can be visible is the id
// itself — and it needs to be visible, because a row written against a "p-" id
// is a row nobody can ever claim.
func NewAccountID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "a-" + hex.EncodeToString(b[:]), nil
}

// Register creates an account and returns it.
func (s *Service) Register(ctx context.Context, username, password, displayName string) (Account, error) {
	username = strings.TrimSpace(username)
	if err := ValidUsername(username); err != nil {
		return Account{}, err
	}
	if err := ValidPassword(password); err != nil {
		return Account{}, err
	}
	displayName = strings.TrimSpace(displayName)
	if displayName == "" {
		displayName = username
	}
	if len([]rune(displayName)) > 32 {
		return Account{}, fmt.Errorf("%w: display name may be at most 32 characters", ErrInvalid)
	}
	id, err := s.newID()
	if err != nil {
		return Account{}, err
	}
	cred, err := s.hash(password)
	if err != nil {
		return Account{}, err
	}
	a := Account{
		ID: id, Username: username, DisplayName: displayName,
		CreatedAt: s.now().UTC(),
	}
	if err := s.store.CreateAccount(ctx, a, cred); err != nil {
		return Account{}, err
	}
	return a, nil
}

// Login verifies a password and returns the account behind it.
//
// An unknown username costs the same work as a wrong password, because it has
// to: skipping the hash on a missing row turns login latency into an oracle for
// which usernames exist, which is how account lists get enumerated.
func (s *Service) Login(ctx context.Context, username, password string) (Account, error) {
	a, cred, err := s.store.AccountByUsername(ctx, strings.TrimSpace(username))
	if errors.Is(err, ErrNoAccount) {
		s.dummyHash(password)
		return Account{}, ErrBadCredentials
	}
	if err != nil {
		return Account{}, err
	}
	if !verify(password, cred) {
		return Account{}, ErrBadCredentials
	}
	// Outside the check on purpose: a failed stamp must not cost a valid login.
	if err := s.store.NoteLogin(ctx, a.ID, s.now().UTC()); err == nil {
		a.LastLoginAt = s.now().UTC()
	}
	return a, nil
}

func (s *Service) Account(ctx context.Context, id string) (Account, error) {
	return s.store.AccountByID(ctx, id)
}

func (s *Service) Profile(ctx context.Context, id string) (Profile, error) {
	return s.store.Profile(ctx, id)
}

func (s *Service) Inventory(ctx context.Context, id string) ([]Stack, error) {
	return s.store.Inventory(ctx, id)
}

func (s *Service) History(ctx context.Context, id string, limit int) ([]MatchRow, error) {
	return s.store.History(ctx, id, limit)
}

func (s *Service) Leaderboard(ctx context.Context, limit int) ([]Rank, error) {
	return s.store.Leaderboard(ctx, limit)
}

// Buy spends currency on an item.
//
// key is chosen by the client and scoped to the account by the store. A client
// that loses the response and retries must not be charged twice, and it has no
// other way to find out whether the first attempt landed — the balance it would
// compare against may have moved for half a dozen other reasons.
func (s *Service) Buy(ctx context.Context, accountID, itemID string, qty int, key string) (Receipt, error) {
	item, ok := s.catalog[itemID]
	if !ok {
		return Receipt{}, ErrNoItem
	}
	if qty <= 0 {
		qty = 1
	}
	if qty > 99 {
		return Receipt{}, fmt.Errorf("%w: quantity may be at most 99", ErrInvalid)
	}
	if key = strings.TrimSpace(key); key == "" {
		return Receipt{}, fmt.Errorf("%w: idempotency key required", ErrInvalid)
	}
	if len(key) > 64 {
		return Receipt{}, fmt.Errorf("%w: idempotency key may be at most 64 characters", ErrInvalid)
	}
	return s.store.Apply(ctx, Entry{
		AccountID: accountID, Key: key, Kind: "purchase",
		ItemID: itemID, Qty: qty, Delta: -item.Price * int64(qty),
		Max: item.Max, At: s.now().UTC(),
	})
}

// Grant hands an account currency or an item with no payment, for an award the
// server decides. Same primitive as Buy with the sign flipped; the key is the
// server's, so it is stable across retries by construction.
func (s *Service) Grant(ctx context.Context, accountID, itemID string, qty int, currency int64, key string) (Receipt, error) {
	max := 0
	if itemID != "" {
		item, ok := s.catalog[itemID]
		if !ok {
			return Receipt{}, ErrNoItem
		}
		max = item.Max
	}
	return s.store.Apply(ctx, Entry{
		AccountID: accountID, Key: key, Kind: "grant",
		ItemID: itemID, Qty: qty, Delta: currency, Max: max, At: s.now().UTC(),
	})
}

// RecordMatch writes a finished match: the new ratings, the record, the reward
// and the history line, for every player, in one transaction.
//
// One call rather than four is the whole point. A rating written without the
// history line is a number a player cannot account for; a reward paid without
// the rating is a wallet that moved for no visible reason. They are one fact
// and the store writes them as one — which is also what makes the retry safe,
// since there is exactly one key to be idempotent on.
//
// Returns how many player lines were newly written; 0 means the match had
// already been recorded, which is not an error.
func (s *Service) RecordMatch(ctx context.Context, rep Report) (int, error) {
	if rep.MatchID == "" || len(rep.Results) == 0 {
		return 0, nil
	}
	at := rep.EndedAt
	if at.IsZero() {
		at = s.now()
	}
	at = at.UTC()
	rows := make([]MatchRow, 0, len(rep.Results))
	for _, r := range rep.Results {
		if r.AccountID == "" {
			continue
		}
		rows = append(rows, MatchRow{
			MatchID: rep.MatchID, AccountID: r.AccountID, Mode: rep.Mode,
			Score: r.Score, Placement: r.Placement,
			RatingBefore: r.RatingBefore, RatingAfter: r.RatingAfter,
			Reward: s.rewards.Payout(r), EndedAt: at,
		})
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return s.store.RecordMatch(ctx, rows)
}

func (s *Service) hash(password string) (Credential, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return Credential{}, err
	}
	sum, err := pbkdf2.Key(sha256.New, password, salt, s.iters, keyLen)
	if err != nil {
		return Credential{}, err
	}
	return Credential{Hash: sum, Salt: salt, Iters: s.iters}, nil
}

// dummyHash burns the work a real verification would have cost. See Login.
//
// The derived key is thrown away — the point is the elapsed time, so that a
// login for an account that does not exist takes as long as one for an account
// that does and the endpoint cannot be used to enumerate usernames. An error
// would mean the work did not happen, which is the one outcome that defeats it:
// the reply comes back early and the timing difference this exists to erase is
// back. There is nothing to return it to, so it is logged.
func (s *Service) dummyHash(password string) {
	var salt [saltLen]byte
	if _, err := pbkdf2.Key(sha256.New, password, salt[:], s.iters, keyLen); err != nil {
		log.Printf("platform: dummy hash: %v (login timing is observable)", err)
	}
}

func verify(password string, c Credential) bool {
	if c.Iters <= 0 || len(c.Hash) == 0 {
		return false
	}
	sum, err := pbkdf2.Key(sha256.New, password, c.Salt, c.Iters, len(c.Hash))
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(sum, c.Hash) == 1
}

// ValidUsername keeps the key narrow on purpose.
//
// It is matched case-insensitively and it is what somebody types to log in, so
// anything that renders two different strings identically is a way to be
// mistaken for someone else. Restricting to ASCII letters, digits and three
// separators is the cheap version of that argument; a product that wants
// Unicode names wants NFKC normalisation and a confusable-skeleton check, and
// it wants the display name to carry the personality instead.
func ValidUsername(u string) error {
	if n := len(u); n < 3 || n > 32 {
		return fmt.Errorf("%w: username must be 3-32 characters", ErrInvalid)
	}
	for _, r := range u {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_' || r == '-' || r == '.':
		default:
			return fmt.Errorf("%w: username may contain letters, digits and _-. only", ErrInvalid)
		}
	}
	return nil
}

// ValidPassword checks length and nothing else. Composition rules ("one digit,
// one symbol") are known not to work: they shrink the search space a user
// actually explores and they are why every password is Password1!. Length is
// the property that matters, and the real answer beyond it is a breach-list
// check, which is a service call this demo does not have.
func ValidPassword(p string) error {
	if len([]rune(p)) < 8 {
		return fmt.Errorf("%w: password must be at least 8 characters", ErrInvalid)
	}
	if len(p) > 256 {
		// PBKDF2 will hash anything, but an unbounded body is an unbounded
		// amount of work an anonymous caller can ask for.
		return fmt.Errorf("%w: password may be at most 256 bytes", ErrInvalid)
	}
	for _, r := range p {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: password may not contain control characters", ErrInvalid)
		}
	}
	return nil
}

// storeVerify is verify as a method, so a test can show that a credential
// written at one work factor still verifies against a service configured with
// another. The cost that matters is the one in the row, not the one in the
// constant.
func (s *Service) storeVerify(password string, c Credential) bool { return verify(password, c) }
