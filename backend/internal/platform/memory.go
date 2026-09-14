package platform

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/rating"
)

// Memory is the Store with no database behind it.
//
// It exists for the same reason every other store in this repo has a memory
// twin: the demo has to come up from a clone with nothing installed, and the
// tests have to run in CI without a container. It is a real implementation, not
// a stub — it enforces the same uniqueness, the same idempotency and the same
// refusal to overdraw a wallet, because a twin that is more permissive than the
// real store is a test that passes and a deployment that does not.
//
// What it is not is durable. Everything here dies with the process, which makes
// it wrong for every purpose this package exists to serve.
type Memory struct {
	mu        sync.RWMutex
	accounts  map[string]*Account
	creds     map[string]Credential
	byName    map[string]string // lowercased username -> account id
	profiles  map[string]*Profile
	inventory map[string]map[string]*Stack
	ledger    map[string]bool // account id + "\x00" + key
	matches   map[string]bool // match id + "\x00" + account id
	history   map[string][]MatchRow
}

func NewMemory() *Memory {
	return &Memory{
		accounts:  map[string]*Account{},
		creds:     map[string]Credential{},
		byName:    map[string]string{},
		profiles:  map[string]*Profile{},
		inventory: map[string]map[string]*Stack{},
		ledger:    map[string]bool{},
		matches:   map[string]bool{},
		history:   map[string][]MatchRow{},
	}
}

func (s *Memory) Close() error { return nil }

func (s *Memory) CreateAccount(_ context.Context, a Account, c Credential) error {
	key := strings.ToLower(a.Username)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byName[key]; ok {
		return ErrUsernameTaken
	}
	cp := a
	s.accounts[a.ID] = &cp
	s.creds[a.ID] = c
	s.byName[key] = a.ID
	// One account, one profile, written together. Postgres does this in the
	// same transaction; doing it in two calls here would let a test see an
	// account with no profile, which the real store can never produce.
	s.profiles[a.ID] = &Profile{AccountID: a.ID, Rating: StartingRating, Currency: StartingCurrency}
	return nil
}

func (s *Memory) AccountByUsername(_ context.Context, username string) (Account, Credential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.byName[strings.ToLower(username)]
	if !ok {
		return Account{}, Credential{}, ErrNoAccount
	}
	return *s.accounts[id], s.creds[id], nil
}

func (s *Memory) AccountByID(_ context.Context, id string) (Account, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.accounts[id]
	if !ok {
		return Account{}, ErrNoAccount
	}
	return *a, nil
}

func (s *Memory) NoteLogin(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.accounts[id]
	if !ok {
		return ErrNoAccount
	}
	a.LastLoginAt = at
	return nil
}

func (s *Memory) Profile(_ context.Context, accountID string) (Profile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.profiles[accountID]
	if !ok {
		return Profile{}, ErrNoAccount
	}
	return *p, nil
}

func (s *Memory) Rating(_ context.Context, accountID string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.profiles[accountID]
	if !ok {
		return 0, ErrNoAccount
	}
	return p.Rating, nil
}

func (s *Memory) RatingsFor(_ context.Context, accountIDs []string) (map[string]int, error) {
	out := make(map[string]int, len(accountIDs))
	s.mu.RLock()
	for _, id := range accountIDs {
		if p, ok := s.profiles[id]; ok {
			out[id] = p.Rating
		}
	}
	s.mu.RUnlock()
	return out, nil
}

func (s *Memory) Inventory(_ context.Context, accountID string) ([]Stack, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Stack
	for _, st := range s.inventory[accountID] {
		if st.Qty > 0 {
			out = append(out, *st)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ItemID < out[j].ItemID })
	return out, nil
}

func (s *Memory) Apply(_ context.Context, e Entry) (Receipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prof, ok := s.profiles[e.AccountID]
	if !ok {
		return Receipt{}, ErrNoAccount
	}
	stack := func() Stack {
		if e.ItemID == "" {
			return Stack{}
		}
		if st := s.inventory[e.AccountID][e.ItemID]; st != nil {
			return *st
		}
		return Stack{ItemID: e.ItemID}
	}
	if s.ledger[e.AccountID+"\x00"+e.Key] {
		return Receipt{Profile: *prof, Item: stack(), Replay: true}, nil
	}
	if prof.Currency+e.Delta < 0 {
		return Receipt{}, ErrInsufficientFunds
	}
	cur := stack()
	if e.ItemID != "" && e.Max > 0 && cur.Qty+e.Qty > e.Max {
		return Receipt{}, ErrItemLimit
	}

	s.ledger[e.AccountID+"\x00"+e.Key] = true
	prof.Currency += e.Delta
	if e.ItemID != "" && e.Qty != 0 {
		items := s.inventory[e.AccountID]
		if items == nil {
			items = map[string]*Stack{}
			s.inventory[e.AccountID] = items
		}
		st := items[e.ItemID]
		if st == nil {
			st = &Stack{ItemID: e.ItemID, AcquiredAt: e.At}
			items[e.ItemID] = st
		}
		st.Qty += e.Qty
		cur = *st
	}
	return Receipt{Profile: *prof, Item: cur}, nil
}

func (s *Memory) RecordMatch(_ context.Context, rows []MatchRow) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	applied := 0
	for _, r := range rows {
		prof, ok := s.profiles[r.AccountID]
		if !ok {
			// A bot, or a player whose account was deleted mid-match. Skipping
			// one line is right; failing the whole match would lose the others.
			continue
		}
		key := r.MatchID + "\x00" + r.AccountID
		if s.matches[key] {
			continue
		}
		s.matches[key] = true
		s.history[r.AccountID] = append(s.history[r.AccountID], r)
		prof.Rating = max(prof.Rating+r.RatingDelta(), rating.Floor)
		prof.Matches++
		prof.Kills += r.Score
		if r.Won() {
			prof.Wins++
		}
		prof.Currency += r.Reward
		applied++
	}
	return applied, nil
}

func (s *Memory) History(_ context.Context, accountID string, limit int) ([]MatchRow, error) {
	if limit <= 0 {
		limit = 20
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows := append([]MatchRow(nil), s.history[accountID]...)
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].EndedAt.Equal(rows[j].EndedAt) {
			return rows[i].EndedAt.After(rows[j].EndedAt)
		}
		return rows[i].MatchID > rows[j].MatchID
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func (s *Memory) Leaderboard(_ context.Context, limit int) ([]Rank, error) {
	if limit <= 0 {
		limit = 50
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Rank
	for id, p := range s.profiles {
		a := s.accounts[id]
		// matches > 0 is the same filter the profiles_rank_idx carries. An
		// account that has never played is not sitting at 1000 on the ladder;
		// it has no position on it at all, and a board full of them buries
		// everybody who actually played.
		if a == nil || p.Matches == 0 {
			continue
		}
		out = append(out, Rank{
			AccountID: id, DisplayName: a.DisplayName,
			Rating: p.Rating, Matches: p.Matches, Wins: p.Wins,
		})
	}
	// Ties break by account id so the order is total. A leaderboard that
	// reshuffles equal ratings between two reads looks broken to a player
	// refreshing it, and to a paginating client it is broken: rows move across
	// the page boundary and are seen twice or not at all.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Rating != out[j].Rating {
			return out[i].Rating > out[j].Rating
		}
		return out[i].AccountID < out[j].AccountID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	for i := range out {
		out[i].Rank = i + 1
	}
	return out, nil
}
