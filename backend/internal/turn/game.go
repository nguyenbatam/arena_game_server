// Package turn implements the repo's second sync family: an event-log game
// with a sequence-number cursor, the shape Telegram calls pts and every
// turn-based game reaches for.
//
// It shares the gateway, auth, presence and session layers with the realtime
// arena and differs in exactly the places the two families differ:
//
//   - No tick loop. State changes on request, and lives in a store rather than
//     in one goroutine's memory.
//   - The wire carries ordered events, not snapshots. Nothing may be dropped or
//     reordered, which is the opposite of the arena's latest-wins policy.
//   - Deadlines come from a scheduler instead of a tick counter.
//   - Players hold hidden cards, so what ships is a projection of the log for
//     one viewer, never the log itself.
//
// The game is a three-trick card game: both players hold three cards, take
// turns playing one, and the higher card takes the trick.
package turn

import (
	"errors"
	"sort"
)

const (
	// HandSize is cards per player; each is played exactly once, so a match is
	// always HandSize tricks long.
	HandSize = 3
	// DeckHigh is the highest card value. Values are 1..DeckHigh.
	DeckHigh = 10
)

var (
	ErrNotYourTurn  = errors.New("not your turn")
	ErrCardNotHeld  = errors.New("card not in hand")
	ErrMatchOver    = errors.New("match already ended")
	ErrStaleTurn    = errors.New("turn already advanced")
	ErrUnknownSeat  = errors.New("player not in this match")
	ErrBadCardValue = errors.New("card value out of range")
)

// State is the full truth about a match. Nothing here is per-viewer; Project
// turns it into what one player is allowed to see.
type State struct {
	MatchID string
	Seq     uint64
	Players [2]string
	Hands   [2][]uint32
	Scores  [2]uint32

	// Turn is the seat index (0 or 1) whose move is awaited.
	Turn int
	// TurnNumber increments on every applied move. It is the compare-and-set
	// token: a move and a timeout racing for the same turn both name it, and
	// only the first to land wins. It starts at 1, so zero can keep its
	// meaning of "caller is not checking".
	TurnNumber uint32

	// TableCard is the card already played this trick, waiting to be beaten.
	TableCard  uint32
	TableOwner int // seat that played TableCard, -1 when the table is empty

	Ended  bool
	Winner string // empty means a draw

	DeadlineMS int64
	// AppliedIdem remembers handled submission keys so a client retry is a
	// no-op rather than a second move. Keys are scoped to the submitting
	// player — see idemKey.
	AppliedIdem map[string]uint32
}

// NewState deals a match. The deal is driven by a seeded RNG so the same match
// id and seed always produce the same cards — replayable, and auditable after
// a cheating report.
func NewState(matchID string, seed int64, players [2]string) *State {
	r := newRNG(uint64(seed))
	deck := make([]uint32, 0, DeckHigh)
	for v := uint32(1); v <= DeckHigh; v++ {
		deck = append(deck, v)
	}
	// Fisher-Yates, deterministic in the seed.
	for i := len(deck) - 1; i > 0; i-- {
		j := int(r.Uint32() % uint32(i+1))
		deck[i], deck[j] = deck[j], deck[i]
	}

	s := &State{
		MatchID:     matchID,
		Players:     players,
		TurnNumber:  1,
		TableOwner:  -1,
		AppliedIdem: map[string]uint32{},
	}
	for seat := 0; seat < 2; seat++ {
		hand := append([]uint32(nil), deck[seat*HandSize:(seat+1)*HandSize]...)
		sort.Slice(hand, func(a, b int) bool { return hand[a] < hand[b] })
		s.Hands[seat] = hand
	}
	return s
}

// Seat maps a player id to its index, or -1.
func (s *State) Seat(playerID string) int {
	for i, p := range s.Players {
		if p == playerID {
			return i
		}
	}
	return -1
}

func (s *State) Opponent(seat int) int { return 1 - seat }

// LowestCard is the auto-play choice when a turn times out.
//
// It is deliberately the weakest legal move. A fallback that played well would
// reward going away: players would stop taking their turns and let the server
// win for them. The safe-but-bad default is the same reasoning behind poker
// auto-folding rather than auto-calling.
func (s *State) LowestCard(seat int) uint32 {
	if len(s.Hands[seat]) == 0 {
		return 0
	}
	low := s.Hands[seat][0]
	for _, c := range s.Hands[seat] {
		if c < low {
			low = c
		}
	}
	return low
}

func (s *State) holds(seat int, card uint32) bool {
	for _, c := range s.Hands[seat] {
		if c == card {
			return true
		}
	}
	return false
}

func (s *State) removeCard(seat int, card uint32) {
	out := s.Hands[seat][:0]
	dropped := false
	for _, c := range s.Hands[seat] {
		if !dropped && c == card {
			dropped = true
			continue
		}
		out = append(out, c)
	}
	s.Hands[seat] = out
}

// Move is one submitted play.
type Move struct {
	PlayerID string
	Card     uint32
	// TurnNumber the client believed was current. Zero skips the check and is
	// only ever right when the caller genuinely has no opinion — every real
	// submission, including the timeout worker's, names the turn it means.
	TurnNumber uint32
	IdemKey    string
	// Auto marks a move the server made on the player's behalf.
	Auto bool
}

// Apply validates and applies a move, returning the events it produced.
//
// Every mutation in the match goes through here — a player's move and the
// timeout worker's auto-play take the same path, so there is one place where
// turn order, legality and the CAS check are enforced.
func (s *State) Apply(m Move) ([]Event, error) {
	if s.Ended {
		return nil, ErrMatchOver
	}
	seat := s.Seat(m.PlayerID)
	if seat < 0 {
		return nil, ErrUnknownSeat
	}
	if k := idemKey(m); k != "" {
		if _, done := s.AppliedIdem[k]; done {
			// Already handled: a retry, not a new move.
			return nil, nil
		}
	}
	if seat != s.Turn {
		return nil, ErrNotYourTurn
	}
	if m.TurnNumber != 0 && m.TurnNumber != s.TurnNumber {
		return nil, ErrStaleTurn
	}
	if m.Card == 0 || m.Card > DeckHigh {
		return nil, ErrBadCardValue
	}
	if !s.holds(seat, m.Card) {
		return nil, ErrCardNotHeld
	}

	s.removeCard(seat, m.Card)
	s.TurnNumber++
	if k := idemKey(m); k != "" {
		s.AppliedIdem[k] = s.TurnNumber
	}

	kind := KindPlayed
	if m.Auto {
		kind = KindTimeout
	}
	events := []Event{{
		Kind:      kind,
		PlayerID:  m.PlayerID,
		Card:      m.Card,
		HandCount: uint32(len(s.Hands[seat])),
	}}

	if s.TableOwner < 0 {
		// First card of the trick; the other player answers.
		s.TableCard = m.Card
		s.TableOwner = seat
		s.Turn = s.Opponent(seat)
		return events, nil
	}

	// Second card: resolve the trick.
	winner := s.TableOwner
	if m.Card > s.TableCard {
		winner = seat
	}
	s.Scores[winner]++
	events = append(events, Event{
		Kind:     KindTrick,
		PlayerID: s.Players[winner],
		Card:     max32(m.Card, s.TableCard),
	})

	s.TableCard = 0
	s.TableOwner = -1
	// Trick winner leads the next one.
	s.Turn = winner

	if len(s.Hands[0]) == 0 && len(s.Hands[1]) == 0 {
		s.Ended = true
		switch {
		case s.Scores[0] > s.Scores[1]:
			s.Winner = s.Players[0]
		case s.Scores[1] > s.Scores[0]:
			s.Winner = s.Players[1]
		default:
			s.Winner = ""
		}
		events = append(events, Event{Kind: KindEnded, Winner: s.Winner})
	}
	return events, nil
}

// idemKey scopes a submission key to the player that sent it.
//
// The key is chosen by the client, and clients pick from the same small space —
// a counter, a turn number, a retry index. Storing it bare lets one player's
// key match another's, and the second player's perfectly legal move is then
// swallowed as a duplicate: Apply returns no events, the caller reports
// success, and the move simply never happened. Namespacing costs one
// concatenation and makes collisions impossible between players.
func idemKey(m Move) string {
	if m.IdemKey == "" {
		return ""
	}
	return m.PlayerID + "\x00" + m.IdemKey
}

func max32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

// Clone is a deep copy, so a caller can try a move without touching the stored
// state until the write is known to have won its CAS.
func (s *State) Clone() *State {
	out := *s
	for i := range s.Hands {
		out.Hands[i] = append([]uint32(nil), s.Hands[i]...)
	}
	out.AppliedIdem = make(map[string]uint32, len(s.AppliedIdem))
	for k, v := range s.AppliedIdem {
		out.AppliedIdem[k] = v
	}
	return &out
}
