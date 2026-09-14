package turn

import pb "github.com/nguyenbatam/arena_game_server/gen/pb"

type Kind int

const (
	KindDealt Kind = iota + 1
	KindPlayed
	KindTrick
	KindTimeout
	KindEnded
)

// Event is one mutation in a match. The log keeps these verbatim; what goes on
// the wire is always Project'ed for a single viewer.
type Event struct {
	Seq        uint64
	Kind       Kind
	PlayerID   string
	Card       uint32
	HandCount  uint32
	Hand       []uint32
	Winner     string
	DeadlineMS int64
}

// Project strips what `viewer` is not entitled to know.
//
// This is the structural difference between a card game and a shooter. In the
// arena every subscriber can be handed the same encoded bytes. Here the same
// logged event becomes different bytes per recipient, because a hand is private
// until its cards are played. The log holds the truth; the projection ships.
func (e Event) Project(viewer string) Event {
	out := e
	if e.Kind == KindDealt && e.PlayerID != viewer {
		// The opponent learns how many cards you hold, never which.
		out.Hand = nil
	} else if len(e.Hand) > 0 {
		out.Hand = append([]uint32(nil), e.Hand...)
	}
	return out
}

func (e Event) toPB() *pb.TurnEvent {
	return &pb.TurnEvent{
		Seq: e.Seq, Kind: kindToPB(e.Kind), PlayerId: e.PlayerID,
		Card: e.Card, HandCount: e.HandCount, Hand: e.Hand,
		Winner: e.Winner, DeadlineUnixMs: e.DeadlineMS,
	}
}

func kindToPB(k Kind) pb.TurnEventKind {
	switch k {
	case KindDealt:
		return pb.TurnEventKind_TURN_EVENT_KIND_DEALT
	case KindPlayed:
		return pb.TurnEventKind_TURN_EVENT_KIND_PLAYED
	case KindTrick:
		return pb.TurnEventKind_TURN_EVENT_KIND_TRICK
	case KindTimeout:
		return pb.TurnEventKind_TURN_EVENT_KIND_TIMEOUT
	case KindEnded:
		return pb.TurnEventKind_TURN_EVENT_KIND_ENDED
	}
	return pb.TurnEventKind_TURN_EVENT_KIND_UNSPECIFIED
}

// Project renders the match for one viewer: their own hand in full, the
// opponent's only as a count.
func (s *State) Project(viewer string) *pb.TurnState {
	seat := s.Seat(viewer)
	out := &pb.TurnState{
		MatchId: s.MatchID, Seq: s.Seq, TurnNumber: s.TurnNumber,
		TableCard: s.TableCard, Ended: s.Ended, Winner: s.Winner,
		DeadlineUnixMs: s.DeadlineMS,
	}
	if s.Turn >= 0 && s.Turn < len(s.Players) {
		out.Turn = s.Players[s.Turn]
	}
	if s.TableOwner >= 0 {
		out.TableOwner = s.Players[s.TableOwner]
	}
	if seat < 0 {
		// A spectator sees only public information.
		out.OpponentHandCount = uint32(len(s.Hands[0]) + len(s.Hands[1]))
		return out
	}
	out.YourId = viewer
	out.YourHand = append([]uint32(nil), s.Hands[seat]...)
	out.OpponentHandCount = uint32(len(s.Hands[s.Opponent(seat)]))
	out.YourScore = s.Scores[seat]
	out.OpponentScore = s.Scores[s.Opponent(seat)]
	return out
}
