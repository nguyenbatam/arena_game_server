package main

import (
	"fmt"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
)

// runTurnBot drives one player through the turn-based mode: pair, play whenever
// the clock is on you, start another match when the last one ends.
//
// It keeps a cursor and checks it, because that is the half of this protocol
// the arena does not have and therefore the half a snapshot-shaped load test
// cannot exercise at all. Events are numbered consecutively, so the run a
// client is owed is exactly seq+1 .. current_seq; anything else is a hole, and
// a hole is the symptom of a push that did not arrive. web/cursor.js applies
// the same rule in the browser.
func runTurnBot(addr, id string, st *stats, stop time.Time) {
	c, err := dial(addr, id)
	if err != nil {
		st.dialFail.Add(1)
		return
	}
	s := &sock{st: st}
	s.set(c)
	defer s.close()
	st.connected.Add(1)

	s.send(protocol.Env(pb.MsgType_MSG_TYPE_HELLO, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Hello{Hello: &pb.Hello{Name: id, SessionId: id, ProtocolVersion: protocol.Version}}
	}))
	s.send(protocol.Env(pb.MsgType_MSG_TYPE_TURN_JOIN, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_TurnJoin{TurnJoin: &pb.TurnJoin{}}
	}))

	var matchID string
	var seq uint64

	for time.Now().Before(stop) {
		data, err := s.read(stop.Add(2 * time.Second))
		if err != nil {
			return
		}
		e, err := protocol.UnmarshalEnv(data)
		if err != nil {
			continue
		}
		switch e.Type {
		case pb.MsgType_MSG_TYPE_ERROR:
			code := e.GetError().GetCode()
			st.serverError(code)
			// A rejected move is a rule violation this bot can survive — it
			// simply is not this bot's turn. Anything else ends the session.
			if code != pb.ErrorCode_ERROR_CODE_BAD_PAYLOAD {
				return
			}
		case pb.MsgType_MSG_TYPE_TURN_UPDATE:
			up := e.GetTurnUpdate()
			if up == nil || up.State == nil {
				continue
			}
			st.updates.Add(1)

			// The cursor belongs to a match, not to a connection. An update
			// naming a different one — the next match starting, or a straggler
			// from the last — cannot be measured against the cursor we hold,
			// and comparing them anyway reports a hole that is not there.
			if up.State.MatchId != matchID {
				matchID, seq = up.State.MatchId, 0
				st.matches.Add(1)
			}

			if up.FullResync {
				st.resyncs.Add(1)
			} else if turnGap(up, seq) {
				// Ask for the missing run and leave the cursor where it is, so
				// the next update is measured against what we actually hold.
				st.gaps.Add(1)
				s.send(protocol.Env(pb.MsgType_MSG_TYPE_TURN_SYNC, func(e *pb.Envelope) {
					e.Payload = &pb.Envelope_TurnSync{TurnSync: &pb.TurnSync{
						MatchId: up.State.MatchId, SinceSeq: seq,
					}}
				}))
				continue
			}
			if up.CurrentSeq > seq {
				seq = up.CurrentSeq
			}

			if up.State.Ended {
				// Straight into another one, so a long run keeps the pairing
				// slot and the store under continuous load.
				matchID, seq = "", 0
				s.send(protocol.Env(pb.MsgType_MSG_TYPE_TURN_JOIN, func(e *pb.Envelope) {
					e.Payload = &pb.Envelope_TurnJoin{TurnJoin: &pb.TurnJoin{}}
				}))
				continue
			}
			if up.State.Turn != up.State.YourId || len(up.State.YourHand) == 0 {
				continue
			}
			// turn_number is the compare-and-set token; idem_key identifies the
			// intent and so must be stable across retries — one intent per turn.
			st.moves.Add(1)
			s.send(protocol.Env(pb.MsgType_MSG_TYPE_TURN_PLAY, func(e *pb.Envelope) {
				e.Payload = &pb.Envelope_TurnPlay{TurnPlay: &pb.TurnPlay{
					MatchId: up.State.MatchId, Card: up.State.YourHand[0],
					TurnNumber: up.State.TurnNumber,
					IdemKey:    fmt.Sprintf("%s:%d", up.State.MatchId, up.State.TurnNumber),
				}}
			}))
		}
	}
}

// turnGap is the Go twin of hasGap in web/cursor.js.
func turnGap(up *pb.TurnUpdate, seq uint64) bool {
	if up.CurrentSeq <= seq {
		return false
	}
	if len(up.Events) == 0 {
		return true
	}
	if up.Events[0].Seq != seq+1 {
		return true
	}
	for i := 1; i < len(up.Events); i++ {
		if up.Events[i].Seq != up.Events[i-1].Seq+1 {
			return true
		}
	}
	return up.Events[len(up.Events)-1].Seq != up.CurrentSeq
}
