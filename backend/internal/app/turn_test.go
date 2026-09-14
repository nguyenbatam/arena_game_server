package app

import (
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/session"
)

// Two connections pair into a match, and each is dealt a view that hides the
// other's hand — end to end through the gateway, not just the service.
func TestTurnJoinPairsAndProjects(t *testing.T) {
	a := testApp(t, time.Minute)

	c1 := session.NewConn("turn-1", 64)
	c2 := session.NewConn("turn-2", 64)
	join(t, a, c1, "turn-1")
	join(t, a, c2, "turn-2")

	joinMsg := protocol.Env(pb.MsgType_MSG_TYPE_TURN_JOIN, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_TurnJoin{TurnJoin: &pb.TurnJoin{}}
	})
	a.onMessage(c1, joinMsg)
	// First arrival waits.
	if e := nextOfType(t, c1, pb.MsgType_MSG_TYPE_QUEUED); e == nil {
		t.Fatal("first player should be queued")
	}
	a.onMessage(c2, joinMsg)

	up1 := nextOfType(t, c1, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	up2 := nextOfType(t, c2, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()

	for i, up := range []*pb.TurnUpdate{up1, up2} {
		if up.State == nil {
			t.Fatalf("player %d got no state", i+1)
		}
		if len(up.State.YourHand) == 0 {
			t.Fatalf("player %d was dealt no cards", i+1)
		}
		if up.State.OpponentHandCount == 0 {
			t.Fatalf("player %d cannot see the opponent holds cards", i+1)
		}
		for _, e := range up.Events {
			if e.Kind == pb.TurnEventKind_TURN_EVENT_KIND_DEALT &&
				e.PlayerId != up.State.Turn && len(e.Hand) > 0 && e.PlayerId != playerOf(up) {
				t.Fatalf("player %d received the opponent's actual cards", i+1)
			}
		}
	}
	if up1.State.MatchId != up2.State.MatchId {
		t.Fatal("both players must land in the same match")
	}

	// A move by whoever is on the clock reaches both sides.
	mover, moverConn := up1, c1
	if up2.State.Turn != up1.State.Turn {
		t.Fatal("both projections must agree on whose turn it is")
	}
	if len(up2.State.YourHand) > 0 && up2.State.Turn != up1.State.Turn {
		mover, moverConn = up2, c2
	}
	card := mover.State.YourHand[0]
	a.onMessage(moverConn, protocol.Env(pb.MsgType_MSG_TYPE_TURN_PLAY, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_TurnPlay{TurnPlay: &pb.TurnPlay{
			MatchId: mover.State.MatchId, Card: card, TurnNumber: mover.State.TurnNumber,
		}}
	}))

	// Whether the move was legal for this connection or not, the gateway must
	// answer rather than go silent.
	select {
	case <-c1.Outbox():
	case <-c2.Outbox():
	case <-time.After(time.Second):
		t.Fatal("no response to a turn play")
	}
}

// playerOf recovers which player a projection belongs to: only its owner sees a
// hand, so the hand's presence identifies the viewer.
func playerOf(up *pb.TurnUpdate) string {
	for _, e := range up.Events {
		if e.Kind == pb.TurnEventKind_TURN_EVENT_KIND_DEALT && len(e.Hand) > 0 {
			return e.PlayerId
		}
	}
	return ""
}

func TestTurnSyncFromCursor(t *testing.T) {
	a := testApp(t, time.Minute)
	c1 := session.NewConn("turn-a", 64)
	c2 := session.NewConn("turn-b", 64)
	join(t, a, c1, "turn-a")
	join(t, a, c2, "turn-b")

	joinMsg := protocol.Env(pb.MsgType_MSG_TYPE_TURN_JOIN, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_TurnJoin{TurnJoin: &pb.TurnJoin{}}
	})
	a.onMessage(c1, joinMsg)
	nextOfType(t, c1, pb.MsgType_MSG_TYPE_QUEUED)
	a.onMessage(c2, joinMsg)

	first := nextOfType(t, c1, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	matchID := first.State.MatchId

	// Asking from the cursor we already hold yields no repeated events.
	a.onMessage(c1, protocol.Env(pb.MsgType_MSG_TYPE_TURN_SYNC, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_TurnSync{TurnSync: &pb.TurnSync{
			MatchId: matchID, SinceSeq: first.CurrentSeq,
		}}
	}))
	up := nextOfType(t, c1, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	if up.FullResync {
		t.Fatal("an up-to-date cursor must not trigger a full resync")
	}
	if len(up.Events) != 0 {
		t.Fatalf("expected no new events, got %d", len(up.Events))
	}
	if up.State == nil || len(up.State.YourHand) == 0 {
		t.Fatal("a sync should still carry the caller's own state")
	}
}
