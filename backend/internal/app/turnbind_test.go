package app

import (
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/session"
)

func turnSyncMsg(matchID string, since uint64) []byte {
	return protocol.Env(pb.MsgType_MSG_TYPE_TURN_SYNC, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_TurnSync{TurnSync: &pb.TurnSync{MatchId: matchID, SinceSeq: since}}
	})
}

// A message still in flight on the connection a reconnect just replaced must
// not re-file that dead connection as where the player's updates go. Every
// later push would be handed to a closed socket and vanish, while the live
// connection sits there receiving nothing.
func TestLateMessageDoesNotStealTurnDelivery(t *testing.T) {
	a := testApp(t, time.Minute)

	c1 := session.NewConn("acct-1", 64)
	c2 := session.NewConn("acct-2", 64)
	join(t, a, c1, "acct-1")
	join(t, a, c2, "acct-2")

	a.onMessage(c1, turnJoinMsg())
	nextOfType(t, c1, pb.MsgType_MSG_TYPE_QUEUED)
	a.onMessage(c2, turnJoinMsg())

	up1 := nextOfType(t, c1, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	nextOfType(t, c2, pb.MsgType_MSG_TYPE_TURN_UPDATE)
	matchID := up1.State.MatchId

	// acct-1 reconnects. hub.Add closes c1 and files c1b under the same id.
	c1b := session.NewConn("acct-1", 64)
	join(t, a, c1b, "acct-1")
	a.onMessage(c1b, turnSyncMsg(matchID, 0))
	nextOfType(t, c1b, pb.MsgType_MSG_TYPE_TURN_UPDATE)

	// Now the frame c1 had already read, arriving after the reconnect.
	a.onMessage(c1, turnSyncMsg(matchID, 0))

	if got := a.turnHub.conn("acct-1"); got != c1b {
		t.Errorf("delivery target is the replaced connection, not the live one")
	}

	// Deliver a push for acct-1. Driving it through turnNotify rather than a
	// real move keeps the test off the deal: whose turn it is varies with the
	// match nonce, and if acct-1 happened to be the mover its own TURN_PLAY
	// would re-bind the live connection and hide the failure.
	a.turnNotify("acct-1", &pb.TurnUpdate{
		CurrentSeq: 99,
		State:      &pb.TurnState{MatchId: matchID, YourId: "acct-1"},
	})

	got := nextOfType(t, c1b, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	if got.CurrentSeq != 99 {
		t.Fatalf("the live connection received seq %d, want the pushed 99", got.CurrentSeq)
	}
}
