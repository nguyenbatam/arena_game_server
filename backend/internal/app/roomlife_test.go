package app

import (
	"context"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/placement"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/session"
)

// playOneShortMatch runs a real room to its end on this node and returns once
// the manager has let go of it.
func playOneShortMatch(t *testing.T, a *App, c *session.Conn, roomID string) {
	t.Helper()
	req := &pb.RoomRequest{
		RoomId: roomID, Seed: 1, TickRate: 100, MatchTicks: 20,
		Seats: []*pb.Seat{{ConnId: c.ID(), PlayerId: 1}}, Bots: 1,
	}
	a.startRoom(placement.NewJob(req))
	a.joinRoom(c, roomID, 1)
	if got, _ := c.Room(); got != roomID {
		t.Fatalf("joinRoom did not seat the connection: room=%q", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for a.rooms.Count() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := a.rooms.Count(); n != 0 {
		t.Fatalf("the match never finished: %d rooms still active", n)
	}
}

// The binding a match puts on a connection has to come off when the match ends.
//
// It never did: Conn.roomID was written on join and nothing cleared it, so
// onJoinQueue refused the player for the rest of their session — and refused
// them silently, because that guard just returns. "Play again" on a socket that
// had already played did nothing at all, with no error and no log.
func TestAFinishedMatchFreesTheConnectionToQueueAgain(t *testing.T) {
	a := testApp(t, time.Minute)
	c := session.NewConn("c1", 256)
	drain(c)
	a.hub.Add(c)

	playOneShortMatch(t, a, c, "r-over")

	if got := c.RoomID(); got != "" {
		t.Fatalf("connection still bound to %q after the match ended", got)
	}
	a.onJoinQueue(c)
	if n, _ := a.queue.Depth(context.Background()); n != 1 {
		t.Fatalf("queue depth after a finished match = %d, want 1", n)
	}
}

// The same binding is what made a disconnect write a presence record naming a
// room that was over. With the seat released, the disconnect is an ordinary one
// and leaves nothing behind for the next HELLO to trip over.
func TestDisconnectAfterAFinishedMatchLeavesNoStaleRoom(t *testing.T) {
	a := testApp(t, time.Minute)
	c := session.NewConn("c1", 256)
	drain(c)
	a.hub.Add(c)

	playOneShortMatch(t, a, c, "r-over")

	a.hub.Remove(c)
	a.onClose(c)

	p, err := a.presence.Get(context.Background(), "c1")
	if err != nil {
		t.Fatal(err)
	}
	if p != nil {
		t.Fatalf("a disconnect after the match ended left presence %v room=%q; want no record at all",
			p.Status, p.RoomId)
	}
}

// A reconnect whose presence record names a match that has since finished is an
// ordinary arrival, not an error.
//
// The record outlives the match by design — it is kept for the grace window —
// and the match can end inside that window, which is exactly what happens to
// somebody whose connection fails in the closing seconds of a game. The old
// path announced MATCH_FOUND, failed to join, answered ROOM_NOT_ON_NODE, and
// returned before writing presence back to online, so every retry for the rest
// of the grace window did the same thing.
func TestReconnectIntoAMatchThatIsOverIsGreetedNormally(t *testing.T) {
	a := testApp(t, time.Minute)
	ctx := context.Background()
	if err := a.presence.Set(ctx, &pb.Presence{
		PlayerId: "c9", Name: "tester", NodeId: a.cfg.NodeID,
		Status: pb.PresenceStatus_PRESENCE_STATUS_DISCONNECTED,
		RoomId: "r-long-gone", SeatId: 2, LastTick: 400,
	}); err != nil {
		t.Fatal(err)
	}

	c := session.NewConn("c9", 256)
	a.hub.Add(c)
	a.onHello(c, &pb.Hello{Name: "tester", SessionId: "c9", ProtocolVersion: 1})

	for _, e := range drainWithin(t, c, 300*time.Millisecond) {
		switch e.Type {
		case pb.MsgType_MSG_TYPE_ERROR:
			t.Fatalf("reconnect answered with %v %q", e.GetError().Code, e.GetError().Message)
		case pb.MsgType_MSG_TYPE_MATCH_FOUND:
			t.Fatalf("a match that no longer exists was announced: room %q", e.GetMatchFound().RoomId)
		}
	}
	p, _ := a.presence.Get(ctx, "c9")
	if p == nil || p.Status != pb.PresenceStatus_PRESENCE_STATUS_ONLINE {
		t.Fatalf("presence after reconnect = %v, want ONLINE", p.GetStatus())
	}
	if p.RoomId != "" {
		t.Fatalf("presence still names room %q", p.RoomId)
	}
}

// A connection that has already been replaced and seated somewhere else must
// not be unbound by the old match finishing — the same identity check onClose
// makes, and for the same reason: the player is live in another room.
func TestReleaseSeatsLeavesAConnectionThatHasMovedOn(t *testing.T) {
	a := testApp(t, time.Minute)
	c := session.NewConn("c1", 32)
	drain(c)
	a.hub.Add(c)
	c.BindRoom("r-next", 4)

	a.releaseSeats(&pb.RoomRequest{
		RoomId: "r-previous",
		Seats:  []*pb.Seat{{ConnId: "c1", PlayerId: 1}},
	})

	if id, seat := c.Room(); id != "r-next" || seat != 4 {
		t.Fatalf("a finished match unbound a connection that had moved on: room=%q seat=%d", id, seat)
	}
}

// Nobody is seated into a room that has stopped ticking. It would never
// broadcast again, so the player would sit on an empty screen — and would be
// bound to a dead room while they did it.
func TestJoinRoomRefusesARoomThatHasStopped(t *testing.T) {
	a := testApp(t, time.Minute)
	c := session.NewConn("c1", 64)
	a.hub.Add(c)

	r := startTestRoom(t, a, "c1", 1)
	r.Stop()

	a.joinRoom(c, testRoomID, 1)
	e := nextOfType(t, c, pb.MsgType_MSG_TYPE_ERROR)
	if e.GetError().Code != pb.ErrorCode_ERROR_CODE_ROOM_NOT_ON_NODE {
		t.Fatalf("got %v, want ROOM_NOT_ON_NODE", e.GetError().Code)
	}
	if got := c.RoomID(); got != "" {
		t.Fatalf("a refused join still bound the connection to %q", got)
	}
}

// drainWithin takes everything a connection has been sent inside the window.
func drainWithin(t *testing.T, c *session.Conn, window time.Duration) []*pb.Envelope {
	t.Helper()
	var out []*pb.Envelope
	deadline := time.After(window)
	for {
		select {
		case raw, ok := <-c.Outbox():
			if !ok {
				return out
			}
			e, err := protocol.UnmarshalEnv(raw)
			if err != nil {
				continue
			}
			out = append(out, e)
		case <-deadline:
			return out
		}
	}
}
