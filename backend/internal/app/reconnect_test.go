package app

import (
	"context"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/config"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/room"
	"github.com/nguyenbatam/arena_game_server/internal/session"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

const testRoomID = "room-1"

func testApp(t *testing.T, grace time.Duration) *App {
	t.Helper()
	cfg := config.Static{
		NodeID:     "gs-test",
		PublicAddr: "ws://localhost:8080/ws",
		Role:       pb.Role_ROLE_ALL,
	}
	dyn := config.NewLive(config.View{
		TickRate: 50, RoomSize: 2, MinPlayers: 1,
		QueueTimeout: time.Second, MatchSeconds: 120,
		SendBuffer: 64, DisconnectGrace: grace,
	}, nil)
	a, err := New(cfg, dyn)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	return a
}

// startTestRoom brings up a room the way startRoom would, and registers it in
// the directory so resumeMatch can find it again after a disconnect.
func startTestRoom(t *testing.T, a *App, connID string, playerID uint32) *room.Room {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		a.rooms.StopAll()
		cancel()
	})
	r := a.rooms.Start(ctx, room.Params{
		ID: testRoomID, Seed: 7, TickRate: 50, MatchTicks: 6000,
		Roster: []sim.Player{{ID: sim.PlayerID(playerID)}, {ID: 1000, Bot: true}},
		Seats:  map[string]uint32{connID: playerID},
	})
	if err := a.registry.RegisterRoom(context.Background(), &pb.RoomInfo{
		RoomId: testRoomID, ServerId: a.cfg.NodeID, PublicAddr: a.cfg.PublicAddr, Seed: 7,
	}); err != nil {
		t.Fatalf("RegisterRoom: %v", err)
	}
	return r
}

// nextOfType drains a connection outbox until a message of the wanted type
// shows up, or the deadline passes.
func nextOfType(t *testing.T, c *session.Conn, want pb.MsgType) *pb.Envelope {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case raw, ok := <-c.Outbox():
			if !ok {
				t.Fatalf("connection closed while waiting for %v", want)
			}
			e, err := protocol.UnmarshalEnv(raw)
			if err != nil {
				continue
			}
			if e.Type == want {
				return e
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %v", want)
		}
	}
}

func join(t *testing.T, a *App, c *session.Conn, sessionID string) {
	t.Helper()
	a.hub.Add(c)
	a.onHello(c, &pb.Hello{Name: "tester", SessionId: sessionID})
}

// A player who drops mid-match keeps the seat, and the reconnecting connection
// is put back in the same room at the current tick.
func TestReconnectWithinGraceKeepsSeatAndResyncs(t *testing.T) {
	a := testApp(t, 30*time.Second)
	const sessionID = "sess-1"
	const seat = uint32(1)
	startTestRoom(t, a, sessionID, seat)

	c1 := session.NewConn(sessionID, 64)
	join(t, a, c1, sessionID)
	a.onMessage(c1, protocol.Env(pb.MsgType_MSG_TYPE_JOIN_ROOM, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_JoinRoom{JoinRoom: &pb.JoinRoom{
			RoomId: testRoomID, YourId: seat, SessionId: sessionID,
		}}
	}))

	first := nextOfType(t, c1, pb.MsgType_MSG_TYPE_SNAPSHOT).GetSnapshot()
	if first.BaselineTick != 0 {
		t.Fatalf("a client that has acked nothing must get a full snapshot, got baseline %d", first.BaselineTick)
	}
	if c1.RoomID() != testRoomID || c1.PlayerID() != seat {
		t.Fatalf("conn bound to room=%q seat=%d, want %q/%d", c1.RoomID(), c1.PlayerID(), testRoomID, seat)
	}

	// Ack, then confirm the server switches to deltas.
	a.onMessage(c1, protocol.Env(pb.MsgType_MSG_TYPE_INPUT, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Input{Input: &pb.Input{Seq: 1, Mx: 1, AckTick: first.Tick}}
	}))
	deadline := time.After(2 * time.Second)
	sawDelta := false
	for !sawDelta {
		select {
		case raw := <-c1.Outbox():
			e, err := protocol.UnmarshalEnv(raw)
			if err != nil {
				continue
			}
			if s := e.GetSnapshot(); s != nil && s.BaselineTick != 0 {
				sawDelta = true
			}
		case <-deadline:
			t.Fatal("server never switched to delta snapshots after an ack")
		}
	}

	// Drop.
	a.onClose(c1)
	c1.Close()
	p, err := a.presence.Get(context.Background(), sessionID)
	if err != nil || p == nil {
		t.Fatalf("presence lookup after close: %v", err)
	}
	if p.Status != pb.PresenceStatus_PRESENCE_STATUS_DISCONNECTED {
		t.Fatalf("status = %v, want DISCONNECTED", p.Status)
	}
	if p.RoomId != testRoomID || p.SeatId != seat {
		t.Fatalf("seat not held: room=%q seat=%d", p.RoomId, p.SeatId)
	}

	// Reconnect on a fresh connection carrying the same session id.
	c2 := session.NewConn(sessionID, 64)
	join(t, a, c2, sessionID)

	mf := nextOfType(t, c2, pb.MsgType_MSG_TYPE_MATCH_FOUND).GetMatchFound()
	if mf.RoomId != testRoomID {
		t.Fatalf("resumed into room %q, want %q", mf.RoomId, testRoomID)
	}
	if mf.YourId != seat {
		t.Fatalf("resumed with seat %d, want %d", mf.YourId, seat)
	}

	a.onMessage(c2, protocol.Env(pb.MsgType_MSG_TYPE_JOIN_ROOM, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_JoinRoom{JoinRoom: &pb.JoinRoom{
			RoomId: testRoomID, YourId: seat, SessionId: sessionID,
			// Claiming an old tick must not make the server delta against a
			// baseline this fresh connection cannot rebuild.
			LastAckTick: first.Tick,
		}}
	}))

	again := nextOfType(t, c2, pb.MsgType_MSG_TYPE_SNAPSHOT).GetSnapshot()
	if again.BaselineTick != 0 {
		t.Fatalf("reconnect must start from a full snapshot, got baseline %d", again.BaselineTick)
	}
	if again.Tick < first.Tick {
		t.Fatalf("resync tick %d went backwards from %d: the match runs on without the player", again.Tick, first.Tick)
	}
}

// Past the grace window the seat is gone and the player lands back in the lobby
// instead of being teleported into a match that moved on without them.
func TestReconnectAfterGraceLosesSeat(t *testing.T) {
	a := testApp(t, 10*time.Millisecond)
	const sessionID = "sess-2"
	const seat = uint32(1)
	startTestRoom(t, a, sessionID, seat)

	c1 := session.NewConn(sessionID, 64)
	join(t, a, c1, sessionID)
	a.onMessage(c1, protocol.Env(pb.MsgType_MSG_TYPE_JOIN_ROOM, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_JoinRoom{JoinRoom: &pb.JoinRoom{
			RoomId: testRoomID, YourId: seat, SessionId: sessionID,
		}}
	}))
	nextOfType(t, c1, pb.MsgType_MSG_TYPE_SNAPSHOT)

	a.onClose(c1)
	c1.Close()
	// Presence stamps Seen in whole seconds, so age it past the window.
	p, _ := a.presence.Get(context.Background(), sessionID)
	p.Seen = time.Now().Add(-time.Hour).Unix()
	if err := a.presence.Set(context.Background(), p); err != nil {
		t.Fatalf("presence.Set: %v", err)
	}

	c2 := session.NewConn(sessionID, 64)
	join(t, a, c2, sessionID)

	select {
	case raw := <-c2.Outbox():
		e, err := protocol.UnmarshalEnv(raw)
		if err == nil && e.Type == pb.MsgType_MSG_TYPE_MATCH_FOUND {
			t.Fatal("expired grace must not resume the match")
		}
	case <-time.After(200 * time.Millisecond):
	}

	got, _ := a.presence.Get(context.Background(), sessionID)
	if got == nil || got.Status != pb.PresenceStatus_PRESENCE_STATUS_ONLINE {
		t.Fatalf("after an expired grace the player should be plain ONLINE, got %v", got)
	}
}

// Joining a room that lives on another node must redirect, not silently fail.
func TestJoinRoomOnAnotherNodeRedirects(t *testing.T) {
	a := testApp(t, time.Minute)
	if err := a.registry.RegisterRoom(context.Background(), &pb.RoomInfo{
		RoomId: "elsewhere", ServerId: "gs-other", PublicAddr: "ws://other:8080/ws",
	}); err != nil {
		t.Fatalf("RegisterRoom: %v", err)
	}

	c := session.NewConn("sess-3", 64)
	join(t, a, c, "sess-3")
	a.onMessage(c, protocol.Env(pb.MsgType_MSG_TYPE_JOIN_ROOM, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_JoinRoom{JoinRoom: &pb.JoinRoom{RoomId: "elsewhere", YourId: 1}}
	}))

	rd := nextOfType(t, c, pb.MsgType_MSG_TYPE_REDIRECT).GetRedirect()
	if rd.Host != "ws://other:8080/ws" {
		t.Fatalf("redirect host = %q", rd.Host)
	}
	if !c.IsHandoff() {
		t.Error("a redirected connection must be marked handoff so closing it does not free the seat")
	}
}

// A dead socket can take until its read deadline to notice it is dead, so the
// replaced connection's onClose routinely lands after the reconnecting one has
// already taken the seat back. It must not tear down what the live connection
// just set up.
func TestLateCloseOfReplacedConnKeepsSeat(t *testing.T) {
	a := testApp(t, time.Minute)
	const sessionID = "sess-late"
	const seat = uint32(1)
	startTestRoom(t, a, sessionID, seat)

	joinRoom := func(c *session.Conn) {
		t.Helper()
		join(t, a, c, sessionID)
		a.onMessage(c, protocol.Env(pb.MsgType_MSG_TYPE_JOIN_ROOM, func(e *pb.Envelope) {
			e.Payload = &pb.Envelope_JoinRoom{JoinRoom: &pb.JoinRoom{
				RoomId: testRoomID, YourId: seat, SessionId: sessionID,
			}}
		}))
		nextOfType(t, c, pb.MsgType_MSG_TYPE_SNAPSHOT)
	}

	c1 := session.NewConn(sessionID, 64)
	joinRoom(c1)

	// The client comes back on a fresh socket; hub.Add files it under the same
	// durable id and closes c1.
	c2 := session.NewConn(sessionID, 64)
	joinRoom(c2)

	// Only now does c1's read loop notice and run its close path.
	a.onClose(c1)
	c1.Close()

	nextOfType(t, c2, pb.MsgType_MSG_TYPE_SNAPSHOT)

	got, _ := a.presence.Get(context.Background(), sessionID)
	if got == nil || got.Status != pb.PresenceStatus_PRESENCE_STATUS_IN_MATCH {
		t.Fatalf("the live connection must stay IN_MATCH, got %v", got)
	}
}
