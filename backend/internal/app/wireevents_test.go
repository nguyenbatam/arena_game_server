package app

import (
	"math"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/placement"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
)

// Gameplay events have to survive the whole way out: simulation, ring, delta
// encoder, protobuf, WebSocket frame.
//
// Every other test in this repo stops one layer short of that — the room tests
// read the message the fanout produced, and the port tests hand web/pb.js a
// snapshot that is already decoded. Neither would notice a field that is
// produced, encoded and then never put on a socket.
func TestGameplayEventsReachARealWebSocketClient(t *testing.T) {
	a := testApp(t, time.Minute)
	srv := httptest.NewServer(a.routes())
	t.Cleanup(srv.Close)

	c, _, err := websocket.DefaultDialer.Dial("ws"+srv.URL[4:]+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if err := c.WriteMessage(websocket.BinaryMessage, protocol.Env(pb.MsgType_MSG_TYPE_HELLO, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Hello{Hello: &pb.Hello{Name: "spy", ProtocolVersion: protocol.Version}}
	})); err != nil {
		t.Fatal(err)
	}

	// The WELCOME carries the durable session id, which is the key the room's
	// seat map is built on.
	var sessionID string
	deadline := time.Now().Add(5 * time.Second)
	for sessionID == "" && time.Now().Before(deadline) {
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, raw, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("reading welcome: %v", err)
		}
		env, err := protocol.UnmarshalEnv(raw)
		if err != nil {
			t.Fatal(err)
		}
		if w := env.GetWelcome(); w != nil {
			sessionID = w.SessionId
		}
	}
	if sessionID == "" {
		t.Fatal("no WELCOME arrived")
	}

	// A crowded match: this connection holds seat 1, seat 2 is a player who
	// never connects, and six bots fight each other in the middle of it.
	//
	// The crowd is what makes the test reliable. Landing a shot on one
	// wandering bot from a client that aims off a snapshot is a coin flip; six
	// bots firing at each other on a spawn ring produce hits continuously,
	// which is the event this is really about.
	req := &pb.RoomRequest{
		RoomId: "r-wire-events", Seed: 5, TickRate: 50, MatchTicks: 1 << 20,
		Seats: []*pb.Seat{
			{ConnId: sessionID, PlayerId: 1},
			{ConnId: "never-connects", PlayerId: 2},
		},
		Bots: 6,
	}
	a.startRoom(placement.NewJob(req))
	r := a.rooms.Get(req.RoomId)
	if r == nil {
		t.Fatal("the room did not start")
	}
	t.Cleanup(r.Stop)
	a.joinRoom(a.hub.Get(sessionID), req.RoomId, 1)

	// And one event that does not depend on anybody's aim: seat 2 is gone. Its
	// avatar lingers and then despawns, which is a DEPART on the wire at a time
	// this test can actually wait for.
	r.Leave(2)

	seen := map[pb.GameEventKind]int{}
	lastEventTick := uint32(0)
	seq := uint32(0)
	ack := uint32(0)
	snapshots := 0
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if seen[pb.GameEventKind_GAME_EVENT_KIND_HIT] > 0 && seen[pb.GameEventKind_GAME_EVENT_KIND_DEPART] > 0 {
			break
		}
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, raw, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("reading snapshot: %v", err)
		}
		env, err := protocol.UnmarshalEnv(raw)
		if err != nil {
			t.Fatal(err)
		}
		s := env.GetSnapshot()
		if s == nil {
			continue
		}
		snapshots++
		for _, e := range s.Events {
			if e.Tick <= lastEventTick {
				continue
			}
			seen[e.Kind]++
		}
		if s.Tick > lastEventTick {
			lastEventTick = s.Tick
		}
		ack = s.Tick

		// Aim at wherever the bot is and fire. The room is on this process, so
		// reading the bot's position through a snapshot is what the client
		// would do anyway.
		var aim int32
		var me, bot *pb.PlayerSnap
		for _, p := range s.Players {
			if p.Id == 1 {
				me = p
			} else {
				bot = p
			}
		}
		if me != nil && bot != nil {
			// Floating point is fine here and only here: this is the client
			// half, and nothing it computes reaches the simulation except as a
			// whole-degree aim the server clamps and re-derives everything from.
			deg := math.Atan2(float64(bot.Y-me.Y), float64(bot.X-me.X)) * 180 / math.Pi
			if deg < 0 {
				deg += 360
			}
			aim = int32(math.Round(deg))
		}
		seq++
		if err := c.WriteMessage(websocket.BinaryMessage, protocol.Env(pb.MsgType_MSG_TYPE_INPUT, func(e *pb.Envelope) {
			e.Payload = &pb.Envelope_Input{Input: &pb.Input{
				PlayerId: 1, Fire: true, Aim: aim, Seq: seq, AckTick: ack,
			}}
		})); err != nil {
			t.Fatalf("sending input: %v", err)
		}
	}

	t.Logf("%d snapshots, events over a real socket: %v", snapshots, seen)
	if snapshots == 0 {
		t.Fatal("no snapshots arrived at all; the client never joined the match")
	}
	if seen[pb.GameEventKind_GAME_EVENT_KIND_DEPART] == 0 {
		t.Errorf("the departure of seat 2 never reached the socket; saw %v", seen)
	}
	if seen[pb.GameEventKind_GAME_EVENT_KIND_HIT] == 0 {
		t.Errorf("no hit reached the socket in %d snapshots; saw %v", snapshots, seen)
	}
}
