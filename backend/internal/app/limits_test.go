package app

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/config"
	"github.com/nguyenbatam/arena_game_server/internal/placement"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/replay"
	"github.com/nguyenbatam/arena_game_server/internal/session"
)

func appWithStatic(t *testing.T, cfg config.Static, v config.View) *App {
	t.Helper()
	cfg.NodeID = "gs-test"
	cfg.PublicAddr = "ws://localhost:8080/ws"
	cfg.Role = pb.Role_ROLE_ALL
	a, err := New(cfg, config.NewLive(v, nil))
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	return a
}

func defaultView() config.View {
	return config.View{
		TickRate: 20, RoomSize: 2, MinPlayers: 1, QueueTimeout: time.Second,
		MatchSeconds: 60, SendBuffer: 64, DisconnectGrace: time.Minute,
	}
}

// collect drains what the server sent this connection, decoding envelopes.
func collect(t *testing.T, c *session.Conn, want int, timeout time.Duration) []*pb.Envelope {
	t.Helper()
	var out []*pb.Envelope
	deadline := time.After(timeout)
	for len(out) < want {
		select {
		case raw, ok := <-c.Outbox():
			if !ok {
				return out
			}
			env, err := protocol.UnmarshalEnv(raw)
			if err != nil {
				t.Fatalf("server sent something unparseable: %v", err)
			}
			out = append(out, env)
		case <-deadline:
			return out
		}
	}
	return out
}

func firstError(envs []*pb.Envelope) *pb.Error {
	for _, e := range envs {
		if e.Type == pb.MsgType_MSG_TYPE_ERROR {
			return e.GetError()
		}
	}
	return nil
}

// An established connection can send whatever it likes at whatever rate it
// likes; the per-IP limiters only guard the door. Without a per-connection
// budget the server parses every one of those messages before the room drops
// them, which is the expensive half.
func TestConnectionBudgetClosesAFloodingClient(t *testing.T) {
	a := appWithStatic(t, config.Static{ConnMsgRate: 10, ConnMsgBurst: 10}, defaultView())
	c := session.NewConn("flood", 256)
	a.hub.Add(c)

	ping := protocol.Env(pb.MsgType_MSG_TYPE_PING, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Ping{Ping: &pb.Ping{Nonce: 1}}
	})
	for i := 0; i < 50; i++ {
		a.onMessage(c, ping)
	}
	if !c.Closed() {
		t.Fatal("a client 5x over its message budget is still connected")
	}
	if err := firstError(collect(t, c, 64, time.Second)); err == nil ||
		err.Code != pb.ErrorCode_ERROR_CODE_RATE_LIMITED {
		t.Fatalf("expected a rate-limited error, got %+v", err)
	}
}

// A normal client must never notice the budget exists. Twenty inputs a second
// is what the arena sends; the default allowance is six times that.
func TestConnectionBudgetLeavesANormalClientAlone(t *testing.T) {
	a := appWithStatic(t, config.Static{ConnMsgRate: 120, ConnMsgBurst: 240}, defaultView())
	c := session.NewConn("normal", 256)
	a.hub.Add(c)

	ping := protocol.Env(pb.MsgType_MSG_TYPE_PING, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Ping{Ping: &pb.Ping{Nonce: 1}}
	})
	for i := 0; i < 100; i++ {
		a.onMessage(c, ping)
	}
	if c.Closed() {
		t.Fatal("a client sending at a normal rate was disconnected")
	}
}

// A few big frames can cost more than many small ones, and a message counter
// alone would never see them.
func TestConnectionBudgetCountsBytesToo(t *testing.T) {
	a := appWithStatic(t, config.Static{ConnMsgRate: 1000, ConnByteRate: 4096, ConnByteBurst: 4096}, defaultView())
	c := session.NewConn("fat", 256)
	a.hub.Add(c)

	// Well inside the message allowance, far past the byte allowance.
	big := make([]byte, 2048)
	for i := 0; i < 8; i++ {
		a.onMessage(c, big)
	}
	if !c.Closed() {
		t.Fatal("16 KB of frames against a 4 KB/s budget did not close the connection")
	}
}

// Version is checked before anything else is believed, so an incompatible
// client is told to update rather than left to desync mid-match.
func TestHelloRejectsAnUnsupportedProtocolVersion(t *testing.T) {
	a := appWithStatic(t, config.Static{}, defaultView())
	c := session.NewConn("old", 32)
	a.hub.Add(c)

	a.onHello(c, &pb.Hello{Name: "ancient", ProtocolVersion: protocol.Version + 1})

	if err := firstError(collect(t, c, 8, time.Second)); err == nil ||
		err.Code != pb.ErrorCode_ERROR_CODE_VERSION_MISMATCH {
		t.Fatalf("expected a version-mismatch error, got %+v", err)
	}
	if !c.Closed() {
		t.Fatal("an incompatible client was left connected")
	}
}

// A client from before the field existed keeps working: zero means "version 1",
// which is the contract it was built against.
func TestHelloAcceptsAClientWithNoVersion(t *testing.T) {
	a := appWithStatic(t, config.Static{}, defaultView())
	c := session.NewConn("legacy", 32)
	a.hub.Add(c)

	a.onHello(c, &pb.Hello{Name: "legacy"})

	if c.Closed() {
		t.Fatal("a client that sent no version was turned away")
	}
	envs := collect(t, c, 1, time.Second)
	if len(envs) == 0 || envs[0].Type != pb.MsgType_MSG_TYPE_WELCOME {
		t.Fatalf("expected a welcome, got %+v", envs)
	}
	if got := envs[0].GetWelcome().ProtocolVersion; got != protocol.Version {
		t.Fatalf("welcome carried version %d, want %d", got, protocol.Version)
	}
}

// A node at its ceiling puts the players back in the queue rather than opening
// a room it cannot tick. Placement avoids full nodes already, but its view is a
// heartbeat old and a burst can land inside that window.
func TestNodeAtCapacityRefusesTheRoomAndRequeues(t *testing.T) {
	a := appWithStatic(t, config.Static{MaxRooms: 1}, defaultView())
	defer a.rooms.StopAll()
	ctx := context.Background()

	a.startRoom(placement.NewJob(&pb.RoomRequest{
		RoomId: "room-1", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Seats: []*pb.Seat{{ConnId: "p1", PlayerId: 1, Name: "p1"}},
	}))
	if got := a.rooms.Count(); got != 1 {
		t.Fatalf("rooms = %d after the first placement, want 1", got)
	}

	a.startRoom(placement.NewJob(&pb.RoomRequest{
		RoomId: "room-2", Seed: 2, TickRate: 20, MatchTicks: 1 << 20,
		Seats: []*pb.Seat{{ConnId: "p2", PlayerId: 1, Name: "p2"}},
	}))
	if got := a.rooms.Count(); got != 1 {
		t.Fatalf("rooms = %d, want the second placement refused", got)
	}
	if n, _ := a.queue.Depth(ctx); n != 1 {
		t.Fatalf("queue depth = %d, want the refused player back in the queue", n)
	}
}

// A refusal is the server's problem. Sending the player back to the end of the
// queue would charge them a second full wait for it — and the room directory
// entry the matchmaker wrote has to go too, or it points at a node that never
// opened the room for the next half hour.
func TestRefusedPlacementKeepsQueuePositionAndCleansTheDirectory(t *testing.T) {
	a := appWithStatic(t, config.Static{MaxRooms: 1}, defaultView())
	defer a.rooms.StopAll()
	ctx := context.Background()

	a.startRoom(placement.NewJob(&pb.RoomRequest{
		RoomId: "room-1", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Seats: []*pb.Seat{{ConnId: "p1", PlayerId: 1, Name: "p1"}},
	}))

	queuedAt := time.Now().Add(-90 * time.Second).UnixMilli()
	if err := a.registry.RegisterRoom(ctx, &pb.RoomInfo{
		RoomId: "room-2", ServerId: a.cfg.NodeID, PublicAddr: a.cfg.PublicAddr,
	}); err != nil {
		t.Fatal(err)
	}
	a.startRoom(placement.NewJob(&pb.RoomRequest{
		RoomId: "room-2", Seed: 2, TickRate: 20, MatchTicks: 1 << 20,
		Seats: []*pb.Seat{{ConnId: "waited", PlayerId: 1, Name: "waited", QueuedAt: queuedAt}},
	}))

	m, err := a.queue.TryForm(ctx, a.matchRules())
	if err != nil {
		t.Fatal(err)
	}
	if m == nil || len(m.Players) != 1 {
		t.Fatalf("the refused player is not back in the queue: %+v", m)
	}
	if got := m.Players[0].QueuedAt; got != queuedAt {
		t.Fatalf("requeued with queued_at %d, want the original %d — they were sent to the back", got, queuedAt)
	}

	info, err := a.registry.GetRoom(ctx, "room-2")
	if err != nil {
		t.Fatal(err)
	}
	if info != nil {
		t.Fatal("the directory still points at a room that was never opened")
	}
}

// SIGTERM starts a drain. A node that keeps opening rooms through it never
// reaches zero, and the players in those new matches are cut off seconds later.
func TestDrainingNodeRefusesNewRooms(t *testing.T) {
	a := appWithStatic(t, config.Static{}, defaultView())
	defer a.rooms.StopAll()
	a.draining.Store(true)

	a.startRoom(placement.NewJob(&pb.RoomRequest{
		RoomId: "late", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Seats: []*pb.Seat{{ConnId: "p1", PlayerId: 1, Name: "p1"}},
	}))
	if got := a.rooms.Count(); got != 0 {
		t.Fatalf("a draining node opened %d room(s)", got)
	}
	if n, _ := a.queue.Depth(context.Background()); n != 1 {
		t.Fatalf("queue depth = %d, want the player requeued", n)
	}
}

// The job reaper requeues anything taken but not acked in time, so a game
// server can be handed the same room twice. The second delivery must not open a
// second recording: rooms.Start hands back the room that is already running and
// drops the recorder on the floor, which then holds one of the MAX_REPLAYS
// sample slots for the life of the process — until nothing records at all.
func TestDuplicateRoomJobDoesNotLeakARecordingSlot(t *testing.T) {
	dir := t.TempDir()
	// Two slots, so the duplicate's recorder is actually handed out — with one
	// slot the sample is already full and the leak cannot show itself.
	a := appWithStatic(t, config.Static{ReplayDir: dir, MaxReplays: 2}, defaultView())
	defer a.rooms.StopAll()

	req := &pb.RoomRequest{
		RoomId: "twice", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Seats: []*pb.Seat{{ConnId: "p1", PlayerId: 1, Name: "p1"}},
	}
	a.startRoom(placement.NewJob(req))
	a.startRoom(placement.NewJob(req))

	if got := a.rooms.Count(); got != 1 {
		t.Fatalf("rooms = %d after the same job twice, want 1", got)
	}
	// Once the room has ended, every slot must be back: one was returned by the
	// recording that closed, and the other was never taken at all.
	a.rooms.StopAll()
	deadline := time.Now().Add(3 * time.Second)
	for a.rooms.Count() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	for i := 0; i < 2; i++ {
		rec := a.replays.Begin(replay.Header{RoomID: fmt.Sprintf("next-%d", i), Seed: 2, TickRate: 20})
		if rec == nil {
			t.Fatalf("slot %d never came back — the duplicate job leaked it", i)
		}
		t.Cleanup(func() { _ = rec.Close(1, 1) })
	}
}
