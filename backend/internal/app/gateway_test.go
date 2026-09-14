package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/config"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/ratelimit"
	"github.com/nguyenbatam/arena_game_server/internal/room"
	"github.com/nguyenbatam/arena_game_server/internal/session"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

func drain(c *session.Conn) {
	go func() {
		for range c.Outbox() {
		}
	}()
}

// ---------------------------------------------------------------------------
// Concurrency: the room's tick goroutine and the connection goroutine both
// touch the same Conn.

// Under -race this pins that a room broadcasting into a connection while that
// connection is being torn down is not a data race.
func TestJoinAndCloseUnderBroadcast(t *testing.T) {
	a := testApp(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		roomID := "race-room"
		connID := "c-race"
		r := a.rooms.Start(ctx, room.Params{
			ID: roomID, Seed: 3, TickRate: 100, MatchTicks: 1 << 20,
			Roster: []sim.Player{{ID: 1}, {ID: 1000, Bot: true}},
			Seats:  map[string]uint32{connID: 1},
		})
		c := session.NewConn(connID, 8)
		drain(c)
		a.hub.Add(c)
		a.joinRoom(c, roomID, 1)

		wg.Add(1)
		go func() {
			defer wg.Done()
			a.onClose(c)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_, _ = c.Room()
				_ = c.LastTick()
			}
		}()
		wg.Wait()
		r.Stop()
		c.Close()
		a.rooms.StopAll()
	}
}

// An assignment arrives on the notify subscriber goroutine while the reader
// goroutine is handling messages on the same connection.
func TestAssignmentAndInputConcurrently(t *testing.T) {
	a := testApp(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer a.rooms.StopAll()

	c := session.NewConn("c-assign", 32)
	drain(c)
	a.hub.Add(c)
	a.rooms.Start(ctx, room.Params{
		ID: "assign-room", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}, {ID: 1000, Bot: true}},
		Seats:  map[string]uint32{"c-assign": 1},
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.onAssignment(&pb.Assignment{ConnId: "c-assign", PlayerId: 1, RoomId: "assign-room", TickRate: 100})
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			a.onMessage(c, protocol.Env(pb.MsgType_MSG_TYPE_INPUT, func(e *pb.Envelope) {
				e.Payload = &pb.Envelope_Input{Input: &pb.Input{Mx: 1, Seq: uint32(i + 1)}}
			}))
		}
	}()
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Seat binding

// An input must be attributed to the seat the server bound, never to whatever
// player id the client put in the message. Checked through the world the room
// actually simulates: the spoofed seat must not move.
func TestInputCannotSpoofAnotherSeat(t *testing.T) {
	a := testApp(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer a.rooms.StopAll()

	a.rooms.Start(ctx, room.Params{
		ID: "spoof-room", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}, {ID: 2}},
		Seats:  map[string]uint32{"attacker": 1},
	})
	c := session.NewConn("attacker", 256)
	a.hub.Add(c)
	a.joinRoom(c, "spoof-room", 1)

	for i := 1; i <= 20; i++ {
		a.onMessage(c, protocol.Env(pb.MsgType_MSG_TYPE_INPUT, func(e *pb.Envelope) {
			e.Payload = &pb.Envelope_Input{Input: &pb.Input{PlayerId: 2, Mx: 1, Seq: uint32(i)}}
		}))
		time.Sleep(5 * time.Millisecond)
	}

	seqs := latestSeqs(t, c)
	if seqs[2] != 0 {
		t.Fatalf("player 2 advanced to seq %d from a message the attacker sent", seqs[2])
	}
	if seqs[1] == 0 {
		t.Fatal("the attacker's own seat never advanced, so the test proved nothing")
	}
}

// A connection with no seat in a room must not be able to submit input to it.
func TestInputBeforeJoinIsDropped(t *testing.T) {
	a := testApp(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer a.rooms.StopAll()

	a.rooms.Start(ctx, room.Params{
		ID: "nojoin-room", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}},
		Seats:  map[string]uint32{"someone-else": 1},
	})
	stranger := session.NewConn("stranger", 32)
	drain(stranger)
	a.hub.Add(stranger)
	for i := 1; i <= 20; i++ {
		a.onMessage(stranger, protocol.Env(pb.MsgType_MSG_TYPE_INPUT, func(e *pb.Envelope) {
			e.Payload = &pb.Envelope_Input{Input: &pb.Input{Mx: 1, Seq: uint32(i)}}
		}))
	}

	// Watch the room from a connection that is actually seated.
	seated := session.NewConn("someone-else", 256)
	a.hub.Add(seated)
	a.joinRoom(seated, "nojoin-room", 1)
	time.Sleep(60 * time.Millisecond)

	if seqs := latestSeqs(t, seated); seqs[1] != 0 {
		t.Fatalf("an unjoined connection drove seat 1 to seq %d", seqs[1])
	}
}

// A connection whose conn id is not in the seat map must be refused.
func TestJoinRoomRefusesAnUnseatedConn(t *testing.T) {
	a := testApp(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer a.rooms.StopAll()

	a.rooms.Start(ctx, room.Params{
		ID: "seated-room", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Roster: []sim.Player{{ID: 1}},
		Seats:  map[string]uint32{"rightful": 1},
	})
	c := session.NewConn("interloper", 32)
	a.hub.Add(c)
	a.joinRoom(c, "seated-room", 1)

	e := nextOfType(t, c, pb.MsgType_MSG_TYPE_ERROR)
	if e.GetError().Code != pb.ErrorCode_ERROR_CODE_ROOM_NOT_FOUND {
		t.Fatalf("got %v", e.GetError().Code)
	}
	if roomID, seat := c.Room(); roomID != "" || seat != 0 {
		t.Fatalf("refused conn was still bound to room=%q seat=%d", roomID, seat)
	}
}

// ---------------------------------------------------------------------------
// Admission and rate limiting

func TestHelloRateLimitClosesTheConnection(t *testing.T) {
	a := testApp(t, time.Minute)
	a.helloRL = newTestWindow(2)

	var last *session.Conn
	for i := 0; i < 4; i++ {
		c := session.NewConn(session.NewID(), 32)
		c.RemoteIP = "203.0.113.7"
		drain(c)
		a.hub.Add(c)
		a.onHello(c, &pb.Hello{Name: "flood"})
		last = c
	}
	if !last.Closed() {
		t.Fatal("connection past the hello limit was not closed")
	}
}

// Clients from different addresses must not share a bucket.
func TestHelloRateLimitIsPerAddress(t *testing.T) {
	a := testApp(t, time.Minute)
	a.helloRL = newTestWindow(1)

	for _, ip := range []string{"198.51.100.1", "198.51.100.2", "198.51.100.3"} {
		c := session.NewConn(session.NewID(), 32)
		c.RemoteIP = ip
		drain(c)
		a.hub.Add(c)
		a.onHello(c, &pb.Hello{Name: "n"})
		if c.Closed() {
			t.Fatalf("first hello from %s was limited", ip)
		}
	}
}

// MaxCCU is a ceiling, not a target: the connection that would exceed it is
// refused, and the one that reaches it is not.
func TestMaxCCUIsAnInclusiveCeiling(t *testing.T) {
	a := testAppWithView(t, config.View{
		TickRate: 20, RoomSize: 2, MinPlayers: 1, MatchSeconds: 60, SendBuffer: 32, MaxCCU: 2,
	})
	conns := make([]*session.Conn, 0, 3)
	for i := 0; i < 3; i++ {
		c := session.NewConn(session.NewID(), 32)
		drain(c)
		a.hub.Add(c)
		a.onHello(c, &pb.Hello{Name: "p"})
		conns = append(conns, c)
	}
	if conns[0].Closed() || conns[1].Closed() {
		t.Fatal("a connection inside MaxCCU was refused")
	}
	if !conns[2].Closed() {
		t.Fatalf("MaxCCU=2 admitted a third connection (hub holds %d)", a.hub.Count())
	}
}

// ---------------------------------------------------------------------------
// Queue

func TestJoinQueueIsRefusedOnAGameServer(t *testing.T) {
	a := testApp(t, time.Minute)
	a.cfg.Role = pb.Role_ROLE_GAME_SERVER
	c := session.NewConn("c1", 32)
	a.hub.Add(c)
	a.onJoinQueue(c)

	e := nextOfType(t, c, pb.MsgType_MSG_TYPE_ERROR)
	if e.GetError().Code != pb.ErrorCode_ERROR_CODE_WRONG_ROLE {
		t.Fatalf("got %v", e.GetError().Code)
	}
}

func TestJoinQueueIsIgnoredWhileInARoom(t *testing.T) {
	a := testApp(t, time.Minute)
	c := session.NewConn("c1", 32)
	drain(c)
	a.hub.Add(c)
	c.BindRoom("already-playing", 1)
	a.onJoinQueue(c)

	if n, _ := a.queue.Depth(context.Background()); n != 0 {
		t.Fatalf("a player already in a room was queued (depth=%d)", n)
	}
}

func TestJoinQueueEnqueuesAndReportsQueued(t *testing.T) {
	a := testApp(t, time.Minute)
	c := session.NewConn("c1", 32)
	a.hub.Add(c)
	c.SetName("alice")
	a.onJoinQueue(c)

	nextOfType(t, c, pb.MsgType_MSG_TYPE_QUEUED)
	if n, _ := a.queue.Depth(context.Background()); n != 1 {
		t.Fatalf("queue depth = %d, want 1", n)
	}
}

// A disconnect must take the player out of the queue, or the matchmaker seats a
// connection that is already gone.
func TestCloseRemovesFromQueue(t *testing.T) {
	a := testApp(t, time.Minute)
	c := session.NewConn("c1", 32)
	drain(c)
	a.hub.Add(c)
	a.onJoinQueue(c)
	a.onClose(c)

	if n, _ := a.queue.Depth(context.Background()); n != 0 {
		t.Fatalf("queue still holds %d after disconnect", n)
	}
}

// ---------------------------------------------------------------------------
// Handoff

// A connection being handed to another node must not have its presence torn
// down by the local close — that presence is what the next node resumes from.
func TestHandoffCloseLeavesPresenceAlone(t *testing.T) {
	a := testApp(t, time.Minute)
	c := session.NewConn("c-handoff", 32)
	drain(c)
	a.hub.Add(c)
	_ = a.presence.Set(context.Background(), &pb.Presence{
		PlayerId: "c-handoff", RoomId: "far-room", SeatId: 4,
		Status: pb.PresenceStatus_PRESENCE_STATUS_IN_MATCH,
	})
	c.SetHandoff(true)
	a.onClose(c)

	got, _ := a.presence.Get(context.Background(), "c-handoff")
	if got == nil || got.Status != pb.PresenceStatus_PRESENCE_STATUS_IN_MATCH {
		t.Fatalf("handoff wiped the presence it was meant to preserve: %v", got)
	}
}

// ---------------------------------------------------------------------------
// Admin surface

func TestAdminConfigRequiresATokenInProduction(t *testing.T) {
	a := testApp(t, time.Minute)
	a.cfg.Env = "production"
	a.cfg.AdminToken = "s3cret"

	req := httptest.NewRequest(http.MethodGet, "/admin/config", nil)
	w := httptest.NewRecorder()
	a.handleConfig(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET returned %d", w.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/config", nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	w = httptest.NewRecorder()
	a.handleConfig(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("authenticated GET returned %d", w.Code)
	}
}

func TestAdminConfigPutAlwaysNeedsAuthorization(t *testing.T) {
	a := testApp(t, time.Minute)
	a.cfg.AdminToken = "s3cret" // even outside production
	req := httptest.NewRequest(http.MethodPut, "/admin/config", strings.NewReader(`{"tick_rate":60}`))
	w := httptest.NewRecorder()
	a.handleConfig(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated PUT returned %d", w.Code)
	}
}

// A hot config push must be held to the same bounds as the environment, or a
// single PUT walks room size past what fits in one datagram.
func TestAdminConfigClampsRoomSize(t *testing.T) {
	a := testApp(t, time.Minute)
	req := httptest.NewRequest(http.MethodPut, "/admin/config", strings.NewReader(`{"room_size":500}`))
	w := httptest.NewRecorder()
	a.handleConfig(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT returned %d: %s", w.Code, w.Body.String())
	}
	var got dynDTO
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.RoomSize > config.MaxRoomSize {
		t.Fatalf("room_size accepted as %d, above MaxRoomSize %d", got.RoomSize, config.MaxRoomSize)
	}
	if a.dyn.Get().RoomSize > config.MaxRoomSize {
		t.Fatalf("live view holds room_size %d", a.dyn.Get().RoomSize)
	}
}

// Removing the CCU ceiling has to survive the round trip through the admin
// surface, which is a GET, an edit and a PUT of the same document. It did not:
// the DTO carried max_ccu 0, ViewFromPB read 0 as "unspecified", and the
// gateway went on enforcing whatever MAX_CCU had been set to at boot.
func TestAdminConfigCanRemoveTheCCUCeiling(t *testing.T) {
	a := testAppWithView(t, config.View{
		TickRate: 20, RoomSize: 4, MinPlayers: 1, SendBuffer: 32, MaxCCU: 12000,
	})
	if got := a.dyn.Get().MaxCCU; got != 12000 {
		t.Fatalf("test app started with max_ccu %d; nothing to remove", got)
	}

	req := httptest.NewRequest(http.MethodPut, "/admin/config", strings.NewReader(`{"max_ccu":0}`))
	w := httptest.NewRecorder()
	a.handleConfig(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT returned %d: %s", w.Code, w.Body.String())
	}

	var got dynDTO
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.MaxCCU != 0 {
		t.Errorf("PUT echoed max_ccu %d, want 0", got.MaxCCU)
	}
	if live := a.dyn.Get().MaxCCU; live != 0 {
		t.Errorf("live view still holds max_ccu %d", live)
	}

	// And the GET a follow-up read would make says the same thing.
	req = httptest.NewRequest(http.MethodGet, "/admin/config", nil)
	w = httptest.NewRecorder()
	a.handleConfig(w, req)
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.MaxCCU != 0 {
		t.Errorf("GET reported max_ccu %d after the ceiling was removed", got.MaxCCU)
	}
}

// Editing one field must not disturb another. The DTO is pre-filled from the
// live view before the body is unmarshalled precisely so an omitted field keeps
// its value, and the "off" encoding has to travel that path intact too.
func TestAdminConfigLeavesAnAbsentCeilingAlone(t *testing.T) {
	a := testAppWithView(t, config.View{
		TickRate: 20, RoomSize: 4, MinPlayers: 1, SendBuffer: 32, MaxCCU: 12000,
	})
	req := httptest.NewRequest(http.MethodPut, "/admin/config", strings.NewReader(`{"max_ccu":0}`))
	a.handleConfig(httptest.NewRecorder(), req)

	req = httptest.NewRequest(http.MethodPut, "/admin/config", strings.NewReader(`{"tick_rate":30}`))
	w := httptest.NewRecorder()
	a.handleConfig(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT returned %d: %s", w.Code, w.Body.String())
	}
	v := a.dyn.Get()
	if v.TickRate != 30 {
		t.Errorf("tick_rate = %d, want 30", v.TickRate)
	}
	if v.MaxCCU != 0 {
		t.Errorf("unrelated PUT restored the ceiling to %d", v.MaxCCU)
	}
}

func TestReadyzFailsWhileDraining(t *testing.T) {
	a := testApp(t, time.Minute)
	a.ready.Store(true)

	w := httptest.NewRecorder()
	a.handleReadyz(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("ready server returned %d", w.Code)
	}

	a.draining.Store(true)
	w = httptest.NewRecorder()
	a.handleReadyz(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining server returned %d, want 503", w.Code)
	}
}

func TestLoginRejectsNonPost(t *testing.T) {
	a := testApp(t, time.Minute)
	w := httptest.NewRecorder()
	a.handleLogin(w, httptest.NewRequest(http.MethodGet, "/auth/login", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("got %d", w.Code)
	}
}

func TestLoginIs404WhenAuthIsDisabled(t *testing.T) {
	a := testApp(t, time.Minute)
	w := httptest.NewRecorder()
	a.handleLogin(w, httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(`{"name":"x"}`)))
	if w.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404 when JWT is off", w.Code)
	}
}

func TestStatsReportsLiveConfig(t *testing.T) {
	a := testApp(t, time.Minute)
	w := httptest.NewRecorder()
	a.handleStats(w, httptest.NewRequest(http.MethodGet, "/stats", nil))
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["node"] != "gs-test" || got["role"] != "all" {
		t.Fatalf("stats = %v", got)
	}
}

// ---------------------------------------------------------------------------
// helpers

func testAppWithView(t *testing.T, v config.View) *App {
	t.Helper()
	a, err := New(config.Static{
		NodeID: "gs-test", PublicAddr: "ws://localhost:8080/ws", Role: pb.Role_ROLE_ALL,
	}, config.NewLive(v, nil))
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	return a
}

func newTestWindow(limit int) *ratelimit.Window {
	return ratelimit.NewWindow(limit, time.Minute)
}

// latestSeqs drains what the room has sent this connection and rebuilds the
// current per-player input sequence, applying deltas the way a client would.
func latestSeqs(t *testing.T, c *session.Conn) map[uint32]uint32 {
	t.Helper()
	var state *pb.Snapshot
	deadline := time.After(2 * time.Second)
	for {
		select {
		case raw, ok := <-c.Outbox():
			if !ok {
				return seqsOf(state)
			}
			e, err := protocol.UnmarshalEnv(raw)
			if err != nil || e.Type != pb.MsgType_MSG_TYPE_SNAPSHOT {
				continue
			}
			if next := room.ApplyDelta(state, e.GetSnapshot()); next != nil {
				state = next
			}
		case <-time.After(80 * time.Millisecond):
			return seqsOf(state)
		case <-deadline:
			return seqsOf(state)
		}
	}
}

func seqsOf(s *pb.Snapshot) map[uint32]uint32 {
	out := map[uint32]uint32{}
	if s == nil {
		return out
	}
	for _, p := range s.Players {
		out[p.Id] = p.Seq
	}
	return out
}
