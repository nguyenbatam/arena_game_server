package app

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/config"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/room"
	"github.com/nguyenbatam/arena_game_server/internal/session"
	"github.com/nguyenbatam/arena_game_server/internal/turn"
)

// nodeOn builds a node of a given role on the shared Redis, the way
// docker-compose.scale.yml wires one: a gateway, a matchmaker and two
// gameservers, all pointed at the same store.
//
// gatewayOn above is the gateway-only version. This takes the role because the
// paths worth testing across nodes — placement, the room directory, the ack a
// handed-off player sends to a server it has only just met — belong to the
// other two roles.
func nodeOn(t *testing.T, addr, nodeID string, role pb.Role) *App {
	t.Helper()
	cfg := config.Static{
		NodeID:     nodeID,
		PublicAddr: "ws://" + nodeID + ":8080/ws",
		Role:       role,
		RedisAddr:  addr,
		TurnLimit:  time.Minute,
	}
	dyn := config.NewLive(config.View{
		TickRate: 20, RoomSize: 2, MinPlayers: 1, QueueTimeout: time.Second,
		MatchSeconds: 120, SendBuffer: 64, DisconnectGrace: time.Minute,
	}, nil)
	a, err := New(cfg, dyn)
	if err != nil {
		t.Fatalf("app.New(%s): %v", nodeID, err)
	}
	return a
}

func seatedRequest(roomID, connID string) *pb.RoomRequest {
	return &pb.RoomRequest{
		RoomId: roomID, Seed: 42, TickRate: 20, MatchTicks: 1 << 20,
		Seats: []*pb.Seat{{ConnId: connID, PlayerId: 1}},
	}
}

// ---------------------------------------------------------------------------
// Placement across a matchmaker and two gameservers.

// A job is delivered to exactly one gameserver, and acking it there retires it
// for good — the other node must never pick it up, and the reaper must not
// resurrect it.
//
// This is the cross-node half of the Ack-by-token fix. Ack used to name the
// list entry by re-marshalling the request, and protobuf does not promise that
// an equal message encodes to identical bytes: a mismatch left the job in the
// processing list, the reaper called it abandoned, and the same match was
// started twice — on two different gameservers, with the players split between
// them.
func TestAckedJobIsNeverRedeliveredToEitherGameserver(t *testing.T) {
	mr := miniredis.RunT(t)
	mm := nodeOn(t, mr.Addr(), "mm-1", pb.Role_ROLE_MATCHMAKER)
	gs1 := nodeOn(t, mr.Addr(), "gs-1", pb.Role_ROLE_GAME_SERVER)
	gs2 := nodeOn(t, mr.Addr(), "gs-2", pb.Role_ROLE_GAME_SERVER)
	ctx := context.Background()

	req := seatedRequest("r-cross-1", "acct-1")
	if err := mm.jobs.Enqueue(ctx, "gs-1", req); err != nil {
		t.Fatal(err)
	}

	job, err := gs1.jobs.Take(ctx, "gs-1")
	if err != nil || job == nil {
		t.Fatalf("gs-1 Take: %+v %v", job, err)
	}
	if job.Req.RoomId != "r-cross-1" {
		t.Fatalf("gs-1 took %q", job.Req.RoomId)
	}
	// The handler mutates the request on its way through — publishAssignments
	// and startRoom both read it, and a seat may be rewritten.
	job.Req.Seats[0].Name = "renamed"
	if err := gs1.jobs.Ack(ctx, "gs-1", job); err != nil {
		t.Fatal(err)
	}

	// Nothing left in flight anywhere.
	n, err := gs1.jobs.ReapStale(ctx, "gs-1", time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("the reaper resurrected %d acked job(s)", n)
	}
	// And neither node has anything to take.
	for name, node := range map[string]*App{"gs-1": gs1, "gs-2": gs2} {
		takeCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		again, err := node.jobs.Take(takeCtx, name)
		cancel()
		if err == nil && again != nil {
			t.Errorf("%s was handed room %q a second time", name, again.Req.RoomId)
		}
	}
}

// Each gameserver has its own queue: a job placed on one is never visible to
// the other, so two nodes polling at the same moment cannot both start the
// same match with the players split between them.
func TestAJobForOneGameserverIsNeverVisibleToTheOther(t *testing.T) {
	mr := miniredis.RunT(t)
	mm := nodeOn(t, mr.Addr(), "mm-1", pb.Role_ROLE_MATCHMAKER)
	gs1 := nodeOn(t, mr.Addr(), "gs-1", pb.Role_ROLE_GAME_SERVER)
	gs2 := nodeOn(t, mr.Addr(), "gs-2", pb.Role_ROLE_GAME_SERVER)
	ctx := context.Background()

	if err := mm.jobs.Enqueue(ctx, "gs-2", seatedRequest("r-for-gs2", "acct-1")); err != nil {
		t.Fatal(err)
	}

	// The node it was not addressed to finds nothing and gives up on its own.
	takeCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	stray, err := gs1.jobs.Take(takeCtx, "gs-1")
	cancel()
	if err == nil && stray != nil {
		t.Fatalf("gs-1 was handed room %q, which was placed on gs-2", stray.Req.RoomId)
	}

	job, err := gs2.jobs.Take(ctx, "gs-2")
	if err != nil || job == nil || job.Req.RoomId != "r-for-gs2" {
		t.Fatalf("gs-2 did not get its own job: %+v %v", job, err)
	}
	if err := gs2.jobs.Ack(ctx, "gs-2", job); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// The room directory: which of the two gameservers holds the match.

// A gateway points a player at the gameserver that actually has their room,
// and stops pointing anywhere once the match is over.
func TestGatewayRedirectsToTheGameserverHoldingTheRoom(t *testing.T) {
	mr := miniredis.RunT(t)
	gw := nodeOn(t, mr.Addr(), "gw-1", pb.Role_ROLE_GATEWAY)
	ctx := context.Background()

	// The match was placed on the second gameserver.
	if err := gw.registry.RegisterRoom(ctx, &pb.RoomInfo{
		RoomId: "r-on-gs2", ServerId: "gs-2", PublicAddr: "ws://gs-2:8080/ws", Seed: 7,
	}); err != nil {
		t.Fatal(err)
	}

	c := session.NewConn("acct-1", 64)
	join(t, gw, c, "acct-1")
	gw.joinRoom(c, "r-on-gs2", 1)

	rd := nextOfType(t, c, pb.MsgType_MSG_TYPE_REDIRECT).GetRedirect()
	if rd.Host != "ws://gs-2:8080/ws" {
		t.Errorf("redirected to %q, want the node holding the room", rd.Host)
	}
	if rd.RoomId != "r-on-gs2" {
		t.Errorf("redirected to room %q", rd.RoomId)
	}
	if !c.IsHandoff() {
		t.Error("the connection was not marked as handed off, so onClose will tear down its bindings")
	}

	// The match ends and the gameserver unregisters it. The gateway must now
	// say there is no such room rather than send the player to a dead one.
	if err := gw.registry.UnregisterRoom(ctx, "r-on-gs2"); err != nil {
		t.Fatal(err)
	}
	c2 := session.NewConn("acct-2", 64)
	join(t, gw, c2, "acct-2")
	gw.joinRoom(c2, "r-on-gs2", 1)
	e := nextOfType(t, c2, pb.MsgType_MSG_TYPE_ERROR).GetError()
	if e.Code != pb.ErrorCode_ERROR_CODE_ROOM_NOT_ON_NODE {
		t.Errorf("got %v, want ROOM_NOT_ON_NODE for a finished room", e.Code)
	}
}

// ---------------------------------------------------------------------------
// A player handed off from the gateway to a gameserver.

// The ack clamp has to hold on the node the player has just been handed to.
//
// That node's room is at some tick of its own, and the arriving client has
// never seen any of them — Subscribe starts it at ack 0 on purpose. A client
// claiming a tick from the far future would otherwise pin itself to a full
// snapshot every tick on a server it has only just met, and the ack never
// moves backwards so it would never recover for the rest of the match.
func TestHandedOffPlayerCannotAckAheadOfTheSecondNode(t *testing.T) {
	mr := miniredis.RunT(t)
	gs := nodeOn(t, mr.Addr(), "gs-2", pb.Role_ROLE_GAME_SERVER)

	// The match has been running here for a while before the player arrives.
	r := gs.rooms.Start(context.Background(), room.Params{
		ID: "r-handoff", Seed: 1, TickRate: 20, MatchTicks: 1 << 20,
		Roster: room.RosterFromIDs([]uint32{1, 2}, 0),
		Seats:  map[string]uint32{"acct-1": 1},
	})
	t.Cleanup(r.Stop)
	waitForTick(t, r, 3)

	c := session.NewConn("acct-1", 64)
	join(t, gs, c, "acct-1")
	gs.joinRoom(c, "r-handoff", 1)

	// Asserted through what the player actually receives rather than through
	// the room's bookkeeping: the cost of a bogus ack is bandwidth, so the
	// size of the messages is the thing that matters.
	ackWith := func(tick uint32, seq uint32) {
		gs.onMessage(c, protocol.Env(pb.MsgType_MSG_TYPE_INPUT, func(e *pb.Envelope) {
			e.Payload = &pb.Envelope_Input{Input: &pb.Input{AckTick: tick, Seq: seq}}
		}))
	}

	// Settle into the steady state: ack what arrives, receive deltas.
	var seq uint32
	steady := 0
	for i := 0; i < 8; i++ {
		steady = len(nextSnapshot(t, c))
		seq++
		ackWith(r.LastTick(), seq)
	}

	// Now claim a tick this node has never produced, and keep acking honestly.
	seq++
	ackWith(4_000_000_000, seq)
	after := 0
	for i := 0; i < 8; i++ {
		after = len(nextSnapshot(t, c))
		seq++
		ackWith(r.LastTick(), seq)
	}

	if after > steady {
		t.Errorf("after a bogus ack this node sends %dB where it sent %dB: the clamp "+
			"does not hold on the node a player is handed to", after, steady)
	}
}

// nextSnapshot waits for the next snapshot this connection is sent.
func nextSnapshot(t *testing.T, c *session.Conn) []byte {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case raw, ok := <-c.Outbox():
			if !ok {
				t.Fatal("connection closed while waiting for a snapshot")
			}
			e, err := protocol.UnmarshalEnv(raw)
			if err == nil && e.Type == pb.MsgType_MSG_TYPE_SNAPSHOT {
				return raw
			}
		case <-deadline:
			t.Fatal("timed out waiting for a snapshot")
		}
	}
}

func waitForTick(t *testing.T, r *room.Room, want uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.LastTick() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("room never reached tick %d (stuck at %d)", want, r.LastTick())
}

// ---------------------------------------------------------------------------
// Turn pairing across two gateways.

// A claim that fails to become a match must put the opponent back in the
// shared slot, so the next arrival — on either gateway — finds them.
//
// The single-node version of this is TestFailedMatchCreateReparksTheOpponent.
// It matters more here: the claimed player is attached to the other gateway,
// so the node that failed holds no connection to them and cannot tell them
// anything. Left unparked they wait out their whole session while every later
// arrival parks behind a slot nobody is in.
func TestFailedCreateReparksAnOpponentOnTheOtherGateway(t *testing.T) {
	mr := miniredis.RunT(t)
	g1 := gatewayOn(t, mr.Addr(), "gw-1")
	g2 := gatewayOn(t, mr.Addr(), "gw-2")
	g3 := gatewayOn(t, mr.Addr(), "gw-3")

	c1 := connectTo(t, g1, "acct-1")
	g1.onMessage(c1, turnJoinMsg())
	nextOfType(t, c1, pb.MsgType_MSG_TYPE_QUEUED)

	// The second gateway claims them and cannot start the match.
	g2.turn = turn.NewService(turn.Options{
		Store:     brokenCreate{turn.NewMemoryStore(turn.TTL{})},
		Deadlines: turn.NewMemoryDeadlines(),
		Notify:    g2.turnNotify,
		TurnLimit: time.Minute,
	})
	c2 := connectTo(t, g2, "acct-2")
	g2.onMessage(c2, turnJoinMsg())
	nextOfType(t, c2, pb.MsgType_MSG_TYPE_ERROR)

	// A third player, on a third gateway, is paired with the reparked one.
	c3 := connectTo(t, g3, "acct-3")
	g3.onMessage(c3, turnJoinMsg())

	up1 := nextOfType(t, c1, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	up3 := nextOfType(t, c3, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	if up1.State.MatchId != up3.State.MatchId {
		t.Fatalf("players landed in different matches: %q vs %q",
			up1.State.MatchId, up3.State.MatchId)
	}
	if len(up1.State.YourHand) == 0 {
		t.Error("the reparked player was never dealt in")
	}
}

// A match must never reach the store without a deadline queued for it, because
// a second node is what sweeps those. Nothing on the gateway that created the
// match is counting down.
func TestNoMatchIsVisibleToOtherNodesWithoutADeadline(t *testing.T) {
	mr := miniredis.RunT(t)
	g1 := gatewayOn(t, mr.Addr(), "gw-1")
	mmNode := gatewayOn(t, mr.Addr(), "mm-1")

	c1 := connectTo(t, g1, "acct-1")
	c2 := connectTo(t, g1, "acct-2")
	g1.onMessage(c1, turnJoinMsg())
	nextOfType(t, c1, pb.MsgType_MSG_TYPE_QUEUED)
	g1.onMessage(c2, turnJoinMsg())
	up := nextOfType(t, c1, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()

	// The deadline queue is shared, and the sweeper runs on the other node
	// entirely — it is the only thing that will ever move this match if both
	// players walk away. Popping it here is exactly what that sweeper does.
	ctx := context.Background()
	dl := turn.NewRedisDeadlines(mustRedis(t, mr.Addr()))
	due, err := dl.PopDue(ctx, time.Now().Add(time.Hour), 16)
	if err != nil {
		t.Fatal(err)
	}
	var found *turn.Due
	for i := range due {
		if due[i].MatchID == up.State.MatchId {
			found = &due[i]
		}
	}
	if found == nil {
		t.Fatalf("no deadline was queued for match %q, so no other node will ever move it",
			up.State.MatchId)
	}
	if err := mmNode.turn.ExpireTurn(ctx, found.MatchID, found.TurnNumber); err != nil {
		t.Fatalf("ExpireTurn on mm-1: %v", err)
	}

	after := nextOfType(t, c1, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	if after.State.TurnNumber <= up.State.TurnNumber {
		t.Fatalf("the match on gw-1 was never swept by mm-1: turn stayed at %d",
			after.State.TurnNumber)
	}
}

// ---------------------------------------------------------------------------
// Two matchmakers on one queue.

// Two replicas polling the same queue must not form a bot-filled match for a
// player who is merely waiting — and must still form the real one when it fills.
func TestTwoMatchmakersOnOneQueueWithNoTimeout(t *testing.T) {
	mr := miniredis.RunT(t)
	v := config.View{
		TickRate: 20, RoomSize: 4, MinPlayers: 1, QueueTimeout: 0,
		MatchSeconds: 120, SendBuffer: 64, DisconnectGrace: time.Minute,
	}
	mm1 := nodeOn(t, mr.Addr(), "mm-1", pb.Role_ROLE_MATCHMAKER)
	mm2 := nodeOn(t, mr.Addr(), "mm-2", pb.Role_ROLE_MATCHMAKER)
	mm1.dyn, mm2.dyn = config.NewLive(v, nil), config.NewLive(v, nil)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		id := string(rune('a' + i))
		if err := mm1.queue.Enqueue(ctx, &pb.QueuePlayer{
			ConnId: id, PlayerId: id, Name: id, Skill: 1000,
			QueuedAt: time.Now().Add(-time.Hour).UnixMilli(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Both replicas take a pass. Neither may form a short match.
	for _, mm := range []*App{mm1, mm2} {
		m, err := mm.queue.TryForm(ctx, mm.matchRules())
		if err != nil {
			t.Fatal(err)
		}
		if m != nil {
			t.Fatalf("%s formed a %d-player match with %d bots and no timeout configured",
				mm.cfg.NodeID, len(m.Players), m.Bots)
		}
	}

	// The fourth player fills the room, and exactly one replica gets it.
	if err := mm2.queue.Enqueue(ctx, &pb.QueuePlayer{
		ConnId: "d", PlayerId: "d", Name: "d", Skill: 1000,
		QueuedAt: time.Now().Add(-time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	formed := 0
	for _, mm := range []*App{mm1, mm2} {
		m, err := mm.queue.TryForm(ctx, mm.matchRules())
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			continue
		}
		formed++
		if len(m.Players) != 4 || m.Bots != 0 {
			t.Errorf("formed %d players and %d bots, want 4 and 0", len(m.Players), m.Bots)
		}
	}
	if formed != 1 {
		t.Errorf("%d replicas formed a match from the same four players, want exactly 1", formed)
	}
}
