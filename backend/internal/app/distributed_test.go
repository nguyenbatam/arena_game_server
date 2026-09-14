package app

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/config"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/session"
	"github.com/redis/go-redis/v9"
)

// gatewayOn builds a gateway backed by the shared Redis at addr and starts the
// subscriptions Run would start for that role. Two of these is the smallest
// honest model of a scaled-out deployment: separate processes, separate hubs,
// one store.
func gatewayOn(t *testing.T, addr, nodeID string) *App {
	t.Helper()
	cfg := config.Static{
		NodeID:     nodeID,
		PublicAddr: "ws://" + nodeID + ":8080/ws",
		Role:       pb.Role_ROLE_GATEWAY,
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
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := a.notify.SubscribeTurn(ctx, a.cfg.NodeID, a.onTurnPush); err != nil {
		t.Fatalf("SubscribeTurn(%s): %v", nodeID, err)
	}
	return a
}

// connectTo attaches a player to a gateway the way a real client does: HELLO
// first, which is what writes the presence record the pairing check reads.
func connectTo(t *testing.T, a *App, id string) *session.Conn {
	t.Helper()
	c := session.NewConn(id, 64)
	join(t, a, c, id)
	return c
}

func turnJoinMsg() []byte {
	return protocol.Env(pb.MsgType_MSG_TYPE_TURN_JOIN, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_TurnJoin{TurnJoin: &pb.TurnJoin{}}
	})
}

// Two players who land on different gateways must still find each other, and a
// move made on one gateway must reach the opponent on the other. Kept per-node,
// the pairing slot never matches them and the push never arrives: both sit on a
// queue screen that resolves for nobody.
func TestTurnPairsAndPushesAcrossNodes(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	g1 := gatewayOn(t, mr.Addr(), "gw-1")
	g2 := gatewayOn(t, mr.Addr(), "gw-2")

	c1 := connectTo(t, g1, "acct-1")
	c2 := connectTo(t, g2, "acct-2")

	g1.onMessage(c1, turnJoinMsg())
	nextOfType(t, c1, pb.MsgType_MSG_TYPE_QUEUED)

	// The opponent arrives on the other gateway entirely.
	g2.onMessage(c2, turnJoinMsg())

	up1 := nextOfType(t, c1, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	up2 := nextOfType(t, c2, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	if up1.State.MatchId != up2.State.MatchId {
		t.Fatalf("players landed in different matches: %q vs %q", up1.State.MatchId, up2.State.MatchId)
	}
	if len(up1.State.YourHand) == 0 || len(up2.State.YourHand) == 0 {
		t.Fatal("both players must be dealt a hand")
	}

	// Whoever is on the clock plays, from their own gateway.
	mover, moverApp, moverConn, watcher := up1, g1, c1, c2
	if up1.State.Turn != up1.State.YourId {
		mover, moverApp, moverConn, watcher = up2, g2, c2, c1
	}
	if mover.State.Turn != mover.State.YourId {
		t.Fatal("neither projection claims the turn")
	}
	moverApp.onMessage(moverConn, protocol.Env(pb.MsgType_MSG_TYPE_TURN_PLAY, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_TurnPlay{TurnPlay: &pb.TurnPlay{
			MatchId: mover.State.MatchId, Card: mover.State.YourHand[0],
			TurnNumber: mover.State.TurnNumber,
		}}
	}))

	// The opponent is on the other node; the push has to cross to reach them.
	got := nextOfType(t, watcher, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	if got.State.TurnNumber <= mover.State.TurnNumber {
		t.Fatalf("opponent's update did not advance the turn: %d", got.State.TurnNumber)
	}
	if len(got.Events) == 0 {
		t.Fatal("opponent was pushed no events for the move")
	}
}

// A turn that runs out of time is applied by the matchmaker, which holds no
// player connections at all. Both players still have to be told.
func TestTurnTimeoutPushesFromMatchmaker(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	g1 := gatewayOn(t, mr.Addr(), "gw-1")
	g2 := gatewayOn(t, mr.Addr(), "gw-2")
	// The matchmaker is a third process with its own hub, and nobody in it.
	mm := gatewayOn(t, mr.Addr(), "mm-1")

	c1 := connectTo(t, g1, "acct-1")
	c2 := connectTo(t, g2, "acct-2")

	g1.onMessage(c1, turnJoinMsg())
	nextOfType(t, c1, pb.MsgType_MSG_TYPE_QUEUED)
	g2.onMessage(c2, turnJoinMsg())

	up1 := nextOfType(t, c1, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	nextOfType(t, c2, pb.MsgType_MSG_TYPE_TURN_UPDATE)
	matchID := up1.State.MatchId

	// Nobody moves and the clock runs out.
	if err := mm.turn.ExpireTurn(context.Background(), matchID, up1.State.TurnNumber); err != nil {
		t.Fatalf("ExpireTurn: %v", err)
	}

	for i, c := range []*session.Conn{c1, c2} {
		up := nextOfType(t, c, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
		if up.State.TurnNumber <= up1.State.TurnNumber {
			t.Fatalf("player %d was not told the turn expired", i+1)
		}
		var sawTimeout bool
		for _, e := range up.Events {
			if e.Kind == pb.TurnEventKind_TURN_EVENT_KIND_TIMEOUT {
				sawTimeout = true
			}
		}
		if !sawTimeout {
			t.Fatalf("player %d got no timeout event: %+v", i+1, up.Events)
		}
	}
}

// A park outlives the connection that made it, so it can name somebody who has
// since left. The next arrival must skip it and wait, not open a match against
// a seat nobody is sitting in and play a whole game against the timeout worker.
func TestStalePairingParkIsSkipped(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	g1 := gatewayOn(t, mr.Addr(), "gw-1")
	g2 := gatewayOn(t, mr.Addr(), "gw-2")

	ghost := connectTo(t, g1, "acct-ghost")
	g1.onMessage(ghost, turnJoinMsg())
	nextOfType(t, ghost, pb.MsgType_MSG_TYPE_QUEUED)

	// They leave. onClose clears the park, so put it back by hand to model the
	// case the park exists for: a node that died without running any handler.
	g1.onClose(ghost)
	ghost.Close()
	if _, err := g1.turnPair.Claim(context.Background(), "acct-ghost", time.Minute); err != nil {
		t.Fatalf("re-park: %v", err)
	}

	// A live player arrives on the other gateway and finds only that park.
	live := connectTo(t, g2, "acct-live")
	g2.onMessage(live, turnJoinMsg())

	if e := nextOfType(t, live, pb.MsgType_MSG_TYPE_QUEUED); e == nil {
		t.Fatal("expected the arrival to park rather than match a departed player")
	}

	// And they are genuinely parked: a third player pairs with them.
	third := connectTo(t, g1, "acct-third")
	g1.onMessage(third, turnJoinMsg())

	up := nextOfType(t, live, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	if up.State == nil || len(up.State.YourHand) == 0 {
		t.Fatal("the parked player was never dealt into a match")
	}
	if got := nextOfType(t, third, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate(); got.State.MatchId != up.State.MatchId {
		t.Fatalf("players landed in different matches: %q vs %q", got.State.MatchId, up.State.MatchId)
	}
}

// A push must go to the node holding the player, not to every gateway in the
// fleet. Broadcasting costs Redis one send per gateway per message, so egress
// carries a factor of the fleet size — and the fleet grows with the player
// count, which makes the bill grow with the square of it.
func TestTurnPushIsAddressedNotBroadcast(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	g1 := gatewayOn(t, mr.Addr(), "gw-1")
	g2 := gatewayOn(t, mr.Addr(), "gw-2")

	// A third gateway holding nobody in this match. It counts what reaches it.
	bystander := gatewayOn(t, mr.Addr(), "gw-3")
	var seen atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := bystander.notify.SubscribeTurn(ctx, "gw-3", func(*pb.TurnPush) {
		seen.Add(1)
	}); err != nil {
		t.Fatalf("SubscribeTurn: %v", err)
	}

	c1 := connectTo(t, g1, "acct-1")
	c2 := connectTo(t, g2, "acct-2")
	g1.onMessage(c1, turnJoinMsg())
	nextOfType(t, c1, pb.MsgType_MSG_TYPE_QUEUED)
	g2.onMessage(c2, turnJoinMsg())

	up1 := nextOfType(t, c1, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()
	up2 := nextOfType(t, c2, pb.MsgType_MSG_TYPE_TURN_UPDATE).GetTurnUpdate()

	mover, moverApp, moverConn, watcher := up1, g1, c1, c2
	if up1.State.Turn != up1.State.YourId {
		mover, moverApp, moverConn, watcher = up2, g2, c2, c1
	}
	moverApp.onMessage(moverConn, protocol.Env(pb.MsgType_MSG_TYPE_TURN_PLAY, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_TurnPlay{TurnPlay: &pb.TurnPlay{
			MatchId: mover.State.MatchId, Card: mover.State.YourHand[0],
			TurnNumber: mover.State.TurnNumber,
		}}
	}))

	// The intended recipient still gets it...
	nextOfType(t, watcher, pb.MsgType_MSG_TYPE_TURN_UPDATE)
	// ...and the gateway with no stake in this match was never woken.
	if n := seen.Load(); n != 0 {
		t.Fatalf("an uninvolved gateway received %d pushes; delivery is still a broadcast", n)
	}
}

// app.New dials the process's one Redis client and hands it to the Live config,
// so a node starting into a fleet adopts whatever the fleet already agreed on.
//
// main used to do this itself, with a client built purely for the purpose —
// two connection pools against the same server, and that one was never closed.
// Moving it here is what makes a single client possible, and this is the
// behaviour that had to survive the move.
func TestAppNewGivesTheLiveConfigItsRedisClient(t *testing.T) {
	mr := miniredis.RunT(t)

	// Another node has already pushed a config.
	seeded := config.View{
		TickRate: 60, RoomSize: 6, MinPlayers: 2, QueueTimeout: 3 * time.Second,
		MatchSeconds: 45, SendBuffer: 64, DisconnectGrace: time.Minute,
	}
	seeder := config.NewLive(config.LoadViewFromEnv(), mustRedis(t, mr.Addr()))
	if err := seeder.Update(context.Background(), seeded); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	// This node starts with a Live that has no client of its own.
	local := config.View{
		TickRate: 20, RoomSize: 2, MinPlayers: 1, QueueTimeout: time.Second,
		MatchSeconds: 120, SendBuffer: 32, DisconnectGrace: time.Minute,
	}
	dyn := config.NewLive(local, nil)
	a, err := New(config.Static{
		NodeID: "gs-adopt", PublicAddr: "ws://gs-adopt:8080/ws",
		Role: pb.Role_ROLE_GATEWAY, RedisAddr: mr.Addr(), TurnLimit: time.Minute,
	}, dyn)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	if a.rdb == nil {
		t.Fatal("app.New built no Redis client")
	}

	got := dyn.Get()
	if got.TickRate != seeded.TickRate || got.RoomSize != seeded.RoomSize {
		t.Fatalf("the node kept its local config (tick=%d room=%d); it should have adopted tick=%d room=%d",
			got.TickRate, got.RoomSize, seeded.TickRate, seeded.RoomSize)
	}

	// And a push made through this node reaches the store the rest of the
	// fleet reads, which is only true if the client really was installed.
	if err := dyn.Update(context.Background(), config.View{
		TickRate: 30, RoomSize: 4, MinPlayers: 1, SendBuffer: 32,
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	elsewhere := config.NewLive(config.LoadViewFromEnv(), mustRedis(t, mr.Addr()))
	if v := elsewhere.Get(); v.TickRate != 30 || v.RoomSize != 4 {
		t.Fatalf("another node read tick=%d room=%d after the push, want tick=30 room=4", v.TickRate, v.RoomSize)
	}
}

func mustRedis(t *testing.T, addr string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	return c
}
