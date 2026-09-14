package app

import (
	"context"
	"errors"
	"log"
	"sync"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/metrics"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/session"
	"github.com/nguyenbatam/arena_game_server/internal/turn"
)

// turnHub is the gateway side of the turn-based mode. It reuses the same
// connections, auth and session ids as the arena — only the match model below
// it differs.
type turnHub struct {
	mu sync.RWMutex
	// conns is keyed by the player's durable id — the same string the session
	// is keyed by — so no id has to be minted here. A gateway that mints its
	// own numbers hands two different people the same id the moment a second
	// gateway does the same, and with a shared store they end up in one match.
	conns map[string]*session.Conn
}

func newTurnHub() *turnHub {
	return &turnHub{conns: map[string]*session.Conn{}}
}

// bind files c as where this player's updates go. Already-on-file is the common
// case by a wide margin — every PLAY and every SYNC comes through here, while
// the entry only ever changes on a reconnect — so it is settled under a read
// lock rather than rewriting the same value under an exclusive one.
func (h *turnHub) bind(c *session.Conn) {
	id := c.ID()
	h.mu.RLock()
	cur := h.conns[id]
	h.mu.RUnlock()
	if cur == c {
		return
	}
	h.mu.Lock()
	h.conns[id] = c
	h.mu.Unlock()
}

// drop forgets a disconnected player, reporting whether this conn was still the
// one on file. Without it the connection map grows for the life of the process.
func (h *turnHub) drop(c *session.Conn) (id string, dropped bool) {
	id = c.ID()
	h.mu.Lock()
	if cur, ok := h.conns[id]; ok && cur == c {
		delete(h.conns, id)
		dropped = true
	}
	h.mu.Unlock()
	return id, dropped
}

func (h *turnHub) conn(id string) *session.Conn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.conns[id]
}

// bindTurn files this connection as the delivery target for its player, and
// returns the player id either way.
//
// The session hub is the authority on which connection is current, the same
// test onClose makes. Without it a frame that was already read when a reconnect
// replaced this connection re-files the dead one on its way through: every
// later push is then handed to a closed socket and discarded without a sound —
// not even counted, since pushTurn sees an already-closed conn and has nothing
// to close — while the live connection sits there receiving nothing.
//
// The message itself is still the player's and still applies. Only delivery
// belongs to the connection that replaced this one.
func (a *App) bindTurn(c *session.Conn) string {
	id := c.ID()
	if c.Closed() {
		return id
	}
	if cur := a.hub.Get(id); cur != nil && cur != c {
		return id
	}
	a.turnHub.bind(c)
	return id
}

func (a *App) turnNotify(pid string, up *pb.TurnUpdate) {
	if c := a.turnHub.conn(pid); c != nil {
		pushTurn(c, up)
		return
	}
	// Not attached here, which is the ordinary case rather than an error: the
	// two players may be on different gateways, and a turn that runs out of
	// time is applied by the matchmaker, which holds no player connections at
	// all. Hand it to the bus and let whichever node has them deliver it.
	//
	// Presence already records which node that is, so the update is addressed
	// to it rather than shouted at the whole fleet. When presence cannot say —
	// the record expired, or the store is unreachable — it falls back to the
	// broadcast channel: a wasteful delivery beats a lost one.
	//
	// If nobody claims it the player really is offline, and this costs one
	// dropped message. They catch up from their cursor on reconnect, which is
	// the whole point of an event log.
	ctx, cancel := a.op()
	defer cancel()
	node := ""
	if p, err := a.presence.Get(ctx, pid); err == nil && p != nil {
		node = p.NodeId
	}
	if err := a.notify.PublishTurn(ctx, node, &pb.TurnPush{PlayerId: pid, Update: up}); err != nil {
		log.Printf("turn: push to %s: %v", pid, err)
	}
}

// onTurnPush delivers an update published by another node. It never republishes
// — a push that arrives for a player this node does not hold is simply not ours.
func (a *App) onTurnPush(p *pb.TurnPush) {
	if p == nil || p.Update == nil {
		return
	}
	c := a.turnHub.conn(p.PlayerId)
	if c == nil {
		return
	}
	pushTurn(c, p.Update)
}

// pushTurn delivers one update on the event log's terms: never silently
// dropped. A connection that cannot take it is closed rather than handed a log
// with a hole in it — the client comes back and replays from its cursor, and
// the browser and the load bot both check for exactly that hole, so a drop that
// did slip through would be repaired rather than believed.
func pushTurn(c *session.Conn, up *pb.TurnUpdate) {
	if c.SendOrdered(protocol.TurnUpdate(up)) {
		return
	}
	if !c.Closed() {
		metrics.TurnPushDropped.Inc()
		c.Close()
	}
}

func (a *App) onTurnJoin(c *session.Conn, tj *pb.TurnJoin) {
	if a.turn == nil {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_WRONG_ROLE, "turn mode not enabled on this node"))
		return
	}
	if !a.allowRate(a.turnRL, c.RemoteIP) {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_RATE_LIMITED, "too many turn requests"))
		return
	}
	pid := a.bindTurn(c)

	if tj.GetMatchId() != "" {
		// Rejoin: hand over everything from the start of the log. A client that
		// kept its cursor should send TURN_SYNC instead and get only the diff.
		a.sendTurnSync(c, tj.GetMatchId(), pid, 0)
		return
	}

	other, ok := a.claimOpponent(pid)
	if !ok {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_UNSPECIFIED, "could not join"))
		return
	}
	if other == "" {
		c.Send(protocol.Queued())
		return
	}
	self := pid
	// The pair alone is not a unique name: the same two players meeting again
	// would reuse the id, and with it the previous match's event log and
	// cursor. The nonce keeps each match its own.
	matchID := "t-" + other + "-" + self + "-" + session.NewID()
	cctx, cancel := a.op()
	defer cancel()
	if _, err := a.turn.Create(cctx, matchID, seedFor(matchID), [2]string{other, self}); err != nil {
		log.Printf("turn: create %s: %v", matchID, err)
		a.repark(cctx, other)
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_UNSPECIFIED, "could not start match"))
	}
}

// repark puts a claimed opponent back in the pairing slot after the match they
// were claimed for failed to start.
//
// Claim consumes the park, so by this point that player is neither in a match
// nor waiting in one — and this node does not hold their connection, so there
// is nothing to tell them. They would sit on a queue screen for the rest of
// their session while every later arrival parks behind them. The player who did
// get an error at least knows to try again.
//
// Best effort, and it says so when it fails. Park declines when somebody else
// has taken the slot in the meantime, which leaves the same orphan this is
// trying to prevent — rare enough to log rather than to build a queue for, and
// the client's own retry is the backstop either way.
func (a *App) repark(ctx context.Context, playerID string) {
	if playerID == "" || a.turnPair == nil {
		return
	}
	switch ok, err := a.turnPair.Park(ctx, playerID, turn.DefaultPairTTL); {
	case err != nil:
		log.Printf("turn: repark %s: %v", playerID, err)
	case !ok:
		log.Printf("turn: repark %s: slot taken, player left unpaired", playerID)
	}
}

// pairAttempts bounds how many stale parks one arrival will clear before giving
// up and waiting itself. Each pass consumes one park, so this cannot spin.
const pairAttempts = 4

// claimOpponent takes the parked player, skipping any who are no longer around.
// It returns an empty id when this player ended up parked instead.
//
// A park outlives the connection that made it — that is the point, it has to be
// visible from every gateway — so it can name someone who has since left. Left
// unchecked the next arrival opens a match against a seat nobody is sitting in,
// and plays a whole game against the timeout worker.
func (a *App) claimOpponent(pid string) (string, bool) {
	ctx, cancel := a.op()
	defer cancel()
	for i := 0; i < pairAttempts; i++ {
		other, err := a.turnPair.Claim(ctx, pid, turn.DefaultPairTTL)
		if err != nil {
			log.Printf("turn: pair %s: %v", pid, err)
			return "", false
		}
		if other == "" || a.playerReachable(other) {
			return other, true
		}
		// That park was stale and Claim has already consumed it. Go round and
		// either take the next one or park ourselves.
	}
	return "", true
}

// playerReachable reports whether a player still appears to be connected
// somewhere in the fleet. Local connections are authoritative; for everyone
// else presence is what the rest of the gateway already trusts for exactly this
// question, and it expires on its own when a node dies without cleaning up.
func (a *App) playerReachable(pid string) bool {
	if c := a.turnHub.conn(pid); c != nil {
		return !c.Closed()
	}
	ctx, cancel := a.op()
	defer cancel()
	p, err := a.presence.Get(ctx, pid)
	if err != nil || p == nil {
		return false
	}
	return p.Status != pb.PresenceStatus_PRESENCE_STATUS_DISCONNECTED
}

func (a *App) onTurnPlay(c *session.Conn, tp *pb.TurnPlay) {
	if a.turn == nil || tp == nil {
		return
	}
	if !a.allowRate(a.turnRL, c.RemoteIP) {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_RATE_LIMITED, "too many turn requests"))
		return
	}
	pid := a.bindTurn(c)
	ctx, cancel := a.op()
	defer cancel()
	err := a.turn.Play(ctx, tp.MatchId, turn.Move{
		PlayerID:   pid,
		Card:       tp.Card,
		TurnNumber: tp.TurnNumber,
		IdemKey:    tp.IdemKey,
	})
	if err == nil {
		return
	}
	// Rule violations are the client's problem to display, not server errors.
	code := pb.ErrorCode_ERROR_CODE_BAD_PAYLOAD
	if errors.Is(err, turn.ErrNoMatch) {
		code = pb.ErrorCode_ERROR_CODE_ROOM_NOT_FOUND
	}
	c.Send(protocol.Err(code, err.Error()))
}

func (a *App) onTurnSync(c *session.Conn, ts *pb.TurnSync) {
	if a.turn == nil || ts == nil {
		return
	}
	// Sync reads a range out of the event log, so an unthrottled client could
	// keep a Redis connection busy for free. Cheap to send, not cheap to serve.
	if !a.allowRate(a.turnRL, c.RemoteIP) {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_RATE_LIMITED, "too many turn requests"))
		return
	}
	a.sendTurnSync(c, ts.MatchId, a.bindTurn(c), ts.SinceSeq)
}

// seedFor derives a match's deal from its id so the same match always deals the
// same cards — replayable, and auditable after a cheating report.
func seedFor(matchID string) int64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(matchID); i++ {
		h ^= uint64(matchID[i])
		h *= 1099511628211
	}
	return int64(h >> 1)
}

func (a *App) sendTurnSync(c *session.Conn, matchID string, viewer string, since uint64) {
	ctx, cancel := a.op()
	defer cancel()
	up, err := a.turn.Sync(ctx, matchID, viewer, since)
	if err != nil {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_ROOM_NOT_FOUND, err.Error()))
		return
	}
	pushTurn(c, up)
}
