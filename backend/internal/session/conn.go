package session

import (
	"sync"
	"sync/atomic"

	"github.com/nguyenbatam/arena_game_server/internal/metrics"
	"github.com/nguyenbatam/arena_game_server/internal/ratelimit"
)

const DefaultSendBuf = 32

// BufSize reports the outbound buffer depth a new connection should get.
//
// A function rather than a number because the depth is hot-reloadable: reading
// it once while wiring up the listeners would pin every connection the process
// ever accepts to whatever the config happened to say at boot, and the knob in
// /admin/config would move nothing.
type BufSize func() int

// FixedBuf is the BufSize for callers with nothing to reload.
func FixedBuf(n int) BufSize { return func() int { return n } }

// Conn is touched by more than one goroutine and every mutable field here
// proves it: the room's tick goroutine advances LastTick while the connection's
// read goroutine is handling a message, and the notify subscriber binds a room
// while the reader is closing one. None of that is contention worth optimising
// — it is a handful of words behind a mutex — but all of it is a data race if
// the fields are left bare, so the mutable state is unexported and reached
// through accessors.
//
// RemoteIP and Token are the exceptions: both are written once by the transport
// before the Conn is published to the Hub, and are read-only afterwards.
type Conn struct {
	RemoteIP string
	Token    string // optional ?token= bootstrap before hello

	// meta guards the fields below. Kept separate from mu so reading the room
	// binding never waits behind a send.
	meta     sync.RWMutex
	id       string
	playerID uint32
	name     string
	roomID   string

	// lastTick is the newest tick this connection was handed. Written by the
	// room's tick goroutine on every broadcast, read by whoever records the
	// disconnect — atomic rather than under meta so the tick path never waits.
	lastTick atomic.Uint32

	mu      sync.Mutex
	send    chan []byte
	closed  atomic.Bool
	handoff atomic.Bool

	// budget is this connection's own message allowance. It is installed on
	// the first message rather than at construction because the transports
	// build the Conn and the limits are the gateway's to decide — and the
	// first message is the earliest point at which anything is spent.
	budget atomic.Pointer[ratelimit.Budget]
}

func NewConn(id string, sendBuf int) *Conn {
	if sendBuf < 4 {
		sendBuf = DefaultSendBuf
	}
	return &Conn{id: id, send: make(chan []byte, sendBuf)}
}

func (c *Conn) ID() string {
	c.meta.RLock()
	defer c.meta.RUnlock()
	return c.id
}

func (c *Conn) Name() string {
	c.meta.RLock()
	defer c.meta.RUnlock()
	return c.name
}

func (c *Conn) SetName(n string) {
	c.meta.Lock()
	c.name = n
	c.meta.Unlock()
}

func (c *Conn) RoomID() string {
	c.meta.RLock()
	defer c.meta.RUnlock()
	return c.roomID
}

func (c *Conn) PlayerID() uint32 {
	c.meta.RLock()
	defer c.meta.RUnlock()
	return c.playerID
}

// Room returns the room binding as one consistent pair. Reading the two halves
// separately can observe a seat from one room and an id from another while a
// rebind is in flight.
func (c *Conn) Room() (roomID string, playerID uint32) {
	c.meta.RLock()
	defer c.meta.RUnlock()
	return c.roomID, c.playerID
}

// BindRoom seats this connection. Room id and seat move together for the same
// reason Room reads them together.
func (c *Conn) BindRoom(roomID string, playerID uint32) {
	c.meta.Lock()
	c.roomID, c.playerID = roomID, playerID
	c.meta.Unlock()
}

// SetPlayerID assigns a seat before the room itself is known — the assignment
// arrives ahead of the join on a gateway that is about to hand the player off.
func (c *Conn) SetPlayerID(id uint32) {
	c.meta.Lock()
	c.playerID = id
	c.meta.Unlock()
}

// Budget returns this connection's allowance, calling mk once to build it the
// first time. Two reads can race here; the loser throws its own away rather
// than letting a client reset its bucket by sending two messages at once.
func (c *Conn) Budget(mk func() *ratelimit.Budget) *ratelimit.Budget {
	if b := c.budget.Load(); b != nil {
		return b
	}
	fresh := mk()
	if c.budget.CompareAndSwap(nil, fresh) {
		return fresh
	}
	return c.budget.Load()
}

func (c *Conn) LastTick() uint32 { return c.lastTick.Load() }

// SetLastTick advances the high-water mark. It never moves backwards: the tick
// goroutine and a client-supplied resume hint both write here, and a stale
// value would tell the reconnect path to resume from a tick the client has
// already moved past.
func (c *Conn) SetLastTick(t uint32) {
	for {
		cur := c.lastTick.Load()
		if t <= cur {
			return
		}
		if c.lastTick.CompareAndSwap(cur, t) {
			return
		}
	}
}

// Send queues a message under the arena's policy: when the outbound buffer is
// full the oldest queued message is thrown away to make room for this one.
//
// That is right for snapshots and only for snapshots. A snapshot describes the
// whole world at a tick, so the newest one supersedes every older one still in
// the queue — holding a stale world back would make the client's view older,
// not more complete. It is exactly wrong for an event log, where every message
// carries a distinct fact and losing one leaves a hole. Use SendOrdered there.
func (c *Conn) Send(msg []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return
	}
	select {
	case c.send <- msg:
	default:
		metrics.SnapshotsDropped.Inc()
		select {
		case <-c.send:
		default:
		}
		select {
		case c.send <- msg:
		default:
		}
	}
}

// SendOrdered queues a message that must not be dropped, reporting whether it
// made it. A full buffer means this connection has not drained in a long while
// — at turn-based message rates the depth is many moves of slack — so the
// caller's move is to give up on the socket, not to quietly delete an event
// from the middle of the log. The client reconnects and replays from its
// cursor, which is the recovery path this mode is built around.
func (c *Conn) SendOrdered(msg []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return false
	}
	select {
	case c.send <- msg:
		return true
	default:
		return false
	}
}

func (c *Conn) Outbox() <-chan []byte { return c.send }

func (c *Conn) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.CompareAndSwap(false, true) {
		close(c.send)
	}
}

func (c *Conn) Closed() bool { return c.closed.Load() }

func (c *Conn) SetHandoff(v bool) { c.handoff.Store(v) }

func (c *Conn) IsHandoff() bool { return c.handoff.Load() }

type Hub struct {
	mu    sync.RWMutex
	conns map[string]*Conn
}

func NewHub() *Hub { return &Hub{conns: make(map[string]*Conn)} }

func (h *Hub) Add(c *Conn) {
	id := c.ID()
	h.mu.Lock()
	if old, ok := h.conns[id]; ok && old != c {
		old.Close()
		metrics.CCU.Dec()
	}
	h.conns[id] = c
	h.mu.Unlock()
	metrics.CCU.Inc()
	metrics.WSConnections.Inc()
}

func (h *Hub) Remove(c *Conn) {
	id := c.ID()
	h.mu.Lock()
	if cur, ok := h.conns[id]; ok && cur == c {
		delete(h.conns, id)
		metrics.CCU.Dec()
	}
	h.mu.Unlock()
}

// Rekey moves a connection from its bootstrap id to its durable one. The id is
// swapped under the hub lock so no lookup can see the conn filed under neither
// key, and under the conn's own lock so a concurrent reader never observes a
// torn string.
func (h *Hub) Rekey(c *Conn, newID string) {
	if newID == "" || newID == c.ID() {
		return
	}
	h.mu.Lock()
	c.meta.Lock()
	if cur, ok := h.conns[c.id]; ok && cur == c {
		delete(h.conns, c.id)
	}
	if old, ok := h.conns[newID]; ok && old != c {
		old.Close()
		// Its own Remove will find this key pointing at c and decline to touch
		// it, so the displaced conn is accounted for here or never. Add does
		// the same when a reconnect lands on an id directly.
		metrics.CCU.Dec()
	}
	c.id = newID
	c.meta.Unlock()
	h.conns[newID] = c
	h.mu.Unlock()
}

func (h *Hub) Get(id string) *Conn {
	h.mu.RLock()
	c := h.conns[id]
	h.mu.RUnlock()
	return c
}

func (h *Hub) Count() int {
	h.mu.RLock()
	n := len(h.conns)
	h.mu.RUnlock()
	return n
}

func (h *Hub) CloseAll() {
	h.mu.RLock()
	cs := make([]*Conn, 0, len(h.conns))
	for _, c := range h.conns {
		cs = append(cs, c)
	}
	h.mu.RUnlock()
	for _, c := range cs {
		c.Close()
	}
}
