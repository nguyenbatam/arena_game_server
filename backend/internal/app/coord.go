package app

import (
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/metrics"
)

// soft records a coordination-store call that failed and did not stop anything.
//
// Presence, the matchmaking queue, the node and room directories, the placement
// queue and the notify bus are all off the tick path, and none of them is worth
// failing a player's connection over: a presence record that did not write
// costs a slower reconnect, and a queue entry that did not clear costs one
// stale row that expires on its own. So every one of these call sites carries
// on, and that is precisely why the failure has to be recorded somewhere.
//
// Discarding them — which is what this replaces at some thirty call sites — has
// a specific failure mode, and it is the worst kind. A Redis that has started
// refusing writes does not show up as an error anywhere: CCU is flat because
// nobody is being disconnected, matches are flat because the matchmaker is
// quietly forming none, and every graph in internal/metrics looks like a server
// with no players rather than a server with a broken dependency.
//
// The counter is the signal; the log line is for working out which call it was.
// Throttled per operation, because the whole set fails together — one outage
// would otherwise write a line per connection event across the fleet, which is
// how an incident loses the log that would have explained it. The same
// reasoning the UDP transport applies to its own dropped-datagram counters.
func (a *App) soft(op string, err error) {
	if err == nil {
		return
	}
	metrics.CoordErrors.WithLabelValues(op).Inc()
	if a.coord.allow(op) {
		log.Printf("coord: %s: %v", op, err)
	}
}

// coordEvery is how often one operation may put a line in the log. Short enough
// that a transient failure is still visible at the moment it happens, long
// enough that a sustained one costs a line a second rather than thousands.
const coordEvery = time.Second

// coordLog throttles per operation rather than globally, so a flood of presence
// failures cannot hide the one directory failure underneath it.
type coordLog struct {
	mu   sync.Mutex
	last map[string]time.Time
	now  func() time.Time
}

func (c *coordLog) allow(op string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.last == nil {
		c.last = make(map[string]time.Time, 16)
	}
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	t := now()
	// The op set is closed — every caller passes a literal — so this map is
	// bounded by the number of call sites and needs no eviction.
	if prev, ok := c.last[op]; ok && t.Sub(prev) < coordEvery {
		return false
	}
	c.last[op] = t
	return true
}

// writeJSONErr records a response that could not be written.
//
// There is nothing to do about it: the status line and headers went out before
// the body failed, so the client cannot be told, and the handler has already
// counted the operation as the success it was. It is recorded because the
// ordinary cause — a caller that hung up mid-response — and the alarming one —
// a value this process cannot encode, producing a 200 with a truncated body —
// are indistinguishable from the client's side and would otherwise both be
// invisible from this one.
func writeJSONErr(handler string, err error) {
	if err == nil {
		return
	}
	metrics.HTTPWriteErrors.WithLabelValues(handler).Inc()
	log.Printf("http: %s: write response: %v", handler, err)
}

// closeErr reports a resource that would not release during shutdown.
//
// Shutdown is the one path where a discarded error costs nothing at runtime and
// everything in diagnosis: the process is on its way out, so nothing downstream
// will ever notice, and a listener that refused to close or a database that
// could not drain is exactly what somebody reading the logs after an unclean
// restart is looking for.
func closeErr(what string, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, http.ErrServerClosed) {
		return
	}
	log.Printf("shutdown: %s: %v", what, err)
}
