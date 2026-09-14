// Package safe confines a panic to one unit of work.
//
// A game server is a process holding a great many independent things: at 8
// players a room, 10k CCU is around 1250 simulations in one binary. Go's
// default — an unrecovered panic anywhere takes the whole process with it —
// turns one out-of-range index in gameplay code into 1250 matches ending at
// once, on every node the bad input reaches. That is the opposite of what the
// room model is for: the room is meant to be the unit of failure as well as the
// unit of parallelism.
//
// So every goroutine this server starts, and every callback it runs on behalf
// of one client, goes through here. A panic kills that room, that connection or
// that one message, is counted, and leaves the rest of the process running.
//
// What this is not: an excuse to panic. Recovery is a blast radius limiter, not
// error handling. arena_panics_total is a paging metric — a room dying this way
// is a bug that reached production, and the counter is how you find out.
package safe

import (
	"context"
	"log"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/metrics"
)

// Do runs fn and reports whether it panicked.
//
// The stack is logged rather than swallowed: a recovered panic with no stack is
// a bug you get to find twice.
func Do(where string, fn func()) (panicked bool) {
	defer func() {
		if v := recover(); v != nil {
			panicked = true
			metrics.Panics.WithLabelValues(where).Inc()
			log.Printf("PANIC in %s: %v\n%s", where, v, debug.Stack())
		}
	}()
	fn()
	return false
}

// Go starts fn in its own goroutine under Do.
func Go(where string, fn func()) {
	go func() { Do(where, fn) }()
}

// restartDelay keeps a loop that panics on every pass from spinning. It is
// deliberately short: these are the server's own background loops, and
// matchmaking being down is worse than a busy log.
//
// Atomic because tests shorten it while loops are running, and a plain
// variable read from those goroutines is a data race — in the test binary only,
// but the race detector is right about it, and an int64 is cheaper than an
// explanation.
var restartDelay atomic.Int64

func init() { restartDelay.Store(int64(time.Second)) }

// Loop runs a long-lived background loop under Do and restarts it if it
// panics, until ctx is cancelled.
//
// Recovery alone is not enough for these: a matchmaker loop that dies leaves
// the queue filling up with nobody forming matches, and nothing in the process
// would report it — the panic counter ticks once and the server goes quiet.
// Restarting is what turns a panic into a blip. A loop that returns on its own
// is finished, not crashed, and is not restarted.
func Loop(ctx context.Context, where string, fn func(context.Context)) {
	for ctx.Err() == nil {
		if !Do(where, func() { fn(ctx) }) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(restartDelay.Load())):
		}
	}
}

// GoLoop starts Loop in its own goroutine.
func GoLoop(ctx context.Context, where string, fn func(context.Context)) {
	go Loop(ctx, where, fn)
}

// restartDelayFor swaps the restart delay and returns the previous one. Tests
// only: a one-second wait per restart is right in production and is dead time
// in a test that asserts the restart happened.
func restartDelayFor(d time.Duration) time.Duration {
	return time.Duration(restartDelay.Swap(int64(d)))
}
