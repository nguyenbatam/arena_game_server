package ratelimit

import (
	"sync"
	"time"
)

// sweepEvery is how many Allow calls pass between sweeps of expired keys.
//
// A rate limiter is keyed by whatever it is asked to limit — here, a remote IP
// — so the key space is the internet, not the player base. Without eviction the
// map keeps one entry per address ever seen and never gives one back: the leak
// is invisible for a day and then is the whole heap. Amortising the sweep over
// every Nth call keeps it off the hot path and needs no janitor goroutine, so
// there is no lifecycle to manage and nothing for goleak to find.
//
// Read together with sweepBatch, which caps the work one sweep does: the two
// are a rate, not two independent knobs. See the invariant stated there.
const sweepEvery = 128

// Window is a fixed-window per-key rate limiter (in-memory, single process).
type Window struct {
	mu      sync.Mutex
	entries map[string]*entry
	limit   int
	window  time.Duration
	// calls counts Allow invocations since the last sweep.
	calls int
	now   func() time.Time
}

type entry struct {
	count int
	reset time.Time
}

func NewWindow(limit int, window time.Duration) *Window {
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Minute
	}
	return &Window{entries: map[string]*entry{}, limit: limit, window: window, now: time.Now}
}

func (w *Window) Allow(key string) bool {
	if key == "" {
		return true
	}
	now := w.now()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.calls >= sweepEvery {
		w.calls = 0
		w.sweep(now)
	}
	e, ok := w.entries[key]
	if !ok || now.After(e.reset) {
		w.entries[key] = &entry{count: 1, reset: now.Add(w.window)}
		return true
	}
	if e.count >= w.limit {
		return false
	}
	e.count++
	return true
}

// sweepBatch bounds how many keys one sweep inspects.
//
// The sweep runs with w.mu held, and the map it walks is keyed by remote IP —
// the key space is the internet, not the player base, so an unbounded "walk the
// whole map" is a pause that grows with how many distinct addresses have ever
// knocked, and every caller of Allow waits behind it, live players included.
// Capping the work makes the worst-case pause a property of this constant
// rather than of the traffic.
//
// The invariant that makes a partial sweep safe, and the one to preserve if
// these numbers are ever retuned:
//
//	sweepBatch / sweepEvery  >  1
//
// Each Allow call inserts at most one key, so the sweep must inspect strictly
// more than one key per call or eviction loses the race and the map grows
// without bound anyway — a slower leak than having no sweep at all, which is
// worse, because it looks fixed. At 512 per 128 calls the margin is 4x.
// TestExpiredKeysAreEvicted is what holds this: it fills the map, expires every
// entry, and asserts that an ordinary trickle of calls reclaims them.
const sweepBatch = 512

// sweep drops entries whose window has closed, inspecting at most sweepBatch of
// them. Callers hold w.mu.
//
// Go randomises map iteration, which is exactly the property wanted here: each
// pass starts somewhere else, so no entry can hide behind the batch limit the
// way it could under a stable order.
func (w *Window) sweep(now time.Time) {
	seen := 0
	for k, e := range w.entries {
		if now.After(e.reset) {
			delete(w.entries, k)
		}
		if seen++; seen >= sweepBatch {
			return
		}
	}
}

// Len reports how many keys are currently tracked.
func (w *Window) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries)
}

func (w *Window) Reset(key string) {
	w.mu.Lock()
	delete(w.entries, key)
	w.mu.Unlock()
}
