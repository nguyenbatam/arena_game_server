package ratelimit

import (
	"sync"
	"time"
)

// Budget is one connection's own allowance, in messages and in bytes.
//
// The Window limiters guard the doors a stranger knocks on — login, hello,
// queue — and they are keyed by IP, which is the only identity available before
// anyone is authenticated. Neither of those properties helps once a connection
// is established and trusted: an authenticated client can then send whatever it
// likes at whatever rate it likes, and the only thing standing between it and
// the server's CPU is that the room drops inputs it cannot keep up with. It
// drops them *after* the parse, which is the expensive half.
//
// So every connection carries its own bucket, checked before the payload is
// unmarshalled. Two dimensions, because they fail differently: a flood of tiny
// inputs burns CPU on parsing, and a slow trickle of 64 KB frames burns
// bandwidth and allocator time without ever tripping a message counter.
//
// Being per-connection also makes it fair under NAT. A per-IP limit tight
// enough to stop one abusive client would throw out everyone behind a carrier
// gateway or a university, which is a real population on mobile.
type Budget struct {
	mu    sync.Mutex
	msgs  bucket
	bytes bucket
	now   func() time.Time
}

// bucket is a token bucket carrying its own refill rate. Tokens are a float so
// a rate below one per call still accrues instead of rounding away.
type bucket struct {
	tokens float64
	rate   float64 // tokens per second
	burst  float64
	last   time.Time
}

// NewBudget builds a budget. A rate of zero leaves that dimension unlimited;
// a burst of zero takes one second's worth, which is the smallest allowance
// that does not punish a client for sending its second's traffic in one go.
func NewBudget(msgRate, msgBurst, byteRate, byteBurst int) *Budget {
	now := time.Now()
	return &Budget{
		msgs:  newBucket(msgRate, msgBurst, now),
		bytes: newBucket(byteRate, byteBurst, now),
		now:   time.Now,
	}
}

func newBucket(rate, burst int, now time.Time) bucket {
	if rate <= 0 {
		return bucket{}
	}
	if burst <= 0 {
		burst = rate
	}
	return bucket{tokens: float64(burst), rate: float64(rate), burst: float64(burst), last: now}
}

// Allow charges one message of size bytes against the budget, reporting whether
// it fits. A message that does not fit is not charged: the caller is expected to
// close the connection, and a client that is already over its allowance should
// not have its debt deepened by the attempt.
func (b *Budget) Allow(size int) bool {
	if b == nil {
		return true
	}
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.msgs.peek(now, 1) {
		return false
	}
	if !b.bytes.peek(now, float64(size)) {
		return false
	}
	b.msgs.take(1)
	b.bytes.take(float64(size))
	return true
}

// peek refills and reports whether n tokens are available, without taking them.
// Both dimensions are checked before either is charged so a message rejected on
// bytes does not still cost a message token.
func (t *bucket) peek(now time.Time, n float64) bool {
	if t.rate == 0 {
		return true
	}
	if elapsed := now.Sub(t.last).Seconds(); elapsed > 0 {
		t.tokens += elapsed * t.rate
		if t.tokens > t.burst {
			t.tokens = t.burst
		}
		t.last = now
	}
	// A single message larger than the whole burst would otherwise never be
	// sendable — the bucket cannot hold enough tokens to pay for it. The
	// transport's own frame limit is the ceiling on message size; this is a
	// rate limit, so let one oversized message through on a full bucket.
	if n > t.burst {
		n = t.burst
	}
	return t.tokens >= n
}

func (t *bucket) take(n float64) {
	if t.rate == 0 {
		return
	}
	if n > t.burst {
		n = t.burst
	}
	t.tokens -= n
}
