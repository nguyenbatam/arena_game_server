package ratelimit

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestWindowAllow(t *testing.T) {
	w := NewWindow(2, time.Minute)
	if !w.Allow("1.2.3.4") {
		t.Fatal("first should pass")
	}
	if !w.Allow("1.2.3.4") {
		t.Fatal("second should pass")
	}
	if w.Allow("1.2.3.4") {
		t.Fatal("third should block")
	}
	if !w.Allow("9.9.9.9") {
		t.Fatal("other key should pass")
	}
}

// A rate limiter keyed on remote IP has the internet for a key space, not the
// player base. Without eviction the map keeps one entry per address ever seen
// and never gives one back.
func TestExpiredKeysAreEvicted(t *testing.T) {
	w := NewWindow(10, time.Minute)
	base := time.Now()
	w.now = func() time.Time { return base }

	for i := 0; i < 4*sweepEvery; i++ {
		w.Allow(strconv.Itoa(i))
	}
	if w.Len() < sweepEvery {
		t.Fatalf("expected the map to have grown, got %d", w.Len())
	}

	// Every window above has now closed; a trickle of new calls must reclaim them.
	w.now = func() time.Time { return base.Add(2 * time.Minute) }
	for i := 0; i < sweepEvery; i++ {
		w.Allow("live")
	}
	if n := w.Len(); n > 8 {
		t.Fatalf("%d expired entries retained after a sweep, want a handful", n)
	}
}

// Eviction must not hand back budget to a key whose window is still open.
func TestSweepDoesNotResetLiveKeys(t *testing.T) {
	w := NewWindow(3, time.Minute)
	base := time.Now()
	w.now = func() time.Time { return base }

	for i := 0; i < 3; i++ {
		if !w.Allow("hot") {
			t.Fatalf("call %d denied inside the limit", i)
		}
	}
	// Drive enough traffic to trigger a sweep while "hot" is still live.
	for i := 0; i < 2*sweepEvery; i++ {
		w.Allow("other-" + strconv.Itoa(i))
	}
	if w.Allow("hot") {
		t.Fatal("a sweep refilled a live key's budget")
	}
}

func TestWindowRollsOverAfterItsDuration(t *testing.T) {
	w := NewWindow(2, time.Minute)
	base := time.Now()
	w.now = func() time.Time { return base }

	if !w.Allow("k") {
		t.Fatal("first should pass")
	}
	if !w.Allow("k") {
		t.Fatal("second should pass")
	}
	if w.Allow("k") {
		t.Fatal("allowed past the limit")
	}
	w.now = func() time.Time { return base.Add(time.Minute + time.Second) }
	if !w.Allow("k") {
		t.Fatal("window did not roll over")
	}
}

// An empty key means "nothing to limit on" and is always allowed — which is
// exactly why a transport that forgets to set RemoteIP disables limiting.
func TestEmptyKeyIsAlwaysAllowed(t *testing.T) {
	w := NewWindow(1, time.Minute)
	for i := 0; i < 100; i++ {
		if !w.Allow("") {
			t.Fatal("empty key was limited")
		}
	}
}

func TestKeysAreLimitedIndependently(t *testing.T) {
	w := NewWindow(1, time.Minute)
	if !w.Allow("a") || !w.Allow("b") {
		t.Fatal("independent keys interfered")
	}
	if w.Allow("a") || w.Allow("b") {
		t.Fatal("second call per key should be denied")
	}
}

func TestConcurrentAllowIsRaceFree(t *testing.T) {
	w := NewWindow(1000000, time.Minute)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				w.Allow(strconv.Itoa((g*2000 + i) % 500))
			}
		}(g)
	}
	wg.Wait()
	if w.Len() == 0 {
		t.Fatal("no keys tracked")
	}
}

func TestResetClearsOneKey(t *testing.T) {
	w := NewWindow(1, time.Minute)
	w.Allow("k")
	if w.Allow("k") {
		t.Fatal("expected denial")
	}
	w.Reset("k")
	if !w.Allow("k") {
		t.Fatal("Reset did not clear the key")
	}
}
