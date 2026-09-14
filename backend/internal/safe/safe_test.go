package safe

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoReportsPanic(t *testing.T) {
	if Do("test", func() { panic("boom") }) != true {
		t.Fatal("Do returned false for a function that panicked")
	}
	if Do("test", func() {}) != false {
		t.Fatal("Do returned true for a function that did not panic")
	}
}

// A nil map write, a nil pointer dereference and an out-of-range index are the
// three that actually happen in gameplay code, and runtime errors travel a
// different path from a plain panic value.
func TestDoRecoversRuntimeErrors(t *testing.T) {
	if !Do("test", func() {
		var m map[string]int
		m["x"] = 1 //nolint:staticcheck // recover a runtime nil-map write
	}) {
		t.Fatal("nil map write was not recovered")
	}
	if !Do("test", func() { s := []int{}; _ = s[3] }) {
		t.Fatal("index out of range was not recovered")
	}
	if !Do("test", func() { panic(errors.New("typed")) }) {
		t.Fatal("error panic was not recovered")
	}
}

// A background loop that panics must come back, or the queue quietly stops
// being drained and nothing in the process says why.
func TestLoopRestartsAfterPanic(t *testing.T) {
	old := restartDelayFor(time.Millisecond)
	defer restartDelayFor(old)

	runs := make(chan int, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := 0
	go Loop(ctx, "test", func(context.Context) {
		n++
		runs <- n
		panic("boom")
	})

	for want := 1; want <= 3; want++ {
		select {
		case got := <-runs:
			if got != want {
				t.Fatalf("run %d, want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("loop did not restart after panic %d", want)
		}
	}
}

// Returning is how a loop says it is finished. Restarting it then would spin
// forever on a job that is already done.
func TestLoopStopsOnCleanReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	done := make(chan struct{})
	go func() {
		Loop(ctx, "test", func(context.Context) { calls++ })
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Loop did not return when fn returned")
	}
	if calls != 1 {
		t.Fatalf("fn ran %d times, want 1", calls)
	}
}

func TestLoopStopsOnContextCancel(t *testing.T) {
	old := restartDelayFor(time.Millisecond)
	defer restartDelayFor(old)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Loop(ctx, "test", func(ctx context.Context) {
			cancel()
			panic("boom")
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Loop kept restarting after its context was cancelled")
	}
}
