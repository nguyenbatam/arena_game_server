package main

import (
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/config"
)

// The block and mutex profilers are global runtime state, and these cases both
// arm and disarm them. Each starts by putting the runtime back to "off" so the
// assertion is about what setupProfiling did and not about whichever case ran
// before it — the test job runs with -shuffle=on, so that ordering is not
// something to rely on.
func resetProfilers(t *testing.T) {
	t.Helper()
	runtime.SetBlockProfileRate(0)
	runtime.SetMutexProfileFraction(0)
	t.Cleanup(func() {
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(0)
	})
}

// blockUntilSampled does a blocking wait long enough that the block profiler
// records it whenever it is armed at all. blockProfileRate samples one event
// per 100us of blocking; ten milliseconds is two orders above that, so a
// recorded sample is certain rather than likely.
func blockUntilSampled() {
	var wg sync.WaitGroup
	ch := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		close(ch)
	}()
	<-ch
	wg.Wait()
}

// blockRecords counts what is in the block profile right now.
//
// Compared as a delta rather than against zero: the profile accumulates for the
// life of the process and is never cleared, so an absolute count would depend
// on what else in this binary had already blocked.
func blockRecords() int { return pprof.Lookup("block").Count() }

// mutexFraction reads the current fraction without changing it. A negative
// argument is the runtime's documented way to ask.
func mutexFraction() int { return runtime.SetMutexProfileFraction(-1) }

// The regression this is here for: both profilers used to be armed at the top
// of main with no condition, so a process running without PPROF paid for two of
// the runtime's most expensive instrumentations to fill buffers that
// App.routes never mounted an endpoint for.
func TestProfilingStaysOffWhenPprofIsDisabled(t *testing.T) {
	resetProfilers(t)

	before := blockRecords()
	setupProfiling(config.Static{PprofEnabled: false})
	blockUntilSampled()

	if got := blockRecords(); got != before {
		t.Errorf("block profiler recorded %d new samples with PPROF off; want none", got-before)
	}
	if got := mutexFraction(); got != 0 {
		t.Errorf("mutex profile fraction = %d with PPROF off; want 0", got)
	}
}

func TestProfilingArmsBothProfilersWhenPprofIsEnabled(t *testing.T) {
	resetProfilers(t)

	before := blockRecords()
	setupProfiling(config.Static{PprofEnabled: true})
	blockUntilSampled()

	if got := blockRecords(); got <= before {
		t.Errorf("block profiler recorded no samples with PPROF on (%d -> %d)", before, got)
	}
	if got := mutexFraction(); got != mutexProfileFraction {
		t.Errorf("mutex profile fraction = %d; want %d", got, mutexProfileFraction)
	}
}

// The rates are part of the contract, not incidental: the values this replaced
// (1000 / 10) are close enough to "record everything" that they cost 2.2x on a
// mutex-and-channel path shaped like session.Conn.Send. A future edit that
// walks them back towards those numbers should have to change this line and
// say why.
func TestProfilingRatesAreSampledNotExhaustive(t *testing.T) {
	if blockProfileRate < 10_000 {
		t.Errorf("blockProfileRate = %d: below 10us per sample this is effectively recording every blocking event", blockProfileRate)
	}
	if mutexProfileFraction < 10 {
		t.Errorf("mutexProfileFraction = %d: below one in ten this is effectively recording every contention event", mutexProfileFraction)
	}
}
