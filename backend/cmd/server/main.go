package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/nguyenbatam/arena_game_server/internal/app"
	"github.com/nguyenbatam/arena_game_server/internal/config"
)

func main() {
	st := config.LoadStatic()
	setupLogging(st)
	setupProfiling(st)
	if err := st.Validate(); err != nil {
		slog.Error("refusing to start", "err", err)
		os.Exit(1)
	}

	// No Redis client here: app.New dials the one the process uses and hands it
	// to the Live config itself. Building one at this level meant two pools
	// against the same server, and this one was never closed.
	dyn := config.NewLive(config.LoadViewFromEnv(), nil)

	a, err := app.New(st, dyn)
	if err != nil {
		slog.Error("startup failed", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = a.Run(ctx)
	cancel()
	if err != nil {
		slog.Error("run failed", "err", err)
		os.Exit(1)
	}
}

func setupLogging(st config.Static) {
	level := slog.LevelInfo
	if !st.Production() {
		level = slog.LevelDebug
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if st.Production() {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(h))
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
}

// blockProfileRate and mutexProfileFraction are what the runtime is asked for
// when PPROF is on.
//
// Both are deliberately coarser than the values this used to run with
// unconditionally (1000 and 10). Those are close to "record every blocking
// event", and every recorded event is a stack walk: measured on a mutex +
// buffered-channel loop shaped like session.Conn.Send, they cost 2.2x — 28.9
// ns/op became 64.0. The sampled rates below still find a lock somebody is
// actually queueing on, which is what these profiles are opened for; they will
// not find a contention event that happens once.
const (
	blockProfileRate     = 100_000 // one sample per 100us of blocking
	mutexProfileFraction = 100     // one in a hundred contention events
)

// setupProfiling arms the block and mutex profilers, and only when something
// can read them.
//
// They used to be switched on at the top of main with no condition at all. With
// PPROF unset, App.routes does not mount /debug/pprof — so the process paid for
// two of the runtime's most expensive instrumentations to fill buffers that no
// endpoint exposed. Tying them to the same flag that mounts the handlers makes
// the cost and the access one decision.
func setupProfiling(st config.Static) {
	if !st.PprofEnabled {
		return
	}
	runtime.SetBlockProfileRate(blockProfileRate)
	runtime.SetMutexProfileFraction(mutexProfileFraction)
	slog.Info("pprof enabled", "block_profile_rate", blockProfileRate, "mutex_profile_fraction", mutexProfileFraction)
}
