package config

import (
	"context"
	"testing"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/redistest"
)

func TestLiveHotUpdateNoRestart(t *testing.T) {
	l := NewLive(View{TickRate: 20, RoomSize: 8, MinPlayers: 2, QueueTimeout: time.Second, MatchSeconds: 90, SendBuffer: 32}, nil)
	if l.Get().TickRate != 20 {
		t.Fatal(l.Get())
	}
	if err := l.Update(context.Background(), View{TickRate: 30, RoomSize: 6, MinPlayers: 2, QueueTimeout: 3 * time.Second, MatchSeconds: 60, SendBuffer: 16, MaxCCU: 100}); err != nil {
		t.Fatal(err)
	}
	// Consumers pull, so the whole view has to be visible through Get the
	// moment Update returns — not just the field the caller happened to change.
	got := l.Get()
	if got.TickRate != 30 || got.RoomSize != 6 || got.SendBuffer != 16 || got.MaxCCU != 100 {
		t.Fatalf("update not applied: %+v", got)
	}
	if got.QueueTimeout != 3*time.Second || got.MatchSeconds != 60 {
		t.Fatalf("update not applied: %+v", got)
	}
}

func TestParseRoleEnum(t *testing.T) {
	if ParseRole("gameserver") == ParseRole("all") {
		t.Fatal("roles collide")
	}
	if ParseRole("gateway") == ParseRole("matchmaker") {
		t.Fatal("roles collide")
	}
}

func TestProductionEnv(t *testing.T) {
	t.Setenv("ENV", "production")
	s := LoadStatic()
	if !s.Production() {
		t.Fatal("expected production")
	}
}

// A Live built without a client picks one up later and pulls what the fleet
// already agreed on. This is what lets the process hold a single Redis client:
// main used to dial one purely to hand to NewLive, and app.New then dialled a
// second for everything else — two pools against the same server, and the first
// was never closed.
func TestUseRedisAdoptsTheStoredConfig(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()

	// Something else in the fleet has already pushed a config.
	published := View{TickRate: 60, RoomSize: 6, MinPlayers: 2, SendBuffer: 64, MaxCCU: 999}
	seeder := NewLive(LoadViewFromEnv(), rdb)
	if err := seeder.Update(ctx, published); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// A process starting now builds its Live before it has a client.
	l := NewLive(LoadViewFromEnv(), nil)
	if l.Get().TickRate == published.TickRate && l.Get().RoomSize == published.RoomSize {
		t.Skip("environment already matches the published config; nothing to observe")
	}

	l.UseRedis(ctx, rdb)
	got := l.Get()
	if got.TickRate != published.TickRate || got.RoomSize != published.RoomSize {
		t.Fatalf("after UseRedis the view is tick=%d room=%d, want tick=%d room=%d",
			got.TickRate, got.RoomSize, published.TickRate, published.RoomSize)
	}
}

// And once adopted, the client is the one Update writes through — otherwise the
// knob moves locally and no other node ever hears about it.
func TestUpdateAfterUseRedisReachesTheOtherNodes(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()

	writer := NewLive(LoadViewFromEnv(), nil)
	writer.UseRedis(ctx, rdb)
	if err := writer.Update(ctx, View{TickRate: 30, RoomSize: 5, MinPlayers: 1, SendBuffer: 32}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	reader := NewLive(LoadViewFromEnv(), rdb)
	if got := reader.Get(); got.TickRate != 30 || got.RoomSize != 5 {
		t.Fatalf("a second node read tick=%d room=%d, want tick=30 room=5", got.TickRate, got.RoomSize)
	}
}

// Nil is the no-Redis mode and has to stay a no-op rather than a panic: that is
// how every test and every single-process demo runs.
func TestUseRedisWithNoClientIsANoOp(t *testing.T) {
	l := NewLive(View{TickRate: 20, RoomSize: 4, MinPlayers: 1, SendBuffer: 32}, nil)
	before := l.Get()
	l.UseRedis(context.Background(), nil)
	if l.Get() != before {
		t.Fatalf("UseRedis(nil) changed the view to %+v", l.Get())
	}
	// And Update still works, purely locally.
	if err := l.Update(context.Background(), View{TickRate: 60, RoomSize: 4, MinPlayers: 1, SendBuffer: 32}); err != nil {
		t.Fatalf("Update with no client: %v", err)
	}
	if l.Get().TickRate != 60 {
		t.Fatalf("local update did not land: tick=%d", l.Get().TickRate)
	}
}
