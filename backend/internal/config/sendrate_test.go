package config

import (
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
)

// SnapshotEvery turns a send rate in Hz into the tick interval a room wants.
//
// The two numbers are separate for the reason every engine keeps them separate:
// the tick rate buys hit resolution and input latency, the send rate costs
// egress — and egress grows with the square of the room size. Tied together,
// moving TICK_RATE from 20 to 60 through /admin/config tripled every room's
// outbound traffic as a side effect of asking for a better simulation.
func TestSnapshotEveryConvertsARateIntoAnInterval(t *testing.T) {
	cases := []struct {
		name     string
		rate     int
		tickRate int
		want     int
	}{
		{"unset sends every tick", 0, 60, 1},
		{"negative is unset", -5, 60, 1},
		{"60 Hz simulation, 20 Hz snapshots", 20, 60, 3},
		{"60 Hz simulation, 30 Hz snapshots", 30, 60, 2},
		{"20 Hz simulation, 20 Hz snapshots", 20, 20, 1},
		{"a rate above the tick rate is every tick", 120, 20, 1},
		{"a rate equal to the tick rate is every tick", 60, 60, 1},
		{"rounds down so the room never sends less often than asked", 25, 60, 2},
		{"a nonsense tick rate is every tick", 20, 0, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := Static{SnapshotRate: c.rate}
			if got := s.SnapshotEvery(c.tickRate); got != c.want {
				t.Fatalf("SnapshotRate=%d tickRate=%d -> %d, want %d",
					c.rate, c.tickRate, got, c.want)
			}
		})
	}
}

// The interval it produces must never be zero — a room dividing by it would
// panic, and one sending every zero ticks is not a thing.
func TestSnapshotEveryIsNeverZero(t *testing.T) {
	for rate := -10; rate <= 200; rate++ {
		for _, tr := range []int{0, 1, 20, 30, 60} {
			s := Static{SnapshotRate: rate}
			if got := s.SnapshotEvery(tr); got < 1 {
				t.Fatalf("SnapshotRate=%d tickRate=%d produced an interval of %d", rate, tr, got)
			}
		}
	}
}

// The two new knobs have to come out of the environment with the defaults the
// documentation claims: sending every tick, and a warmup that is on.
func TestNetcodeDefaults(t *testing.T) {
	t.Setenv("SNAPSHOT_RATE", "")
	t.Setenv("WARMUP_TIMEOUT", "")
	s := LoadStatic()
	if s.SnapshotRate != 0 {
		t.Fatalf("SNAPSHOT_RATE defaults to %d, want 0 (every tick)", s.SnapshotRate)
	}
	if s.SnapshotEvery(60) != 1 {
		t.Fatal("the default must leave the send rate tied to the tick rate")
	}
	if s.WarmupTimeout <= 0 {
		t.Fatalf("WARMUP_TIMEOUT defaults to %s, want it on", s.WarmupTimeout)
	}
}

// And they have to be settable, or the knob is decoration.
func TestNetcodeKnobsReadTheEnvironment(t *testing.T) {
	t.Setenv("SNAPSHOT_RATE", "20")
	t.Setenv("WARMUP_TIMEOUT", "250ms")
	s := LoadStatic()
	if s.SnapshotRate != 20 {
		t.Fatalf("SNAPSHOT_RATE = %d, want 20", s.SnapshotRate)
	}
	if s.SnapshotEvery(60) != 3 {
		t.Fatalf("20 Hz on a 60 Hz tick is every %d ticks, want 3", s.SnapshotEvery(60))
	}
	if s.WarmupTimeout != 250*time.Millisecond {
		t.Fatalf("WARMUP_TIMEOUT = %s, want 250ms", s.WarmupTimeout)
	}
}

// Zero switches the warmup off, which is what a single-process deployment that
// does not want the wait gets.
func TestWarmupCanBeSwitchedOff(t *testing.T) {
	t.Setenv("WARMUP_TIMEOUT", "0s")
	if got := LoadStatic().WarmupTimeout; got != 0 {
		t.Fatalf("WARMUP_TIMEOUT=0s left %s", got)
	}
}

// The reconnect window is enforced against a presence record that expires, so a
// grace longer than the record survives is a knob that silently does nothing.
// Clamped and said out loud, the way room_size is against MaxRoomSize.
func TestDisconnectGraceIsClampedToWhatPresenceCanHonour(t *testing.T) {
	v := View{
		TickRate: 20, RoomSize: 8, MinPlayers: 2, MatchSeconds: 60, SendBuffer: 32,
		DisconnectGrace: MaxDisconnectGrace + 10*time.Minute,
	}
	v.clamp()
	if v.DisconnectGrace != MaxDisconnectGrace {
		t.Fatalf("disconnect grace = %s, want it clamped to %s", v.DisconnectGrace, MaxDisconnectGrace)
	}
}

// A grace inside the window is left exactly alone — the clamp is a ceiling, not
// a default.
func TestAReasonableDisconnectGraceIsUntouched(t *testing.T) {
	for _, d := range []time.Duration{0, time.Second, 15 * time.Second, MaxDisconnectGrace} {
		v := View{
			TickRate: 20, RoomSize: 8, MinPlayers: 2, MatchSeconds: 60, SendBuffer: 32,
			DisconnectGrace: d,
		}
		v.clamp()
		if v.DisconnectGrace != d {
			t.Fatalf("grace %s became %s", d, v.DisconnectGrace)
		}
	}
}

// And a hot push over /admin/config goes through the same ceiling, which is the
// path that actually matters: DISCONNECT_GRACE is hot-reloadable, so this is
// how an operator would set an impossible one.
func TestAHotPushCannotSetAnImpossibleGrace(t *testing.T) {
	v := ViewFromPB(&pb.DynamicConfig{
		TickRate:          20,
		DisconnectGraceMs: (MaxDisconnectGrace + time.Hour).Milliseconds(),
	})
	if v.DisconnectGrace != MaxDisconnectGrace {
		t.Fatalf("hot push set a grace of %s, want %s", v.DisconnectGrace, MaxDisconnectGrace)
	}
}
