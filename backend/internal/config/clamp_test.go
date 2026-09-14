package config

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

// Room size has a hard ceiling: past roughly 28 players a full snapshot
// outgrows one UDP datagram, and the transport drops oversized datagrams
// rather than fragmenting. The compile-time snapshot tests pin the measured
// threshold; this clamp is what stops a hot config push from walking past it
// at runtime, where no test is watching.
func TestRoomSizeIsClampedOnHotUpdate(t *testing.T) {
	l := NewLive(View{TickRate: 20, RoomSize: 8, MinPlayers: 2, MatchSeconds: 60, SendBuffer: 32}, nil)
	if err := l.Update(context.Background(), View{
		TickRate: 20, RoomSize: 500, MinPlayers: 2, MatchSeconds: 60, SendBuffer: 32,
	}); err != nil {
		t.Fatal(err)
	}
	if got := l.Get().RoomSize; got != MaxRoomSize {
		t.Fatalf("room_size = %d after a push of 500, want it clamped to %d", got, MaxRoomSize)
	}
}

func TestRoomSizeClampAtTheBoundary(t *testing.T) {
	for _, tc := range []struct{ in, want int }{
		{MaxRoomSize - 1, MaxRoomSize - 1},
		{MaxRoomSize, MaxRoomSize},
		{MaxRoomSize + 1, MaxRoomSize},
	} {
		v := View{TickRate: 20, RoomSize: tc.in, MinPlayers: 1, MatchSeconds: 60, SendBuffer: 32}
		v.clamp()
		if v.RoomSize != tc.want {
			t.Errorf("clamp(%d) = %d, want %d", tc.in, v.RoomSize, tc.want)
		}
	}
}

// MinPlayers must never end up above RoomSize, or TryForm can never form.
func TestMinPlayersNeverExceedsRoomSize(t *testing.T) {
	v := View{TickRate: 20, RoomSize: 4, MinPlayers: 900, MatchSeconds: 60, SendBuffer: 32}
	v.clamp()
	if v.MinPlayers > v.RoomSize {
		t.Fatalf("min_players %d > room_size %d", v.MinPlayers, v.RoomSize)
	}
	if v.MinPlayers > MaxRoomSize {
		t.Fatalf("min_players %d above the ceiling", v.MinPlayers)
	}
}

func TestClampFloors(t *testing.T) {
	v := View{TickRate: 20, RoomSize: 0, MinPlayers: 0, SendBuffer: 1}
	v.clamp()
	if v.MinPlayers < 1 {
		t.Errorf("min_players = %d", v.MinPlayers)
	}
	if v.RoomSize < v.MinPlayers {
		t.Errorf("room_size %d below min_players %d", v.RoomSize, v.MinPlayers)
	}
	if v.SendBuffer < 4 {
		t.Errorf("send_buffer = %d", v.SendBuffer)
	}
}

// The same bounds must apply whichever door the value came in through.
func TestViewFromPBAppliesTheSameBounds(t *testing.T) {
	v := ViewFromPB(View{
		TickRate: 30, RoomSize: 9999, MinPlayers: 1,
		QueueTimeout: time.Second, MatchSeconds: 60, SendBuffer: 32,
	}.ToPB())
	if v.RoomSize > MaxRoomSize {
		t.Fatalf("ViewFromPB let room_size through as %d", v.RoomSize)
	}
}

// Only rates the simulation is built for are accepted; anything else keeps the
// current value rather than silently retiming every room.
func TestTickRateIsRestrictedToKnownValues(t *testing.T) {
	base := View{TickRate: 20, RoomSize: 8, MinPlayers: 1, MatchSeconds: 60, SendBuffer: 32}
	for _, tr := range []int{20, 30, 60} {
		in := base
		in.TickRate = tr
		if got := ViewFromPB(in.ToPB()).TickRate; got != tr {
			t.Errorf("tick_rate %d became %d", tr, got)
		}
	}
	for _, tr := range []int{0, 7, 45, 1000, -1} {
		in := base
		in.TickRate = tr
		if got := ViewFromPB(in.ToPB()).TickRate; got == tr && tr != 20 {
			t.Errorf("tick_rate %d was accepted", tr)
		}
	}
}

// Zero is the documented way to switch a limiter off, which is exactly what a
// load-test recipe does — and exactly what must not travel into a production
// manifest unnoticed. Booting is still the right call; going quiet is not.
func TestProductionWarnsOnDisabledLimiter(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("HELLO_RATE_LIMIT", "0")

	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	s := LoadStatic()
	if s.HelloRateLimit != 0 {
		t.Fatalf("HELLO_RATE_LIMIT=0 must disable the limiter, got %d", s.HelloRateLimit)
	}
	if !strings.Contains(buf.String(), "HELLO_RATE_LIMIT") {
		t.Fatalf("production must say a limiter is off, logged: %q", buf.String())
	}
	// A limiter that is still on must not be reported as off.
	if strings.Contains(buf.String(), "QUEUE_RATE_LIMIT") {
		t.Fatalf("QUEUE_RATE_LIMIT defaults to 12 in production, should not warn: %q", buf.String())
	}
}

// A config has to survive its own round trip: GET, PUT the same document back,
// and get the same thing. Zero means "unspecified" everywhere in
// DynamicConfig, which is fine until a field arrives where zero is a real
// setting — and for the skill knobs it is: no window is pure FIFO, no widening
// is a fixed window, no cap is unbounded growth.
func TestSkillKnobsSurviveARoundTrip(t *testing.T) {
	for _, v := range []View{
		{SkillWindow: 0, SkillWiden: 0, SkillMaxWindow: 0},
		{SkillWindow: 250, SkillWiden: 0, SkillMaxWindow: 0},
		{SkillWindow: 250, SkillWiden: 75, SkillMaxWindow: 900},
	} {
		got := ViewFromPB(v.ToPB())
		if got.SkillWindow != v.SkillWindow || got.SkillWiden != v.SkillWiden ||
			got.SkillMaxWindow != v.SkillMaxWindow {
			t.Fatalf("round trip of (%d,%d,%d) gave (%d,%d,%d)",
				v.SkillWindow, v.SkillWiden, v.SkillMaxWindow,
				got.SkillWindow, got.SkillWiden, got.SkillMaxWindow)
		}
	}
}

// Switching skill matching off has to stick. It used to land back on whatever
// the environment said, because the "off" value and the "unspecified" value
// were the same number.
func TestSkillCanBeSwitchedOff(t *testing.T) {
	t.Setenv("SKILL_WINDOW", "400")
	v := LoadViewFromEnv()
	if v.SkillWindow != 400 {
		t.Fatalf("env gave skill_window %d, want 400", v.SkillWindow)
	}
	v.SkillWindow = 0
	if got := ViewFromPB(v.ToPB()).SkillWindow; got != 0 {
		t.Fatalf("skill_window came back as %d after being switched off", got)
	}
}

// UDP_ADDR="" is documented as "do not listen", and it has to actually mean
// that: the fallback opened :8082 anyway, so a process told to run without UDP
// would fail to bind whenever anything else already held the port — and exit.
func TestEmptyUDPAddrDisablesUDP(t *testing.T) {
	t.Setenv("UDP_ADDR", "")
	if got := LoadStatic().UDPAddr; got != "" {
		t.Fatalf("UDP_ADDR=\"\" gave %q, want it disabled", got)
	}
}

func TestUnsetUDPAddrTakesTheDefault(t *testing.T) {
	t.Setenv("UDP_ADDR", "")
	os.Unsetenv("UDP_ADDR")
	if got := LoadStatic().UDPAddr; got != ":8082" {
		t.Fatalf("unset UDP_ADDR gave %q, want the default :8082", got)
	}
}

func TestExplicitUDPAddrWins(t *testing.T) {
	t.Setenv("UDP_ADDR", ":9999")
	if got := LoadStatic().UDPAddr; got != ":9999" {
		t.Fatalf("UDP_ADDR=:9999 gave %q", got)
	}
}

// MaxCCU has the same shape as the skill knobs and had the same bug: zero is a
// real setting — App.onHello only enforces a ceiling when it is positive, so
// zero is the documented way to say a gateway has none — and zero is also what
// DynamicConfig means by "unspecified". An operator turning the ceiling off
// through /admin/config got the environment's number handed straight back.
func TestMaxCCUCeilingCanBeSwitchedOff(t *testing.T) {
	t.Setenv("MAX_CCU", "12000")
	v := LoadViewFromEnv()
	if v.MaxCCU != 12000 {
		t.Fatalf("env gave max_ccu %d, want 12000", v.MaxCCU)
	}
	v.MaxCCU = 0
	if got := ViewFromPB(v.ToPB()).MaxCCU; got != 0 {
		t.Fatalf("max_ccu came back as %d after the ceiling was removed", got)
	}
}

func TestMaxCCUSurvivesARoundTrip(t *testing.T) {
	t.Setenv("MAX_CCU", "12000")
	for _, want := range []int{0, 1, 500, 12000, 250000} {
		v := LoadViewFromEnv()
		v.MaxCCU = want
		if got := ViewFromPB(v.ToPB()).MaxCCU; got != want {
			t.Errorf("round trip of max_ccu %d gave %d", want, got)
		}
	}
}

// A negative arriving over the wire is the "off" encoding, not a ceiling of
// minus one: clamp has to land it on zero so nothing downstream compares
// against a negative.
func TestNegativeMaxCCUClampsToNoCeiling(t *testing.T) {
	v := View{MaxCCU: -5}
	v.clamp()
	if v.MaxCCU != 0 {
		t.Fatalf("clamp left max_ccu at %d, want 0", v.MaxCCU)
	}
}
