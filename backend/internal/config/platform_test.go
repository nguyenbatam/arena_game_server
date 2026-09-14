package config

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

// PLATFORM_DSN decides the whole mode, the way REDIS_ADDR does, so the three
// values it can take are worth spelling out — including the one that looks like
// a typo and is not.
func TestPlatformDSNSelectsTheMode(t *testing.T) {
	for _, tc := range []struct {
		dsn             string
		enabled, memory bool
	}{
		{"", false, false},
		{"memory", true, true},
		// Case-insensitive on purpose: it is a word, not a hostname, and
		// MEMORY in an env file should not quietly try to dial a database
		// called "MEMORY".
		{"MEMORY", true, true},
		{"Memory", true, true},
		{"postgres://u:p@db:5432/arena?sslmode=require", true, false},
		// A value that is only whitespace is set, not absent. It cannot parse,
		// so the process refuses to start rather than running with the tier
		// silently off — see platform.ErrBadDSN.
		{"   ", true, false},
	} {
		s := Static{PlatformDSN: tc.dsn}
		if got := s.PlatformEnabled(); got != tc.enabled {
			t.Errorf("PLATFORM_DSN=%q: enabled = %v, want %v", tc.dsn, got, tc.enabled)
		}
		if got := s.PlatformMemory(); got != tc.memory {
			t.Errorf("PLATFORM_DSN=%q: memory = %v, want %v", tc.dsn, got, tc.memory)
		}
	}
}

func TestPlatformDefaults(t *testing.T) {
	// Hermetic: a developer with PLATFORM_DSN exported must not fail this.
	t.Setenv("PLATFORM_DSN", "")
	s := LoadStatic()
	if s.PlatformEnabled() {
		t.Fatalf("the platform tier is on by default (DSN=%q); a clone must come up without a database", s.PlatformDSN)
	}
	// Wider than REDIS_TIMEOUT on purpose: a purchase is a four-table
	// transaction, not a key-value lookup. If these ever converge, one of them
	// was tuned for the wrong thing.
	if s.PlatformTimeout <= s.RedisTimeout {
		t.Fatalf("PLATFORM_TIMEOUT %s is not wider than REDIS_TIMEOUT %s", s.PlatformTimeout, s.RedisTimeout)
	}
	if s.PlatformMaxConns <= 0 || s.PlatformRateLimit <= 0 {
		t.Fatalf("pool=%d limit=%d", s.PlatformMaxConns, s.PlatformRateLimit)
	}
}

func TestPlatformEnvIsRead(t *testing.T) {
	t.Setenv("PLATFORM_DSN", "postgres://localhost/x")
	t.Setenv("PLATFORM_MAX_CONNS", "4")
	t.Setenv("PLATFORM_TIMEOUT", "7s")
	t.Setenv("PLATFORM_RATE_LIMIT", "11")

	s := LoadStatic()
	if s.PlatformDSN != "postgres://localhost/x" || s.PlatformMaxConns != 4 ||
		s.PlatformTimeout != 7*time.Second || s.PlatformRateLimit != 11 {
		t.Fatalf("got %+v", struct {
			DSN   string
			Conns int
			TO    time.Duration
			RL    int
		}{s.PlatformDSN, s.PlatformMaxConns, s.PlatformTimeout, s.PlatformRateLimit})
	}
}

// The in-memory store is right for a demo and wrong for a deployment, and the
// failure mode is silent and delayed: everything works until the restart that
// throws away every account. Booting anyway is deliberate — a staging tier may
// genuinely want it — so it has to say what it did.
func TestProductionSaysSoWhenAccountsAreHeldInRAM(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("JWT_SECRET", "long-enough-secret-for-a-test")
	t.Setenv("PLATFORM_DSN", "memory")

	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	s := LoadStatic()
	if err := s.Validate(); err != nil {
		t.Fatalf("memory mode must warn, not refuse: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "PLATFORM_DSN=memory") || !strings.Contains(out, "lost on restart") {
		t.Fatalf("production did not say accounts are in RAM, logged: %q", out)
	}
}

func TestADurableDSNDrawsNoWarning(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("JWT_SECRET", "long-enough-secret-for-a-test")
	t.Setenv("PLATFORM_DSN", "postgres://u:p@db:5432/arena?sslmode=require")

	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	if err := LoadStatic().Validate(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "PLATFORM_DSN") {
		t.Fatalf("a real database warned about itself: %q", buf.String())
	}
}

// The limiter warning is noise on a process that does not mount those
// endpoints, and noise is how a real warning gets skimmed past.
func TestThePlatformLimiterOnlyWarnsWhenThereIsSomethingBehindIt(t *testing.T) {
	warned := func(dsn string) bool {
		t.Helper()
		t.Setenv("ENV", "production")
		t.Setenv("JWT_SECRET", "long-enough-secret-for-a-test")
		t.Setenv("PLATFORM_DSN", dsn)
		t.Setenv("PLATFORM_RATE_LIMIT", "0")
		var buf bytes.Buffer
		log.SetOutput(&buf)
		defer log.SetOutput(os.Stderr)
		LoadStatic()
		return strings.Contains(buf.String(), "PLATFORM_RATE_LIMIT=0")
	}
	if warned("") {
		t.Error("warned about a limiter on endpoints that are not mounted")
	}
	if !warned("memory") {
		t.Error("the platform tier is on with its limiter off and nothing said so")
	}
}
