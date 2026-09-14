package presence

import (
	"context"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/config"
)

func rec(id string, status pb.PresenceStatus) *pb.Presence {
	return &pb.Presence{PlayerId: id, Name: "p", Status: status, RoomId: "r"}
}

// The configured reconnect window has to fit inside the record that enforces
// it. Raise one without the other and DISCONNECT_GRACE quietly stops meaning
// anything past the TTL: the record is gone, and the player is greeted as a new
// arrival however generous the number says it is.
func TestDisconnectTTLCoversTheConfiguredGrace(t *testing.T) {
	if config.MaxDisconnectGrace > DisconnectTTL {
		t.Fatalf("config.MaxDisconnectGrace (%s) is longer than the record survives (%s)",
			config.MaxDisconnectGrace, DisconnectTTL)
	}
}

// The memory store expires records the way the Redis one does. It used to
// expire nothing at all, which made it a leak and a disagreement at once: a
// reconnect hours later was a returning player in development and a new arrival
// in production.
func TestMemoryRecordsExpire(t *testing.T) {
	now := time.Now()
	s := NewMemory()
	s.now = func() time.Time { return now }
	ctx := context.Background()

	if err := s.Set(ctx, rec("p1", pb.PresenceStatus_PRESENCE_STATUS_ONLINE)); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get(ctx, "p1"); got == nil {
		t.Fatal("a record written now is already gone")
	}

	now = now.Add(OnlineTTL - time.Second)
	if got, _ := s.Get(ctx, "p1"); got == nil {
		t.Fatal("an online record expired before its TTL")
	}
	now = now.Add(2 * time.Second)
	if got, _ := s.Get(ctx, "p1"); got != nil {
		t.Fatal("an online record outlived its TTL")
	}
}

// A disconnected record gets the longer window, because it is the one a
// reconnect reads.
func TestDisconnectedRecordsGetTheLongerWindow(t *testing.T) {
	now := time.Now()
	s := NewMemory()
	s.now = func() time.Time { return now }
	ctx := context.Background()

	if err := s.Set(ctx, rec("p2", pb.PresenceStatus_PRESENCE_STATUS_DISCONNECTED)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(OnlineTTL + time.Second)
	if got, _ := s.Get(ctx, "p2"); got == nil {
		t.Fatal("a disconnected record expired on the online TTL")
	}
	now = now.Add(DisconnectTTL)
	if got, _ := s.Get(ctx, "p2"); got != nil {
		t.Fatal("a disconnected record outlived its TTL")
	}
}

// Expiry is measured from the write, not from Seen. Seen is deliberately
// allowed to name an older moment than the write that carried it — that is how
// a reconnect ages a record past the grace window — and measuring the TTL from
// it would delete records the moment they were written.
func TestExpiryIsMeasuredFromTheWriteNotFromSeen(t *testing.T) {
	now := time.Now()
	s := NewMemory()
	s.now = func() time.Time { return now }

	p := rec("p3", pb.PresenceStatus_PRESENCE_STATUS_DISCONNECTED)
	p.Seen = now.Add(-time.Hour).Unix()
	if err := s.Set(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(context.Background(), "p3")
	if got == nil {
		t.Fatal("a record carrying an old Seen was treated as already expired")
	}
	if got.Seen != p.Seen {
		t.Fatalf("Seen was overwritten: %d, want %d", got.Seen, p.Seen)
	}
}

// The map must not grow without bound. This is the leak: onClose deletes the
// record of a player who was not in a match, and writes one for a player who
// was — so every disconnect from a match left an entry nothing ever removed.
func TestTheMemoryStoreDoesNotGrowWithoutBound(t *testing.T) {
	now := time.Now()
	s := NewMemory()
	s.now = func() time.Time { return now }
	ctx := context.Background()

	// A long churn of players who each disconnect once and never come back.
	for i := 0; i < 5000; i++ {
		id := "player-" + time.Duration(i).String()
		if err := s.Set(ctx, rec(id, pb.PresenceStatus_PRESENCE_STATUS_DISCONNECTED)); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
	}

	s.mu.Lock()
	held := len(s.m)
	s.mu.Unlock()

	// Records live DisconnectTTL and arrive one a second, so the live set is
	// bounded by the window. A generous ceiling: the point is that it is
	// bounded by time rather than by how long the process has been running.
	if ceiling := int(DisconnectTTL/time.Second) * 4; held > ceiling {
		t.Fatalf("holding %d records after 5000 disconnects; the window only covers %v",
			held, DisconnectTTL)
	}
}
