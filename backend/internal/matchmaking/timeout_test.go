package matchmaking

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// A queue with no timeout configured must wait for a full room, not form a
// bot-filled one on the first pass.
//
// "wait >= 0" is true immediately, so taking the field literally turned
// QUEUE_TIMEOUT=0 into "every arrival gets its own match, padded with bots" —
// at the matchmaker's 50 ms tick, and with the real players in the queue never
// given the chance to meet each other.
func TestZeroTimeoutWaitsForAFullRoom(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		// Queued a while ago, so any literal reading of the timeout has
		// certainly elapsed.
		queued := time.Now().Add(-time.Hour)
		for i := 0; i < 3; i++ {
			if err := q.Enqueue(ctx, player(fmt.Sprintf("p%d", i), queued)); err != nil {
				t.Fatal(err)
			}
		}

		r := Rules{RoomSize: 4, MinPlayers: 1, Timeout: 0}
		m, err := q.TryForm(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if m != nil {
			t.Fatalf("formed a %d-player match with %d bots and no timeout set",
				len(m.Players), m.Bots)
		}

		// The fourth player fills the room, which is the only thing that should
		// form a match here.
		if err := q.Enqueue(ctx, player("p3", queued)); err != nil {
			t.Fatal(err)
		}
		m, err = q.TryForm(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			t.Fatal("a full room did not form")
		}
		if len(m.Players) != 4 || m.Bots != 0 {
			t.Errorf("got %d players and %d bots, want 4 and 0", len(m.Players), m.Bots)
		}
	})
}

// A negative timeout reads the same as zero: the rule is off.
func TestNegativeTimeoutIsAlsoOff(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		if err := q.Enqueue(ctx, player("solo", time.Now().Add(-time.Hour))); err != nil {
			t.Fatal(err)
		}
		m, err := q.TryForm(ctx, Rules{RoomSize: 4, MinPlayers: 1, Timeout: -time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if m != nil {
			t.Fatalf("a negative timeout formed a match of %d with %d bots", len(m.Players), m.Bots)
		}
	})
}

// The timeout still has to work when it is set, or the guard above would have
// disabled bot-filling outright.
func TestTimeoutStillFormsAShortMatch(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		if err := q.Enqueue(ctx, player("waited", time.Now().Add(-time.Hour))); err != nil {
			t.Fatal(err)
		}
		m, err := q.TryForm(ctx, Rules{RoomSize: 4, MinPlayers: 1, Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			t.Fatal("an expired anchor did not get a match")
		}
		if len(m.Players) != 1 || m.Bots != 3 {
			t.Errorf("got %d players and %d bots, want 1 and 3", len(m.Players), m.Bots)
		}
	})
}
