package matchmaking

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
)

func rated(id string, skill int32, queuedAt time.Time) *pb.QueuePlayer {
	return &pb.QueuePlayer{ConnId: id, PlayerId: id, Name: id, Skill: skill, QueuedAt: queuedAt.UnixMilli()}
}

func ids(m *Match) []string {
	out := make([]string, 0, len(m.Players))
	for _, p := range m.Players {
		out = append(out, p.ConnId)
	}
	return out
}

// The whole point of a rating: a match is not formed out of whoever happens to
// be standing there when a queue is deep enough to be selective.
func TestSkillWindowKeepsMismatchedPlayersApart(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		// Distinct arrival times: the queue is ordered by wait, and the anchor
		// it forms a match around is whoever has waited longest.
		now := time.Now()
		if err := q.Enqueue(ctx, rated("novice", 900, now.Add(-2*time.Second))); err != nil {
			t.Fatal(err)
		}
		if err := q.Enqueue(ctx, rated("expert", 2400, now.Add(-time.Second))); err != nil {
			t.Fatal(err)
		}

		r := Rules{RoomSize: 2, MinPlayers: 2, Timeout: time.Hour, SkillWindow: 200}
		m, err := q.TryForm(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if m != nil {
			t.Fatalf("1500 points apart and still matched: %v", ids(m))
		}

		// Someone at the novice's level turns up and the match forms around the
		// two of them, leaving the expert waiting.
		if err := q.Enqueue(ctx, rated("peer", 950, now)); err != nil {
			t.Fatal(err)
		}
		m, err = q.TryForm(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			t.Fatal("two players 50 points apart were not matched")
		}
		got := ids(m)
		if len(got) != 2 || got[0] != "novice" || got[1] != "peer" {
			t.Fatalf("matched %v, want [novice peer]", got)
		}
	})
}

// A window that never grows is a player who never plays. Every shipped
// matchmaker trades quality for wait, and this is where that trade lives.
func TestWindowWidensWithTheWait(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		// The anchor has been waiting ten seconds; the other player arrived now.
		waited := time.Now().Add(-10 * time.Second)
		if err := q.Enqueue(ctx, rated("waiting", 1000, waited)); err != nil {
			t.Fatal(err)
		}
		if err := q.Enqueue(ctx, rated("stronger", 1400, time.Now())); err != nil {
			t.Fatal(err)
		}

		tight := Rules{RoomSize: 2, MinPlayers: 2, Timeout: time.Hour, SkillWindow: 100, MaxWindow: 150}
		if m, err := q.TryForm(ctx, tight); err != nil {
			t.Fatal(err)
		} else if m != nil {
			t.Fatalf("a capped window matched 400 points apart: %v", ids(m))
		}

		// Same starting width, but now it grows by 50 a second: ten seconds of
		// waiting buys 500 points of tolerance, which is enough.
		widening := Rules{RoomSize: 2, MinPlayers: 2, Timeout: time.Hour, SkillWindow: 100, Widen: 50}
		m, err := q.TryForm(ctx, widening)
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			t.Fatal("the window never widened — the anchor waits forever")
		}
		if got := ids(m); len(got) != 2 || got[0] != "waiting" {
			t.Fatalf("matched %v, want the waiting player first", got)
		}
	})
}

// Widening is measured from the anchor's wait, so the player the queue is
// failing is the one who gets served — not whoever happens to be nearest the
// middle of the rating distribution.
func TestAnchorIsAlwaysInTheMatchItFormed(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		old := time.Now().Add(-30 * time.Second)
		if err := q.Enqueue(ctx, rated("anchor", 1500, old)); err != nil {
			t.Fatal(err)
		}
		// A crowd at the bottom of the anchor's (now wide) window, all newer.
		for i := 0; i < 12; i++ {
			if err := q.Enqueue(ctx, rated(fmt.Sprintf("crowd%02d", i), 1000, time.Now())); err != nil {
				t.Fatal(err)
			}
		}

		m, err := q.TryForm(ctx, Rules{
			RoomSize: 4, MinPlayers: 2, Timeout: time.Hour,
			SkillWindow: 100, Widen: 100,
		})
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			t.Fatal("nothing formed with a 30-second anchor and a full queue")
		}
		if got := ids(m); got[0] != "anchor" {
			t.Fatalf("formed %v without the player who had waited longest", got)
		}
	})
}

// Skill must not be able to stop a lonely player from ever getting a game: the
// timeout is the backstop, and it fills with bots as it always did.
func TestTimeoutStillFormsWithBotsUnderASkillWindow(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		if err := q.Enqueue(ctx, rated("alone", 1000, time.Now().Add(-5*time.Second))); err != nil {
			t.Fatal(err)
		}
		if err := q.Enqueue(ctx, rated("faraway", 3000, time.Now())); err != nil {
			t.Fatal(err)
		}

		m, err := q.TryForm(ctx, Rules{
			RoomSize: 4, MinPlayers: 1, Timeout: time.Second,
			SkillWindow: 50, MaxWindow: 50,
		})
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			t.Fatal("the timeout did not fire for a player waiting alone")
		}
		if got := ids(m); len(got) != 1 || got[0] != "alone" {
			t.Fatalf("formed %v, want just the waiting player", got)
		}
		if m.Bots != 3 {
			t.Fatalf("bots = %d, want 3", m.Bots)
		}
	})
}

// A zero window is the old FIFO queue, and the load test and the single-node
// demo both rely on it.
func TestZeroWindowIgnoresSkillEntirely(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		now := time.Now()
		if err := q.Enqueue(ctx, rated("low", 100, now)); err != nil {
			t.Fatal(err)
		}
		if err := q.Enqueue(ctx, rated("high", 4000, now)); err != nil {
			t.Fatal(err)
		}
		m, err := q.TryForm(ctx, Rules{RoomSize: 2, MinPlayers: 2, Timeout: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		if m == nil || len(m.Players) != 2 {
			t.Fatalf("a zero window did not behave as FIFO: %+v", m)
		}
	})
}

func TestWindowAt(t *testing.T) {
	r := Rules{SkillWindow: 100, Widen: 50, MaxWindow: 300}
	for _, tc := range []struct {
		wait time.Duration
		want int
	}{
		{0, 100},
		{2 * time.Second, 200},
		{time.Hour, 300}, // capped
	} {
		if got := r.windowAt(tc.wait); got != tc.want {
			t.Fatalf("windowAt(%s) = %d, want %d", tc.wait, got, tc.want)
		}
	}
	if got := (Rules{}).windowAt(time.Hour); got != 0 {
		t.Fatalf("an unset window widened to %d", got)
	}
}

// A pass looks only so far down the queue, and both implementations have to
// look exactly as far in exactly the same order. Redis holds the queue in a
// sorted set and the memory twin in a slice, so this is the kind of difference
// that would otherwise appear for the first time under production load: a deep
// queue, where one of them reaches a candidate the other never sees.
func TestBothImplementationsScanTheSameDepth(t *testing.T) {
	r := Rules{RoomSize: 2, MinPlayers: 2, Timeout: time.Hour, SkillWindow: 50}
	depth := r.scan() // 16 at room size 2

	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		base := time.Now().Add(-time.Minute)

		// The anchor, then a wall of players nobody can be matched with, then
		// one suitable partner sitting just past the scan depth.
		if err := q.Enqueue(ctx, rated("anchor", 1000, base)); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < depth+4; i++ {
			id := fmt.Sprintf("wall%03d", i)
			if err := q.Enqueue(ctx, rated(id, 3000, base.Add(time.Duration(i+1)*time.Millisecond))); err != nil {
				t.Fatal(err)
			}
		}
		buried := base.Add(time.Duration(depth+10) * time.Millisecond)
		if err := q.Enqueue(ctx, rated("buried", 1000, buried)); err != nil {
			t.Fatal(err)
		}

		if m, err := q.TryForm(ctx, r); err != nil {
			t.Fatal(err)
		} else if m != nil {
			t.Fatalf("matched %v — a partner past the scan depth was reached", ids(m))
		}

		// Bring that same player inside the depth and the match forms. Same
		// queue, same rules; the only thing that changed is how far down they
		// were sitting.
		if err := q.Remove(ctx, "wall000"); err != nil {
			t.Fatal(err)
		}
		if err := q.Remove(ctx, "wall001"); err != nil {
			t.Fatal(err)
		}
		for i := 2; i < 6; i++ {
			if err := q.Remove(ctx, fmt.Sprintf("wall%03d", i)); err != nil {
				t.Fatal(err)
			}
		}
		m, err := q.TryForm(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			t.Fatal("nothing formed once the partner was inside the scan depth")
		}
		if got := ids(m); len(got) != 2 || got[0] != "anchor" || got[1] != "buried" {
			t.Fatalf("formed %v, want [anchor buried]", got)
		}
	})
}
