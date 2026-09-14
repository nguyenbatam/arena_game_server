package app

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/config"
	"github.com/nguyenbatam/arena_game_server/internal/matchmaking"
	"github.com/nguyenbatam/arena_game_server/internal/placement"
	"github.com/nguyenbatam/arena_game_server/internal/rating"
	"github.com/nguyenbatam/arena_game_server/internal/session"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

// The loop that makes skill-based matchmaking mean anything: a rating is read
// when a player queues and written back when their match ends. Break either
// half and the queue is searching around a constant.
func TestQueueCarriesTheRatingAndAMatchMovesIt(t *testing.T) {
	// The shortest real queue timeout, so one player forms a match on the first
	// pass and the test is about the rating, not about waiting. Not zero: that
	// switches the timeout rule off entirely rather than expiring it — see
	// matchmaking.Rules.Timeout.
	v := defaultView()
	v.QueueTimeout = time.Nanosecond
	a := appWithStatic(t, config.Static{}, v)
	ctx := context.Background()

	if err := a.rating.Put(ctx, map[string]int{"veteran": 1600}); err != nil {
		t.Fatal(err)
	}

	c := session.NewConn("veteran", 32)
	a.hub.Add(c)
	a.onJoinQueue(c)

	m, err := a.queue.TryForm(ctx, a.matchRules())
	if err != nil {
		t.Fatal(err)
	}
	if m == nil || len(m.Players) != 1 {
		t.Fatalf("expected the queued player back, got %+v", m)
	}
	if got := m.Players[0].Skill; got != 1600 {
		t.Fatalf("queued with skill %d, want the stored rating 1600", got)
	}

	// Now play that match out: the veteran is beaten by an unrated newcomer.
	req := &pb.RoomRequest{
		RoomId: "r-1",
		Seats: []*pb.Seat{
			{ConnId: "veteran", PlayerId: 1, Name: "veteran"},
			{ConnId: "newcomer", PlayerId: 2, Name: "newcomer"},
		},
	}
	a.recordResult(ctx, req, sim.Snapshot{
		Tick: 100, Ended: true, Winner: 2,
		Players: []sim.Player{
			{ID: 1, Score: 1},
			{ID: 2, Score: 9},
		},
	})

	vet, _ := a.rating.Get(ctx, "veteran")
	new, _ := a.rating.Get(ctx, "newcomer")
	if vet >= 1600 {
		t.Fatalf("the veteran lost and their rating is %d, was 1600", vet)
	}
	if new <= rating.Default {
		t.Fatalf("the newcomer won and their rating is %d, was %d", new, rating.Default)
	}
	// An upset against a much stronger player is worth more than the loser lost
	// is not the claim here; what matters is that the points went somewhere.
	if vet+new <= 0 {
		t.Fatal("ratings went to nothing")
	}
}

// Bots have no identity to carry a rating, and a win over one must not be
// farmable — the timeout fills rooms with them for anyone waiting alone.
func TestAMatchAgainstBotsMovesNobody(t *testing.T) {
	a := appWithStatic(t, config.Static{}, defaultView())
	ctx := context.Background()

	req := &pb.RoomRequest{
		RoomId: "r-bots",
		Seats:  []*pb.Seat{{ConnId: "solo", PlayerId: 1, Name: "solo"}},
	}
	a.recordResult(ctx, req, sim.Snapshot{
		Tick: 100, Ended: true, Winner: 1,
		Players: []sim.Player{
			{ID: 1, Score: 20},
			{ID: 1000, Score: 0, Bot: true},
			{ID: 1001, Score: 0, Bot: true},
		},
	})

	if got, _ := a.rating.Get(ctx, "solo"); got != rating.Default {
		t.Fatalf("farming bots moved a rating to %d", got)
	}
}

// ---------------------------------------------------------------------------
// Batched reads at the end of a match

// countingRatings wraps a rating.Store and records how it was called.
type countingRatings struct {
	inner rating.Store
	gets  atomic.Int32 // single-player reads
	many  atomic.Int32 // batched reads
	seen  atomic.Int32 // ids handed to the batched read
	err   error        // when set, every read fails
}

func (c *countingRatings) Get(ctx context.Context, id string) (int, error) {
	c.gets.Add(1)
	if c.err != nil {
		return rating.Default, c.err
	}
	return c.inner.Get(ctx, id)
}

func (c *countingRatings) GetMany(ctx context.Context, ids []string) ([]int, error) {
	c.many.Add(1)
	c.seen.Add(int32(len(ids)))
	if c.err != nil {
		out := make([]int, len(ids))
		for i := range out {
			out[i] = rating.Default
		}
		return out, c.err
	}
	return c.inner.GetMany(ctx, ids)
}

func (c *countingRatings) Put(ctx context.Context, r map[string]int) error {
	return c.inner.Put(ctx, r)
}

// OnEnd runs on the room's own tick goroutine, and Run's cleanup — the room
// leaving rooms.Count(), and with it the node's advertised load and its
// capacity check — does not happen until it returns. Reading the ratings one
// seat at a time held that open for a round trip per player, which is a
// database query each with the platform tier on.
func TestRecordingAMatchReadsEveryRatingInOneRoundTrip(t *testing.T) {
	a := appWithStatic(t, config.Static{}, defaultView())
	ctx := context.Background()
	counted := &countingRatings{inner: a.rating}
	a.rating = counted

	seats := make([]*pb.Seat, 0, 8)
	players := make([]sim.Player, 0, 8)
	for i := 1; i <= 8; i++ {
		seats = append(seats, &pb.Seat{
			ConnId: fmt.Sprintf("p%d", i), PlayerId: uint32(i), Name: fmt.Sprintf("p%d", i),
		})
		players = append(players, sim.Player{ID: sim.PlayerID(i), Score: uint16(i)})
	}

	a.recordResult(ctx, &pb.RoomRequest{RoomId: "r-batch", Seats: seats}, sim.Snapshot{
		Tick: 100, Ended: true, Winner: 8, Players: players,
	})

	if got := counted.many.Load(); got != 1 {
		t.Errorf("batched read called %d times for one match, want 1", got)
	}
	if got := counted.gets.Load(); got != 0 {
		t.Errorf("recording a match still made %d single-player reads", got)
	}
	if got := counted.seen.Load(); got != 8 {
		t.Errorf("the batched read was given %d ids, want all 8 seats", got)
	}
}

// Batching must not change the numbers. The same eight-player result is
// recorded through the batched path and compared against what Update produces
// from the ratings read one at a time.
func TestBatchedReadsProduceTheSameRatingsAsSingleReads(t *testing.T) {
	a := appWithStatic(t, config.Static{}, defaultView())
	ctx := context.Background()

	start := map[string]int{"p1": 1400, "p2": 1200, "p3": 900, "p4": 1000}
	if err := a.rating.Put(ctx, start); err != nil {
		t.Fatal(err)
	}

	ids := []string{"p1", "p2", "p3", "p4"}
	scores := []int{9, 4, 4, 0}
	before := make([]int, len(ids))
	for i, id := range ids {
		before[i], _ = a.rating.Get(ctx, id)
	}
	want := rating.Update(before, scores)

	seats := make([]*pb.Seat, 0, len(ids))
	players := make([]sim.Player, 0, len(ids))
	for i, id := range ids {
		seats = append(seats, &pb.Seat{ConnId: id, PlayerId: uint32(i + 1), Name: id})
		players = append(players, sim.Player{ID: sim.PlayerID(i + 1), Score: uint16(scores[i])})
	}

	a.recordResult(ctx, &pb.RoomRequest{RoomId: "r-same", Seats: seats}, sim.Snapshot{
		Tick: 100, Ended: true, Winner: 1, Players: players,
	})

	for i, id := range ids {
		got, _ := a.rating.Get(ctx, id)
		if got != want[i] {
			t.Errorf("%s = %d after the batched path, want %d", id, got, want[i])
		}
	}
}

// A rating store that cannot answer must not cost the record of a match that
// was played. The batched read hands back defaults alongside its error, and the
// result is still written on top of them.
func TestABrokenRatingStoreStillRecordsTheMatch(t *testing.T) {
	a := appWithStatic(t, config.Static{}, defaultView())
	ctx := context.Background()
	inner := rating.NewMemory()
	counted := &countingRatings{inner: inner, err: errors.New("rating store is down")}
	a.rating = counted

	a.recordResult(ctx, &pb.RoomRequest{
		RoomId: "r-broken",
		Seats: []*pb.Seat{
			{ConnId: "winner", PlayerId: 1, Name: "winner"},
			{ConnId: "loser", PlayerId: 2, Name: "loser"},
		},
	}, sim.Snapshot{
		Tick: 100, Ended: true, Winner: 1,
		Players: []sim.Player{{ID: 1, Score: 9}, {ID: 2, Score: 1}},
	})

	// Put goes to the inner store, which is working — so the match was recorded
	// against guessed starting ratings rather than dropped.
	win, _ := inner.Get(ctx, "winner")
	lose, _ := inner.Get(ctx, "loser")
	if win <= rating.Default {
		t.Errorf("winner = %d, want above the default %d", win, rating.Default)
	}
	if lose >= rating.Default {
		t.Errorf("loser = %d, want below the default %d", lose, rating.Default)
	}
}

// A node at its ceiling puts the players back in the queue, and it should not
// spend a round trip per seat to do it — that path runs precisely when the node
// has no headroom to spend.
func TestRefusingARoomReadsSkillsInOneRoundTrip(t *testing.T) {
	a := appWithStatic(t, config.Static{MaxRooms: 1}, defaultView())
	counted := &countingRatings{inner: a.rating}
	a.rating = counted
	if err := a.rating.Put(context.Background(), map[string]int{"q2": 1500}); err != nil {
		t.Fatal(err)
	}
	counted.many.Store(0)
	counted.gets.Store(0)
	a.draining.Store(true) // the cheapest way to make startRoom refuse

	a.startRoom(placement.NewJob(&pb.RoomRequest{
		RoomId: "r-refused",
		Seats: []*pb.Seat{
			{ConnId: "q1", PlayerId: 1, Name: "q1", QueuedAt: 1},
			{ConnId: "q2", PlayerId: 2, Name: "q2", QueuedAt: 2},
			{ConnId: "", PlayerId: 3, Name: "bot"},
		},
	}))

	if got := counted.many.Load(); got != 1 {
		t.Errorf("batched read called %d times for one refusal, want 1", got)
	}
	if got := counted.gets.Load(); got != 0 {
		t.Errorf("refusal still made %d single-player reads", got)
	}
	if got := counted.seen.Load(); got != 2 {
		t.Errorf("the batched read was given %d ids, want the 2 seated players", got)
	}

	// And the requeued players keep the skill that was read for them — the
	// seatless bot must not shift anyone's rating onto the wrong player.
	m, err := a.queue.TryForm(context.Background(), matchmaking.Rules{RoomSize: 2, MinPlayers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if m == nil || len(m.Players) != 2 {
		t.Fatalf("expected both players back in the queue, got %+v", m)
	}
	skill := map[string]int32{}
	for _, p := range m.Players {
		skill[p.ConnId] = p.Skill
	}
	if skill["q2"] != 1500 {
		t.Errorf("q2 requeued with skill %d, want 1500", skill["q2"])
	}
	if skill["q1"] != rating.Default {
		t.Errorf("q1 requeued with skill %d, want the default %d", skill["q1"], rating.Default)
	}
}
