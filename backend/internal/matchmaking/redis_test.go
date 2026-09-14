package matchmaking

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/redis/go-redis/v9"
)

func redisQueue(t *testing.T) *RedisQueue {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewRedis(rdb)
}

// The memory and Redis queues must form matches the same way. They are swapped
// by a single env var, so a behavioural difference between them would only ever
// show up in production.
func forEachQueue(t *testing.T, fn func(t *testing.T, q Queue)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { fn(t, NewMemory()) })
	t.Run("redis", func(t *testing.T) { fn(t, redisQueue(t)) })
}

func player(id string, queuedAt time.Time) *pb.QueuePlayer {
	return &pb.QueuePlayer{ConnId: id, PlayerId: id, Name: id, QueuedAt: queuedAt.UnixMilli()}
}

func TestRedisFormsWhenRoomIsFull(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		now := time.Now()
		for i := 0; i < 4; i++ {
			if err := q.Enqueue(ctx, player(fmt.Sprintf("p%d", i), now)); err != nil {
				t.Fatal(err)
			}
		}

		// Not enough yet, and the wait has not expired.
		m, err := q.TryForm(ctx, Rules{RoomSize: 8, MinPlayers: 2, Timeout: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		if m != nil {
			t.Fatalf("formed early with 4/8 players: %+v", m)
		}

		m, err = q.TryForm(ctx, Rules{RoomSize: 4, MinPlayers: 2, Timeout: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		if m == nil || len(m.Players) != 4 || m.Bots != 0 {
			t.Fatalf("want a full 4-player match, got %+v", m)
		}

		depth, _ := q.Depth(ctx)
		if depth != 0 {
			t.Fatalf("queue still holds %d after forming — players were not popped", depth)
		}
	})
}

func TestRedisFillsBotsAfterTimeout(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		old := time.Now().Add(-time.Minute)
		for i := 0; i < 2; i++ {
			if err := q.Enqueue(ctx, player(fmt.Sprintf("p%d", i), old)); err != nil {
				t.Fatal(err)
			}
		}

		m, err := q.TryForm(ctx, Rules{RoomSize: 8, MinPlayers: 2, Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			t.Fatal("waited past the timeout with enough players and still no match")
		}
		if len(m.Players) != 2 || m.Bots != 6 {
			t.Fatalf("want 2 players + 6 bots, got %d players + %d bots", len(m.Players), m.Bots)
		}
	})
}

func TestRedisDoesNotFormBelowMinPlayers(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		if err := q.Enqueue(ctx, player("lonely", time.Now().Add(-time.Hour))); err != nil {
			t.Fatal(err)
		}
		m, err := q.TryForm(ctx, Rules{RoomSize: 8, MinPlayers: 3, Timeout: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		if m != nil {
			t.Fatalf("formed with 1 player when min is 3: %+v", m)
		}
	})
}

// Enqueue is idempotent per connection: a client spamming join must not take
// several seats in the same match.
func TestRedisEnqueueIsIdempotent(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		now := time.Now()
		for i := 0; i < 5; i++ {
			if err := q.Enqueue(ctx, player("same", now)); err != nil {
				t.Fatal(err)
			}
		}
		if depth, _ := q.Depth(ctx); depth != 1 {
			t.Fatalf("depth = %d after 5 enqueues of one conn, want 1", depth)
		}
	})
}

func TestRedisRemoveTakesPlayerOut(t *testing.T) {
	forEachQueue(t, func(t *testing.T, q Queue) {
		ctx := context.Background()
		now := time.Now()
		_ = q.Enqueue(ctx, player("a", now))
		_ = q.Enqueue(ctx, player("b", now))
		if err := q.Remove(ctx, "a"); err != nil {
			t.Fatal(err)
		}
		if depth, _ := q.Depth(ctx); depth != 1 {
			t.Fatalf("depth = %d after removing one of two", depth)
		}
		m, _ := q.TryForm(ctx, Rules{RoomSize: 1, MinPlayers: 1, Timeout: time.Hour})
		if m == nil || len(m.Players) != 1 || m.Players[0].ConnId != "b" {
			t.Fatalf("removed player came back: %+v", m)
		}
	})
}

// The headline claim in the README: two matchmaker replicas popping the same
// queue must never hand one player to two matches.
func TestRedisConcurrentFormNeverDoubleBooks(t *testing.T) {
	q := redisQueue(t)
	ctx := context.Background()
	now := time.Now()
	const total = 64
	for i := 0; i < total; i++ {
		if err := q.Enqueue(ctx, player(fmt.Sprintf("p%02d", i), now)); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				m, err := q.TryForm(ctx, Rules{RoomSize: 4, MinPlayers: 4, Timeout: time.Hour})
				if err != nil || m == nil {
					return
				}
				mu.Lock()
				for _, p := range m.Players {
					seen[p.ConnId]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != total {
		t.Fatalf("placed %d distinct players, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s was placed in %d matches — two replicas took the same player", id, n)
		}
	}
	if depth, _ := q.Depth(ctx); depth != 0 {
		t.Fatalf("%d players left in the queue", depth)
	}
}
