package matchmaking

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestFormFullRoom(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		if err := q.Enqueue(ctx, &pb.QueuePlayer{ConnId: string(rune('a' + i))}); err != nil {
			t.Fatal(err)
		}
	}
	m, err := q.TryForm(ctx, Rules{RoomSize: 8, MinPlayers: 2, Timeout: time.Second})
	if err != nil || m == nil || len(m.Players) != 8 {
		t.Fatalf("got %+v err=%v", m, err)
	}
}

func TestTimeoutFillBots(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	_ = q.Enqueue(ctx, &pb.QueuePlayer{ConnId: "a", QueuedAt: time.Now().Add(-5 * time.Second).UnixMilli()})
	_ = q.Enqueue(ctx, &pb.QueuePlayer{ConnId: "b", QueuedAt: time.Now().Add(-5 * time.Second).UnixMilli()})
	m, err := q.TryForm(ctx, Rules{RoomSize: 8, MinPlayers: 2, Timeout: time.Second})
	if err != nil || m == nil || m.Bots != 6 {
		t.Fatalf("got %+v err=%v", m, err)
	}
}

func TestConcurrentEnqueueNoDuplicate(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = q.Enqueue(ctx, &pb.QueuePlayer{ConnId: fmt.Sprintf("%d", i)})
		}(i)
	}
	wg.Wait()
	seen := map[string]bool{}
	total := 0
	for {
		m, err := q.TryForm(ctx, Rules{RoomSize: 8, MinPlayers: 8, Timeout: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			break
		}
		for _, p := range m.Players {
			if seen[p.ConnId] {
				t.Fatalf("duplicate %s", p.ConnId)
			}
			seen[p.ConnId] = true
			total++
		}
	}
	if total != 200 {
		t.Fatalf("total %d", total)
	}
}
