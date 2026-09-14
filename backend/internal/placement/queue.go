package placement

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// TakeWait bounds how long Take parks with nothing to do. Returning after it
// lets a consumer loop notice a cancelled context and do periodic work instead
// of blocking forever.
const TakeWait = 2 * time.Second

// Job is one delivered room request, together with whatever the queue needs to
// retire it again.
//
// The token exists because the Redis queue retires a delivery by removing it
// from a list by value, and the only value that is certain to match is the one
// it actually popped. Ack used to re-marshal the request to produce it, which
// reads as equivalent and is not: protobuf does not promise byte-identical
// output for equal messages, and the encoding is explicitly allowed to differ
// between builds of the library. A single byte of drift makes LREM match
// nothing, so the job stays in the processing list, the reaper decides it is
// stale ninety seconds later, and the match is delivered a second time — the
// one failure the whole ack protocol exists to prevent, arriving silently.
//
// Callers do not construct these. They hand back the Job they were given.
type Job struct {
	Req *pb.RoomRequest
	// token is the queue's own name for this delivery, opaque above this
	// package and empty for queues that do not need one.
	token string
}

// NewJob wraps a request that did not come from a queue delivery, so it has no
// token to retire. For callers that build a room request directly — a test, a
// single-process path — rather than taking one off a queue.
func NewJob(req *pb.RoomRequest) *Job { return &Job{Req: req} }

type Queue interface {
	Enqueue(ctx context.Context, nodeID string, req *pb.RoomRequest) error
	// Take claims the next job for this node. It blocks until one arrives, ctx
	// is done, or TakeWait elapses — in which case it returns (nil, nil).
	//
	// Both implementations must honour that last clause. They did not always:
	// the Redis one polled with a timeout while the memory one parked forever,
	// so a caller written against either would hang or spin against the other.
	Take(ctx context.Context, nodeID string) (*Job, error)
	Ack(ctx context.Context, nodeID string, job *Job) error
	ReapStale(ctx context.Context, nodeID string, maxAge time.Duration) (int, error)
}

type Memory struct {
	mu sync.Mutex
	ch map[string]chan *pb.RoomRequest
}

func NewMemory() *Memory {
	return &Memory{ch: make(map[string]chan *pb.RoomRequest)}
}

func (m *Memory) chFor(node string) chan *pb.RoomRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.ch[node]
	if !ok {
		c = make(chan *pb.RoomRequest, 256)
		m.ch[node] = c
	}
	return c
}

func (m *Memory) Enqueue(ctx context.Context, nodeID string, req *pb.RoomRequest) error {
	select {
	case m.chFor(nodeID) <- req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Memory) Take(ctx context.Context, nodeID string) (*Job, error) {
	t := time.NewTimer(TakeWait)
	defer t.Stop()
	select {
	case req := <-m.chFor(nodeID):
		return &Job{Req: req}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.C:
		return nil, nil
	}
}

// Ack has nothing to retire: a channel receive already removed the job.
func (m *Memory) Ack(_ context.Context, _ string, _ *Job) error { return nil }

func (m *Memory) ReapStale(_ context.Context, _ string, _ time.Duration) (int, error) {
	return 0, nil
}

type Redis struct {
	rdb *redis.Client
}

func NewRedis(rdb *redis.Client) *Redis { return &Redis{rdb: rdb} }

func pendingKey(node string) string { return "gs:jobs:" + node }

func procKey(node string) string { return "gs:jobs:" + node + ":proc" }

func procTSKey(roomID string) string { return "gs:jobts:" + roomID }

func (r *Redis) Enqueue(ctx context.Context, nodeID string, req *pb.RoomRequest) error {
	b, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	return r.rdb.LPush(ctx, pendingKey(nodeID), b).Err()
}

func (r *Redis) Take(ctx context.Context, nodeID string) (*Job, error) {
	raw, err := r.rdb.BLMove(ctx, pendingKey(nodeID), procKey(nodeID), "RIGHT", "LEFT", TakeWait).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	var req pb.RoomRequest
	if err := proto.Unmarshal([]byte(raw), &req); err != nil {
		// Undeliverable either way, so the unmarshal error is what the caller
		// gets — but a failed removal leaves the poison entry in the processing
		// list, where the reaper will keep handing it back every ninety seconds
		// forever. Joined so whoever reads the log sees both halves.
		if rerr := r.rdb.LRem(ctx, procKey(nodeID), 1, raw).Err(); rerr != nil {
			return nil, fmt.Errorf("placement: undecodable job (%w) and could not drop it: %w", err, rerr)
		}
		return nil, err
	}
	// The delivery timestamp is what ReapStale measures age against. Without it
	// the entry sits in the processing list with no recorded age, and the
	// reaper — which skips anything it cannot read a timestamp for — will never
	// requeue it: a match that this node failed to start is then lost rather
	// than retried. So a failure here is returned, and the job is left for the
	// reaper's own timeout rather than started on a node that cannot account
	// for it.
	if err := r.rdb.Set(ctx, procTSKey(req.RoomId), time.Now().Unix(), 15*time.Minute).Err(); err != nil {
		return nil, fmt.Errorf("placement: stamp delivery of %s: %w", req.RoomId, err)
	}
	// The bytes that were popped, kept so Ack can remove exactly them. See Job.
	return &Job{Req: &req, token: raw}, nil
}

func (r *Redis) Ack(ctx context.Context, nodeID string, job *Job) error {
	if job == nil || job.Req == nil {
		return nil
	}
	pipe := r.rdb.Pipeline()
	pipe.LRem(ctx, procKey(nodeID), 1, job.token)
	pipe.Del(ctx, procTSKey(job.Req.RoomId))
	_, err := pipe.Exec(ctx)
	return err
}

func (r *Redis) ReapStale(ctx context.Context, nodeID string, maxAge time.Duration) (int, error) {
	items, err := r.rdb.LRange(ctx, procKey(nodeID), 0, -1).Result()
	if err != nil {
		return 0, err
	}
	cutoff := time.Now().Add(-maxAge).Unix()
	requeued := 0
	for _, raw := range items {
		var req pb.RoomRequest
		if proto.Unmarshal([]byte(raw), &req) != nil {
			continue
		}
		ts, err := r.rdb.Get(ctx, procTSKey(req.RoomId)).Int64()
		if err != nil || ts >= cutoff {
			continue
		}
		pipe := r.rdb.Pipeline()
		pipe.LRem(ctx, procKey(nodeID), 1, raw)
		pipe.LPush(ctx, pendingKey(nodeID), raw)
		pipe.Del(ctx, procTSKey(req.RoomId))
		if _, err := pipe.Exec(ctx); err != nil {
			return requeued, err
		}
		requeued++
	}
	return requeued, nil
}
