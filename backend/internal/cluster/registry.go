package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type GameServer = pb.GameServer
type RoomInfo = pb.RoomInfo

type Registry interface {
	Heartbeat(ctx context.Context, gs *pb.GameServer) error
	// Unregister takes a node out of the placement pool immediately.
	//
	// Draining is advertised in the heartbeat, but a node that is shutting down
	// stops heartbeating, and the record it left behind stays valid for its
	// full TTL. Fifteen seconds of a matchmaker placing matches onto a process
	// that is on its way out is fifteen seconds of players being sent to a
	// server that will never start their room.
	Unregister(ctx context.Context, nodeID string) error
	PickLeastLoaded(ctx context.Context) (*pb.GameServer, error)
	ReleaseInflight(ctx context.Context, nodeID string) error
	RegisterRoom(ctx context.Context, r *pb.RoomInfo) error
	GetRoom(ctx context.Context, roomID string) (*pb.RoomInfo, error)
	UnregisterRoom(ctx context.Context, roomID string) error
	ListServers(ctx context.Context) ([]*pb.GameServer, error)
}

type Memory struct {
	mu      sync.Mutex
	self    *pb.GameServer
	servers map[string]*pb.GameServer
	// inflight counts rooms placed on a node but not yet reported in one of its
	// heartbeats. Kept beside the server record rather than added into its
	// Rooms, because Heartbeat replaces that record wholesale every couple of
	// seconds and would erase the reservation — the same reason the Redis twin
	// keeps its own key instead of writing back into the blob.
	inflight map[string]int32
	rooms    map[string]*pb.RoomInfo
}

func NewMemory(self *pb.GameServer) *Memory {
	cp := proto.Clone(self).(*pb.GameServer)
	cp.UpdatedAt = time.Now().Unix()
	m := &Memory{
		self: cp, servers: map[string]*pb.GameServer{},
		inflight: map[string]int32{}, rooms: map[string]*pb.RoomInfo{},
	}
	m.servers[cp.Id] = proto.Clone(cp).(*pb.GameServer)
	return m
}

func (m *Memory) Heartbeat(_ context.Context, gs *pb.GameServer) error {
	m.mu.Lock()
	cp := proto.Clone(gs).(*pb.GameServer)
	cp.UpdatedAt = time.Now().Unix()
	m.servers[cp.Id] = cp
	// The fallback candidate has to learn what this node has become, or the
	// capacity and draining flags it heartbeats are only enforced against
	// *other* nodes. In single-process mode the fallback is the only path that
	// is ever taken, so a stale self record there means the ceiling is not
	// enforced at all: placement keeps choosing the node, the node refuses the
	// job, the players go back in the queue, and the pair spin at the
	// matchmaker's tick rate — measured at 150 refusals for 16 players.
	if m.self != nil && m.self.Id == cp.Id {
		m.self = proto.Clone(cp).(*pb.GameServer)
	}
	m.mu.Unlock()
	return nil
}

// accepting reports whether a node should be handed another match.
//
// Balancing is not the same as refusing. PickLeastLoaded always returned the
// least loaded node, which is the right answer right up to the point where
// every node is past what it can simulate — and then it keeps handing matches
// to whichever server is failing its tick budget by the smallest margin. A
// declared ceiling is what turns "spread the load" into "and stop when there is
// none left", and the players already in those rooms are the ones it protects.
func accepting(gs *pb.GameServer, load int32) bool {
	if gs == nil || gs.Draining {
		return false
	}
	return gs.Capacity <= 0 || load < gs.Capacity
}

func (m *Memory) PickLeastLoaded(_ context.Context) (*pb.GameServer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var best *pb.GameServer
	var bestLoad int32
	now := time.Now().Unix()
	for _, gs := range m.servers {
		if now-gs.UpdatedAt > 15 {
			continue
		}
		load := gs.Rooms + m.inflight[gs.Id]
		if !accepting(gs, load) {
			continue
		}
		if best == nil || load < bestLoad || (load == bestLoad && gs.Id < best.Id) {
			best, bestLoad = gs, load
		}
	}
	if best == nil {
		// Single-process mode: this node is the fleet, and its own record may
		// be older than the staleness window if nothing is heartbeating.
		// Load is the node's own room count plus what is reserved on it — the
		// same sum the loop above uses. Passing only the reservations would
		// compare an empty counter against the ceiling and accept every time.
		// Checked before the fields are read. accepting handles a nil server,
		// but evaluating the load argument would dereference it first — so the
		// guard it offers was unreachable from here.
		if m.self == nil || !accepting(m.self, m.self.Rooms+m.inflight[m.self.Id]) {
			return nil, nil
		}
		best = m.self
	}
	m.inflight[best.Id]++
	return proto.Clone(best).(*pb.GameServer), nil
}

func (m *Memory) Unregister(_ context.Context, nodeID string) error {
	m.mu.Lock()
	delete(m.servers, nodeID)
	if m.self != nil && m.self.Id == nodeID {
		// The fallback candidate has to go too, or single-process mode keeps
		// placing matches on a node that is shutting down.
		m.self.Draining = true
	}
	m.mu.Unlock()
	return nil
}

// ReleaseInflight gives a reservation back. It floors at zero: released more
// often than taken — a retry path that unwinds twice — a node would otherwise
// sit at a permanently negative load and collect every match in the fleet.
func (m *Memory) ReleaseInflight(_ context.Context, id string) error {
	m.mu.Lock()
	if n := m.inflight[id]; n > 1 {
		m.inflight[id] = n - 1
	} else {
		delete(m.inflight, id)
	}
	m.mu.Unlock()
	return nil
}

func (m *Memory) RegisterRoom(_ context.Context, r *pb.RoomInfo) error {
	m.mu.Lock()
	m.rooms[r.RoomId] = proto.Clone(r).(*pb.RoomInfo)
	m.mu.Unlock()
	return nil
}

func (m *Memory) GetRoom(_ context.Context, roomID string) (*pb.RoomInfo, error) {
	m.mu.Lock()
	r, ok := m.rooms[roomID]
	m.mu.Unlock()
	if !ok {
		return nil, nil
	}
	return proto.Clone(r).(*pb.RoomInfo), nil
}

func (m *Memory) UnregisterRoom(_ context.Context, roomID string) error {
	m.mu.Lock()
	delete(m.rooms, roomID)
	m.mu.Unlock()
	return nil
}

func (m *Memory) ListServers(_ context.Context) ([]*pb.GameServer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*pb.GameServer, 0, len(m.servers))
	for _, gs := range m.servers {
		out = append(out, proto.Clone(gs).(*pb.GameServer))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Id < out[j].Id })
	return out, nil
}

type Redis struct {
	rdb *redis.Client
}

func NewRedis(rdb *redis.Client) *Redis { return &Redis{rdb: rdb} }

func (r *Redis) Heartbeat(ctx context.Context, gs *pb.GameServer) error {
	// Stamped on a copy: the caller reuses its GameServer across ticks and is
	// not expecting this call to write into it.
	cp := proto.Clone(gs).(*pb.GameServer)
	cp.UpdatedAt = time.Now().Unix()
	b, err := proto.Marshal(cp)
	if err != nil {
		return err
	}
	pipe := r.rdb.Pipeline()
	pipe.Set(ctx, "gs:"+cp.Id, b, 15*time.Second)
	pipe.SAdd(ctx, "gs:index", cp.Id)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *Redis) ListServers(ctx context.Context) ([]*pb.GameServer, error) {
	ids, err := r.rdb.SMembers(ctx, "gs:index").Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = "gs:" + id
	}
	vals, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]*pb.GameServer, 0, len(ids))
	now := time.Now().Unix()
	for i, v := range vals {
		if v == nil {
			// The record expired but its id is still in the index. Tidying it
			// away is opportunistic — the next pass would try again, and a
			// leftover id costs one nil in the MGet above — so a failure is
			// logged rather than failing a placement that is otherwise fine.
			if err := r.rdb.SRem(ctx, "gs:index", ids[i]).Err(); err != nil {
				log.Printf("cluster: prune stale index entry %s: %v", ids[i], err)
			}
			continue
		}
		var b []byte
		switch t := v.(type) {
		case string:
			b = []byte(t)
		case []byte:
			b = t
		default:
			continue
		}
		// Declared inside the loop, so each iteration unmarshals into a
		// message of its own and there is nothing for the clone this used to
		// make to defend against — it was a copy of a value with exactly one
		// reference, made once per server per placement, twenty times a second.
		var gs pb.GameServer
		if proto.Unmarshal(b, &gs) != nil {
			continue
		}
		if now-gs.UpdatedAt > 15 {
			continue
		}
		out = append(out, &gs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Id < out[j].Id })
	return out, nil
}

func inflightKey(id string) string { return "gs:inflight:" + id }

func (r *Redis) PickLeastLoaded(ctx context.Context) (*pb.GameServer, error) {
	list, err := r.ListServers(ctx)
	if err != nil || len(list) == 0 {
		return nil, err
	}
	// One MGet rather than a GET per server. Placement runs on the matchmaker's
	// 50ms ticker, so a round trip per candidate turns a fixed cost into one
	// that grows with the fleet — exactly backwards.
	keys := make([]string, len(list))
	for i, gs := range list {
		keys[i] = inflightKey(gs.Id)
	}
	inflight, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	if len(inflight) != len(list) {
		// Positional, so a reply of the wrong length cannot be read at all —
		// it would charge one node's reservations against another and place
		// onto whichever server happened to line up with an empty slot.
		return nil, fmt.Errorf("cluster: inflight reply has %d values for %d servers", len(inflight), len(list))
	}
	load := func(i int) int64 {
		n := int64(list[i].Rooms)
		// A missing key is a node with nothing reserved, which is the ordinary
		// case and not an error. A key holding something unparsable is not
		// something this package can write, so it is counted as zero rather
		// than failing the whole placement over one node's junk.
		if s, ok := inflight[i].(string); ok {
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				log.Printf("cluster: unparsable inflight for %s: %q", list[i].Id, s)
			} else {
				n += v
			}
		}
		return n
	}
	// list is already sorted by id, so the first server at the lowest load is
	// also the lowest id at that load: a strict < keeps the tie-break without
	// needing to compare ids again.
	bestIdx := -1
	var bestLoad int64
	for i := range list {
		l := load(i)
		if !accepting(list[i], int32(l)) {
			continue
		}
		if bestIdx < 0 || l < bestLoad {
			bestIdx, bestLoad = i, l
		}
	}
	if bestIdx < 0 {
		// Every node is draining or full. Saying so lets the caller put the
		// players back in the queue rather than sending them to a server that
		// has already said no.
		return nil, nil
	}
	// The reservation is the whole point of picking, so a failure to record it
	// is a failure to pick.
	//
	// It used to be discarded, which quietly removed the guarantee this counter
	// exists for: the node is handed back as chosen while the fleet's view of
	// its load does not move, so every other matchmaker replica in the same
	// window picks it too. The heartbeat corrects it two seconds later, which
	// is dozens of placements at this ticker's rate. Reporting it instead lets
	// createMatch put the players back in the queue.
	best := list[bestIdx]
	pipe := r.rdb.Pipeline()
	pipe.Incr(ctx, inflightKey(best.Id))
	pipe.Expire(ctx, inflightKey(best.Id), 8*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("cluster: reserve on %s: %w", best.Id, err)
	}
	return best, nil
}

func (r *Redis) Unregister(ctx context.Context, nodeID string) error {
	pipe := r.rdb.Pipeline()
	pipe.Del(ctx, "gs:"+nodeID)
	pipe.SRem(ctx, "gs:index", nodeID)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *Redis) ReleaseInflight(ctx context.Context, id string) error {
	n, err := r.rdb.Decr(ctx, inflightKey(id)).Result()
	if err != nil {
		return err
	}
	if n < 0 {
		// Released more often than taken. Flooring it matters more than the
		// decrement did: a node left at a negative reservation count reads as
		// the emptiest in the fleet and collects every match placed until its
		// key expires.
		if err := r.rdb.Set(ctx, inflightKey(id), 0, 8*time.Second).Err(); err != nil {
			return fmt.Errorf("cluster: floor inflight for %s: %w", id, err)
		}
	}
	return nil
}

func (r *Redis) RegisterRoom(ctx context.Context, info *pb.RoomInfo) error {
	b, err := proto.Marshal(info)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, "room:"+info.RoomId, b, 30*time.Minute).Err()
}

func (r *Redis) GetRoom(ctx context.Context, roomID string) (*pb.RoomInfo, error) {
	b, err := r.rdb.Get(ctx, "room:"+roomID).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var info pb.RoomInfo
	if err := proto.Unmarshal(b, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func (r *Redis) UnregisterRoom(ctx context.Context, roomID string) error {
	return r.rdb.Del(ctx, "room:"+roomID).Err()
}
