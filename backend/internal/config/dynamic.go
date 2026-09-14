package config

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

const dynKey = "arena:dynconfig"
const dynCh = "arena:dynconfig:ch"

// MaxRoomSize is the largest room this server will form.
//
// The ceiling is not a preference, it is where the wire stops working: past
// roughly 28 players a full snapshot outgrows udp.MaxDatagram, and the UDP
// transport drops oversized datagrams rather than relying on IP fragmentation.
// internal/room's snapshot-size tests pin the measured threshold; this clamp is
// what stops a hot config push from walking past it at runtime, where no test
// is watching. Raising it is an interest-management project, not a number edit.
const MaxRoomSize = 24

// MaxDisconnectGrace is the longest reconnect window that can actually work.
//
// The window is enforced against a presence record, and that record expires:
// past this, the record is gone before the grace is up and the player is
// greeted as a new arrival however generous the number says it is. A knob that
// silently stops working above a threshold is worse than one that is clamped
// and says so.
//
// It is presence.DisconnectTTL, written out rather than imported for the reason
// InputBuffer gives about room.DefaultInputBuffer: a package of knobs should
// not depend on the packages it configures. TestDisconnectTTLCoversTheConfiguredGrace
// in internal/presence holds the two together, so this cannot quietly drift.
const MaxDisconnectGrace = 2 * time.Minute

// View is the hot-reloadable half of the configuration.
type View struct {
	TickRate        int
	RoomSize        int
	MinPlayers      int
	QueueTimeout    time.Duration
	MatchSeconds    int
	DisconnectGrace time.Duration
	SendBuffer      int
	MaxCCU          int

	// SkillWindow / SkillWiden / SkillMaxWindow are the matchmaker's quality
	// knobs. They live here rather than in the static config because the right
	// width depends on how many people are queueing at this moment — which is
	// the one thing a build cannot know. Zero SkillWindow is pure FIFO.
	SkillWindow    int
	SkillWiden     int
	SkillMaxWindow int
}

func (v View) ToPB() *pb.DynamicConfig {
	return &pb.DynamicConfig{
		TickRate:          int32(v.TickRate),
		RoomSize:          int32(v.RoomSize),
		MinPlayers:        int32(v.MinPlayers),
		QueueTimeoutMs:    v.QueueTimeout.Milliseconds(),
		MatchSeconds:      int32(v.MatchSeconds),
		DisconnectGraceMs: v.DisconnectGrace.Milliseconds(),
		SendBuffer:        int32(v.SendBuffer),
		MaxCcu:            offOrValue(v.MaxCCU),
		SkillWindow:       offOrValue(v.SkillWindow),
		SkillWiden:        offOrValue(v.SkillWiden),
		SkillMaxWindow:    offOrValue(v.SkillMaxWindow),
	}
}

// offOrValue encodes a switched-off knob as -1 rather than 0.
//
// Every field in DynamicConfig reads 0 as "not specified, keep what the
// environment said" — which is fine for a tick rate, where zero is not a
// setting anyone wants, and wrong for the four fields that use this, where zero
// is a real choice: no skill window is pure FIFO, no widening is a fixed
// window, no cap is unbounded growth, and MaxCCU of zero is the documented way
// to say a gateway has no connection ceiling (App.onHello only enforces one
// when it is positive). Without this, GET /admin/config followed by PUT of the
// same document would quietly re-enable whatever the environment had, and the
// config would not survive its own round trip.
func offOrValue(v int) int32 {
	if v == 0 {
		return -1
	}
	return int32(v)
}

func ViewFromPB(d *pb.DynamicConfig) View {
	v := LoadViewFromEnv()
	if d == nil {
		return v
	}
	if d.TickRate == 20 || d.TickRate == 30 || d.TickRate == 60 {
		v.TickRate = int(d.TickRate)
	}
	if d.RoomSize > 0 {
		v.RoomSize = int(d.RoomSize)
	}
	if d.MinPlayers > 0 {
		v.MinPlayers = int(d.MinPlayers)
	}
	if d.QueueTimeoutMs > 0 {
		v.QueueTimeout = time.Duration(d.QueueTimeoutMs) * time.Millisecond
	}
	if d.MatchSeconds > 0 {
		v.MatchSeconds = int(d.MatchSeconds)
	}
	if d.DisconnectGraceMs > 0 {
		v.DisconnectGrace = time.Duration(d.DisconnectGraceMs) * time.Millisecond
	}
	if d.SendBuffer > 0 {
		v.SendBuffer = int(d.SendBuffer)
	}
	// Negative is the explicit "no ceiling", the same encoding the skill knobs
	// below use, and it lands as the zero App.onHello reads as unlimited.
	if d.MaxCcu != 0 {
		v.MaxCCU = max(0, int(d.MaxCcu))
	}
	// Zero means "not specified" for every field above, which leaves no way to
	// switch skill matching off from a config push. A negative window says so
	// explicitly and lands as the zero the matchmaker reads as FIFO.
	if d.SkillWindow != 0 {
		v.SkillWindow = max(0, int(d.SkillWindow))
	}
	if d.SkillWiden != 0 {
		v.SkillWiden = max(0, int(d.SkillWiden))
	}
	if d.SkillMaxWindow != 0 {
		v.SkillMaxWindow = max(0, int(d.SkillMaxWindow))
	}
	v.clamp()
	return v
}

// clamp is the single place a View is made safe to run with, so a value that
// arrives over the hot-reload channel is held to the same bounds as one that
// came from the environment.
func (v *View) clamp() {
	if v.MinPlayers < 1 {
		v.MinPlayers = 1
	}
	if v.RoomSize > MaxRoomSize {
		log.Printf("config: room_size %d exceeds MaxRoomSize %d (snapshots would outgrow one datagram); clamped",
			v.RoomSize, MaxRoomSize)
		v.RoomSize = MaxRoomSize
	}
	if v.MinPlayers > MaxRoomSize {
		v.MinPlayers = MaxRoomSize
	}
	if v.RoomSize < v.MinPlayers {
		v.RoomSize = v.MinPlayers
	}
	if v.DisconnectGrace > MaxDisconnectGrace {
		log.Printf("config: disconnect_grace %s exceeds %s (the presence record expires first, so the extra window does nothing); clamped",
			v.DisconnectGrace, MaxDisconnectGrace)
		v.DisconnectGrace = MaxDisconnectGrace
	}
	if v.SendBuffer < 4 {
		v.SendBuffer = 4
	}
	if v.MaxCCU < 0 {
		v.MaxCCU = 0
	}
	if v.SkillWindow < 0 {
		v.SkillWindow = 0
	}
	if v.SkillWiden < 0 {
		v.SkillWiden = 0
	}
	if v.SkillMaxWindow < 0 {
		v.SkillMaxWindow = 0
	}
	if v.SkillMaxWindow > 0 && v.SkillMaxWindow < v.SkillWindow {
		// A cap below the starting width would make the window shrink the
		// moment anyone waited, which is the opposite of what it is for.
		v.SkillMaxWindow = v.SkillWindow
	}
}

// Live holds the hot-reloadable View. Readers pull with Get rather than being
// called back on change: every consumer needs the current value at a moment it
// chooses — the tick a room starts, the connection it accepts — and none of
// them can act on a change at the instant it lands.
type Live struct {
	cur atomic.Pointer[View]
	// rdb is atomic because it can be installed after construction — see
	// UseRedis — and is read from the watch loop and from every Update.
	rdb atomic.Pointer[redis.Client]
}

func NewLive(initial View, rdb *redis.Client) *Live {
	l := &Live{}
	v := initial
	l.cur.Store(&v)
	if rdb != nil {
		l.UseRedis(context.Background(), rdb)
	}
	return l
}

// UseRedis points a Live that was built without one at a client, and pulls
// whatever the fleet already agreed on.
//
// It exists so the process can hold a single Redis client. main used to build
// one purely to hand to NewLive, and app.New then built a second for the queue,
// the registry and the rest — two connection pools to the same server, one of
// them never closed. The dependency is really the app's, so the app is what
// supplies it, and main no longer needs to know whether Redis is configured at
// all.
//
// The context bounds the initial read. A config server that is not answering
// should delay a startup, not hold it forever.
func (l *Live) UseRedis(ctx context.Context, rdb *redis.Client) {
	if l == nil || rdb == nil {
		return
	}
	l.rdb.Store(rdb)
	if remote, err := loadRedis(ctx, rdb); err == nil && remote != nil {
		got := ViewFromPB(remote)
		l.cur.Store(&got)
	}
}

func (l *Live) Get() View {
	if p := l.cur.Load(); p != nil {
		return *p
	}
	return LoadViewFromEnv()
}

func (l *Live) Update(ctx context.Context, v View) error {
	v = ViewFromPB(v.ToPB())
	l.cur.Store(&v)
	rdb := l.rdb.Load()
	if rdb != nil {
		b, err := proto.Marshal(v.ToPB())
		if err != nil {
			return err
		}
		if err := rdb.Set(ctx, dynKey, b, 0).Err(); err != nil {
			return err
		}
		// The publish is the fast path, not the durable one: the value is
		// already stored, and Watch re-reads dynKey every three seconds
		// precisely so a missed notification costs latency rather than
		// correctness. So this does not fail the update — a caller told its
		// config push failed would try again and rewrite a value that is
		// already right — but a fleet that has silently stopped hearing pushes
		// takes three seconds per change instead of none, and that is worth a
		// line.
		if err := rdb.Publish(ctx, dynCh, b).Err(); err != nil {
			log.Printf("dynconfig: publish: %v (subscribers will pick it up on their next poll)", err)
		}
	}
	return nil
}

func (l *Live) Watch(ctx context.Context) {
	rdb := l.rdb.Load()
	if rdb == nil {
		return
	}
	sub := rdb.Subscribe(ctx, dynCh)
	ch := sub.Channel()
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()
	defer sub.Close()
	apply := func(b []byte) {
		var d pb.DynamicConfig
		if proto.Unmarshal(b, &d) != nil {
			return
		}
		v := ViewFromPB(&d)
		l.cur.Store(&v)
		log.Printf("dynconfig tick=%d room=%d min=%d queue=%s", v.TickRate, v.RoomSize, v.MinPlayers, v.QueueTimeout)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			apply([]byte(msg.Payload))
		case <-tick.C:
			b, err := rdb.Get(ctx, dynKey).Bytes()
			if err == nil {
				apply(b)
			}
		}
	}
}

func loadRedis(ctx context.Context, rdb *redis.Client) (*pb.DynamicConfig, error) {
	b, err := rdb.Get(ctx, dynKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var d pb.DynamicConfig
	if err := proto.Unmarshal(b, &d); err != nil {
		return nil, err
	}
	return &d, nil
}
