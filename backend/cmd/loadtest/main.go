package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/room"
)

// stats is what one run reports. Failures are counted by cause rather than
// totalled: every bot dials the same loopback address, so the gateway sees one
// client IP and its per-IP limiters apply to the whole fleet at once. Rolled up
// into a single "errors" number that looks like the server buckling under load,
// when it is the server doing exactly what it was configured to do.
type stats struct {
	connected atomic.Int64
	dialFail  atomic.Int64
	snapshots atomic.Int64
	matches   atomic.Int64
	// writeFail counts messages this generator failed to put on the wire.
	//
	// It is reported because a load generator that silently drops its own
	// traffic does not measure a smaller load, it measures the wrong one: the
	// bot stays counted in connections_ok, its inputs never reach the server,
	// and the run reports a server comfortably handling a load it was never
	// offered. Any number here at all means the throughput figures below are
	// understated by an unknown amount.
	writeFail atomic.Int64

	// Turn mode.
	updates atomic.Int64
	moves   atomic.Int64
	gaps    atomic.Int64
	resyncs atomic.Int64

	mu     sync.Mutex
	byCode map[string]int64
}

func (s *stats) serverError(code pb.ErrorCode) {
	s.mu.Lock()
	if s.byCode == nil {
		s.byCode = map[string]int64{}
	}
	s.byCode[code.String()]++
	s.mu.Unlock()
}

func (s *stats) errorSummary() (string, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.byCode) == 0 {
		return "none", 0
	}
	codes := make([]string, 0, len(s.byCode))
	var total int64
	for c := range s.byCode {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	parts := make([]string, 0, len(codes))
	for _, c := range codes {
		parts = append(parts, fmt.Sprintf("%s=%d", strings.TrimPrefix(c, "ERROR_CODE_"), s.byCode[c]))
		total += s.byCode[c]
	}
	return strings.Join(parts, " "), total
}

func main() {
	n := flag.Int("n", 1000, "connections")
	addr := flag.String("addr", "ws://localhost:8080/ws", "websocket url")
	ramp := flag.Duration("ramp", 5*time.Second, "ramp duration")
	dur := flag.Duration("dur", 30*time.Second, "hold duration after ramp")
	mode := flag.String("mode", "arena", "arena (tick loop + snapshots) or turn (event log + cursor)")
	// The session id prefix. It needs to be distinct per instance whenever two
	// of these run at once — pointing one at each gateway of a scaled-out
	// fleet, say, which is the interesting case for turn mode because the two
	// players in a match then sit on different nodes.
	//
	// Session ids are the server's identity for a player: HELLO carries one and
	// the gateway rekeys the connection to it, closing whatever was already
	// filed under that id. Two instances left on the same prefix therefore hand
	// every id to two connections, which spend the run evicting each other —
	// and that shows up in the results as cursor gaps and rejected moves, as if
	// the server were losing pushes, when what it is really doing is obeying two
	// clients that each claim to be the same player.
	prefix := flag.String("id", "lt", "session id prefix; must differ between concurrent instances")
	flag.Parse()

	if *mode != "arena" && *mode != "turn" {
		log.Fatalf("unknown -mode %q: want arena or turn", *mode)
	}

	st := &stats{}
	var wg sync.WaitGroup
	start := time.Now()
	interval := *ramp / time.Duration(*n)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}

	stop := time.Now().Add(*ramp + *dur)
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("%s-%d", *prefix, i)
			if *mode == "turn" {
				runTurnBot(*addr, id, st, stop)
				return
			}
			runBot(*addr, id, i, st, stop)
		}(i)
		time.Sleep(interval)
	}

	wg.Wait()
	elapsed := time.Since(start).Seconds()
	codes, rejected := st.errorSummary()

	if n := st.writeFail.Load(); n > 0 {
		fmt.Printf("WARNING: %d messages could not be sent; the figures below understate the offered load\n", n)
	}
	fmt.Printf("mode=%s connections_ok=%d dial_failed=%d server_rejected=%d [%s] elapsed=%.1fs\n",
		*mode, st.connected.Load(), st.dialFail.Load(), rejected, codes, elapsed)
	if *mode == "turn" {
		fmt.Printf("matches=%d moves=%d updates=%d updates_per_sec=%.0f gaps=%d full_resyncs=%d\n",
			st.matches.Load(), st.moves.Load(), st.updates.Load(),
			float64(st.updates.Load())/elapsed, st.gaps.Load(), st.resyncs.Load())
	} else {
		fmt.Printf("matches=%d snapshots=%d snapshots_per_sec=%.0f\n",
			st.matches.Load(), st.snapshots.Load(), float64(st.snapshots.Load())/elapsed)
	}

	// The trap this tool walks into by default, called out where it is seen
	// rather than left in a README nobody reads mid-run.
	if st.byCode["ERROR_CODE_RATE_LIMITED"] > 0 {
		fmt.Println("\nnote: every bot shares one client IP, so the gateway's per-IP limiters")
		fmt.Println("      cap the whole fleet. Run the server with HELLO_RATE_LIMIT=0")
		fmt.Println("      QUEUE_RATE_LIMIT=0 TURN_RATE_LIMIT=0 to load test past them.")
	}
}

type sock struct {
	mu sync.Mutex
	c  *websocket.Conn
	st *stats
}

// send writes a message and accounts for a failure. Every caller in this file
// goes through it rather than through write, so there is one place where a
// dropped message is counted and none where it is discarded.
//
// The bot carries on afterwards. A write that failed is usually a socket that
// has gone, and the read loop is about to notice and unwind; stopping here
// instead would race with that and lose the server's own error message, which
// is the more interesting half of a failed run.
func (s *sock) send(msg []byte) {
	if err := s.write(msg); err != nil {
		s.st.writeFail.Add(1)
	}
}

func (s *sock) set(c *websocket.Conn) {
	s.mu.Lock()
	old := s.c
	s.c = c
	s.mu.Unlock()
	if old != nil && old != c {
		// The socket being replaced is finished either way — this is a handoff
		// to another node — so a close that fails costs nothing but the
		// descriptor, and a run that leaks descriptors hits its own ulimit long
		// before it reaches the connection count it was asked for.
		if err := old.Close(); err != nil {
			log.Printf("loadtest: close replaced socket: %v", err)
		}
	}
}

func (s *sock) write(msg []byte) error {
	s.mu.Lock()
	c := s.c
	s.mu.Unlock()
	if c == nil {
		return fmt.Errorf("closed")
	}
	return c.WriteMessage(websocket.BinaryMessage, msg)
}

func (s *sock) read(deadline time.Time) ([]byte, error) {
	s.mu.Lock()
	c := s.c
	s.mu.Unlock()
	if c == nil {
		return nil, fmt.Errorf("closed")
	}
	// Without a deadline this read never returns and the bot is a goroutine
	// that stopped generating load without ever finishing, which the run would
	// report as a connection happily doing nothing.
	if err := c.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	_, data, err := c.ReadMessage()
	return data, err
}

func (s *sock) close() {
	s.set(nil)
}

func runBot(addr, id string, n int, st *stats, stop time.Time) {
	c, err := dial(addr, id)
	if err != nil {
		st.dialFail.Add(1)
		return
	}
	st.connected.Add(1)
	s := &sock{st: st}
	s.set(c)
	defer s.close()

	hello := protocol.Env(pb.MsgType_MSG_TYPE_HELLO, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Hello{Hello: &pb.Hello{
			Name: fmt.Sprintf("bot-%d", n), SessionId: id, ProtocolVersion: protocol.Version,
		}}
	})
	s.send(hello)
	s.send(protocol.Env(pb.MsgType_MSG_TYPE_JOIN_QUEUE, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_JoinQueue{JoinQueue: &pb.JoinQueue{}}
	}))

	var inMatch atomic.Bool
	// lastTick is telemetry (where this client got to). ackTick is the delta
	// baseline and must only ever name a tick we still hold in history.
	var lastTick atomic.Uint32
	var ackTick atomic.Uint32

	go func() {
		t := time.NewTicker(time.Second / 20)
		defer t.Stop()
		seq := uint32(0)
		for time.Now().Before(stop) {
			<-t.C
			if !inMatch.Load() {
				continue
			}
			seq++
			s.send(protocol.Env(pb.MsgType_MSG_TYPE_INPUT, func(e *pb.Envelope) {
				e.Payload = &pb.Envelope_Input{Input: &pb.Input{
					Seq: seq, Mx: int32(seq%3) - 1, My: int32((seq/2)%3) - 1,
					Fire: seq%6 == 0, Aim: int32((seq * 17) % 360),
					// Ack only what we actually applied — see the snapshot case.
					AckTick: ackTick.Load(),
					// InterpMs stays zero, and that is the honest answer rather
					// than an oversight: this bot draws nothing, so it holds no
					// interpolation buffer and aims at the newest tick it has.
					// Copying the browser's 100 ms here would claim a handicap
					// it does not take and collect compensation for it.
				}}
			}))
		}
	}()

	// Snapshot history, same idea as the server ring: a delta names the
	// baseline it was built from, and we can only apply it if we still hold
	// that tick.
	history := make(map[uint32]*pb.Snapshot, room.SnapshotHistory)

	for time.Now().Before(stop) {
		data, err := s.read(stop.Add(2 * time.Second))
		if err != nil {
			return
		}
		e, err := protocol.UnmarshalEnv(data)
		if err != nil {
			continue
		}
		switch e.Type {
		case pb.MsgType_MSG_TYPE_SNAPSHOT:
			st.snapshots.Add(1)
			snap := e.GetSnapshot()
			if snap == nil {
				break
			}
			applied := room.ApplyDelta(history[snap.BaselineTick], snap)
			if applied == nil {
				// Baseline we no longer hold. Keep acking the last good tick;
				// the server keeps encoding from there until an ack moves it.
				break
			}
			history[applied.Tick] = applied
			for t := range history {
				if t+room.SnapshotHistory <= applied.Tick {
					delete(history, t)
				}
			}
			if applied.Tick > lastTick.Load() {
				lastTick.Store(applied.Tick)
				ackTick.Store(applied.Tick)
			}
		case pb.MsgType_MSG_TYPE_MATCH_FOUND, pb.MsgType_MSG_TYPE_REDIRECT:
			host, roomID, yourID := assignment(e)
			st.matches.Add(1)
			// New connection means a new signon state: the server restarts us
			// from a full snapshot, so the old history is worthless.
			clear(history)
			ackTick.Store(0)
			if host != "" {
				nc, err := dial(host, id)
				if err != nil {
					st.dialFail.Add(1)
					return
				}
				s.set(nc)
				s.send(hello)
				s.send(protocol.Env(pb.MsgType_MSG_TYPE_JOIN_ROOM, func(e *pb.Envelope) {
					e.Payload = &pb.Envelope_JoinRoom{JoinRoom: &pb.JoinRoom{
						RoomId: roomID, YourId: yourID, LastAckTick: lastTick.Load(), SessionId: id,
					}}
				}))
			}
			inMatch.Store(true)
		case pb.MsgType_MSG_TYPE_ERROR:
			st.serverError(e.GetError().GetCode())
			return
		}
	}
}

func assignment(e *pb.Envelope) (host, room string, yourID uint32) {
	if mf := e.GetMatchFound(); mf != nil {
		return mf.Host, mf.RoomId, mf.YourId
	}
	if rd := e.GetRedirect(); rd != nil {
		return rd.Host, rd.RoomId, rd.YourId
	}
	return "", "", 0
}

// dialer resolves each host once and then connects straight to the address.
//
// Resolving per connection is what stops this tool reaching the scale it
// advertises. On macOS the default resolver is cgo, and a cgo call blocks a
// whole OS thread for its duration — ramping ten thousand bots asks for ten
// thousand of them at once, the kernel refuses somewhere under its per-task
// cap, and the process aborts in pthread_create long before the server is
// under any real load. One lookup per host, cached, removes the resolver from
// the hot path entirely.
var dialer = struct {
	mu   sync.Mutex
	seen map[string]string
	ws   *websocket.Dialer
}{seen: map[string]string{}}

func init() {
	dialer.ws = &websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
		NetDial: func(network, address string) (net.Conn, error) {
			return net.DialTimeout(network, resolveOnce(address), 15*time.Second)
		},
	}
}

func resolveOnce(address string) string {
	dialer.mu.Lock()
	got, ok := dialer.seen[address]
	dialer.mu.Unlock()
	if ok {
		return got
	}
	out := address
	if host, port, err := net.SplitHostPort(address); err == nil {
		if ips, err := net.LookupIP(host); err == nil && len(ips) > 0 {
			out = net.JoinHostPort(ips[0].String(), port)
		}
	}
	dialer.mu.Lock()
	dialer.seen[address] = out
	dialer.mu.Unlock()
	return out
}

func dial(addr, id string) (*websocket.Conn, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("id", id)
	u.RawQuery = q.Encode()
	c, _, err := dialer.ws.Dial(u.String(), nil)
	return c, err
}

func init() { log.SetFlags(0) }
