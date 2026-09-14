// Package udp carries the same protobuf envelopes as the WS and TCP
// transports, but over datagrams — the transport a shooter actually wants.
//
// Three things change once you leave TCP, and all three show up below:
//
//   - No framing. A datagram has a boundary, so the length prefix the TCP
//     transport needs disappears: one datagram is exactly one message.
//   - No connection. There is no accept, no FIN, no RST. This package keeps its
//     own peer table keyed by remote address and evicts on silence, because
//     nothing else will ever tell it a client left.
//   - No delivery guarantee, which is the point. A snapshot that did not arrive
//     is worthless anyway; the delta baseline only advances on ack, so loss
//     costs bandwidth on the next tick and nothing else.
package udp

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/safe"
	"github.com/nguyenbatam/arena_game_server/internal/session"
)

// MaxDatagram stays under the usual 1500-byte Ethernet MTU with room for IP and
// UDP headers, so a message never relies on IP fragmentation — a fragmented
// datagram is lost entirely if any fragment is, which multiplies the loss rate.
const MaxDatagram = 1200

// DefaultIdle is how long a peer may stay silent before its seat is released.
// Clients send input at the tick rate, so silence really does mean gone.
const DefaultIdle = 30 * time.Second

// peerInbox is how many datagrams one peer may have waiting for its handler.
//
// It only ever fills when that peer's handler is slower than its send rate, and
// at the tick rate this depth is over a second of slack. Deeper would not help:
// a peer that far behind is not going to catch up, and the datagrams already
// queued describe a world that has moved on.
const peerInbox = 32

// CookieLifetime bounds how long an address proof stays usable. Short enough
// that a stolen cookie is worthless, long enough to survive a slow handshake.
const CookieLifetime = 30 * time.Second

// cookieLen is the reply to an unverified HELLO: a token bound to the sender's
// address.
const cookieLen = 4 + sha256.Size

// MinInitial is the smallest first datagram the server will answer.
//
// The cookie reply is 36 bytes; a bare HELLO is about 13. Answering one with
// the other is a ~3x amplifier — modest, but an attacker forging a victim's
// address gets it for free, and there is no reason to hand it over. Requiring
// the unverified HELLO to be padded first makes the reply strictly smaller than
// the request. QUIC pads its Initial packets to 1200 bytes for this exact
// reason; the number here only has to exceed cookieLen.
const MinInitial = 512

// Cookies prove a sender can actually receive at the address it claims.
//
// UDP has no handshake, so anyone can put a victim's address in the source
// field and the server would happily stream snapshots at them — a reflector,
// pointed by the attacker. Before committing any state the server replies once
// with a cookie derived from the address; only a sender that genuinely receives
// at that address can echo it back. This is what DTLS calls
// HelloVerifyRequest and QUIC calls Retry.
type cookies struct {
	key []byte
	now func() time.Time
}

func newCookies() *cookies {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		// A predictable key defeats the whole mechanism, so refuse to run with
		// one rather than quietly downgrade.
		panic("udp: cannot seed cookie key: " + err.Error())
	}
	return &cookies{key: key, now: time.Now}
}

// issue mints a cookie for addr, valid until it ages past CookieLifetime. The
// server keeps no per-address state, so a flood of spoofed HELLOs costs it
// nothing but the reply.
func (c *cookies) issue(addr string) []byte {
	out := make([]byte, cookieLen)
	binary.BigEndian.PutUint32(out[:4], uint32(c.now().Unix()))
	mac := hmac.New(sha256.New, c.key)
	mac.Write(out[:4])
	mac.Write([]byte(addr))
	copy(out[4:], mac.Sum(nil))
	return out
}

func (c *cookies) valid(addr string, token []byte) bool {
	if len(token) != cookieLen {
		return false
	}
	issued := int64(binary.BigEndian.Uint32(token[:4]))
	age := c.now().Unix() - issued
	if age < 0 || age > int64(CookieLifetime.Seconds()) {
		return false
	}
	mac := hmac.New(sha256.New, c.key)
	mac.Write(token[:4])
	mac.Write([]byte(addr))
	return hmac.Equal(token[4:], mac.Sum(nil))
}

// Handler is called with the bytes of one datagram.
//
// msg is only valid for the duration of the call: it is a pooled buffer and is
// handed back the moment this returns. A handler that wants to keep the bytes
// must copy them. Unmarshalling is not keeping them — protobuf copies every
// string and bytes field out on the way in, which is what makes the pooling
// safe and is pinned by TestUnmarshalDoesNotAliasTheReadBuffer over in the TCP
// transport, which relies on the same property.
//
// This is the same contract tcp.Handler states, for the same reason.
type Handler func(c *session.Conn, msg []byte)

// dgrams lends out per-datagram buffers.
//
// Every datagram used to be copied into an allocation of its own — one per
// message, which at the tick rate and ten thousand players is 200k a second of
// garbage whose whole life is to be carried to a peer's goroutine and
// unmarshalled. The read loop cannot simply hand out its own buffer: the
// handler runs on another goroutine and the next ReadFrom would overwrite the
// bytes underneath it.
//
// A sync.Pool rather than a free list because it is already per-P, so the read
// loop takes a buffer without contending with the peer goroutines giving them
// back, and the GC reclaims the pool under memory pressure instead of holding
// the high-water mark for the life of the process.
//
// A buffer that is dropped rather than returned — one left in a dead peer's
// inbox — is simply collected. A pool is a cache, not an allocator, so losing
// one costs nothing but the next allocation.
var dgrams = sync.Pool{New: func() any {
	b := make([]byte, MaxDatagram)
	return &b
}}

func getDgram() []byte { return (*dgrams.Get().(*[]byte))[:MaxDatagram] }

// putDgram returns a buffer. Buffers that did not come from the pool are
// dropped rather than admitted, so a short one can never be handed out as if it
// were full-sized.
func putDgram(b []byte) {
	if cap(b) != MaxDatagram {
		return
	}
	full := b[:MaxDatagram]
	dgrams.Put(&full)
}

type peer struct {
	conn *session.Conn
	addr net.Addr
	last atomic.Int64 // unix nano of the most recent datagram

	// in hands this peer's datagrams to its own goroutine. The socket has one
	// receive queue shared by every client, so handling a datagram on the read
	// loop makes the slowest handler the arrival rate for everyone: one HELLO
	// doing its presence lookups stalls the loop, the kernel buffer fills, and
	// datagrams are dropped for peers that did nothing wrong. Per-peer delivery
	// keeps the read loop doing nothing but reading, and still serialises each
	// peer against itself so its own messages cannot overtake each other.
	in   chan []byte
	dead chan struct{}
	gone atomic.Bool
}

type server struct {
	pc      net.PacketConn
	hub     *session.Hub
	sendBuf session.BufSize
	onMsg   Handler
	onClose func(*session.Conn)

	mu    sync.Mutex
	peers map[string]*peer

	cookies *cookies

	oversize atomic.Uint64
	backlog  atomic.Uint64
	// cookieFails counts address-proof replies that could not be sent. See
	// dispatch.
	cookieFails atomic.Uint64
	done        chan struct{}
}

// Serve reads datagrams until pc is closed.
func Serve(pc net.PacketConn, hub *session.Hub, sendBuf session.BufSize, onMsg Handler, onClose func(*session.Conn), idle time.Duration) {
	if idle <= 0 {
		idle = DefaultIdle
	}
	s := &server{
		pc: pc, hub: hub, sendBuf: sendBuf, onMsg: onMsg, onClose: onClose,
		peers:   make(map[string]*peer),
		cookies: newCookies(),
		done:    make(chan struct{}),
	}
	safe.Go("udp.sweep", func() { s.sweep(idle) })
	defer close(s.done)

	buf := make([]byte, MaxDatagram)
	for {
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			s.shutdown()
			return
		}
		if n == 0 {
			continue
		}
		// ReadFrom reuses buf, and the handler runs on another goroutine and
		// may outlive this iteration, so the bytes are copied out. The copy
		// goes into a pooled buffer rather than a fresh allocation — see
		// dgrams — and the peer goroutine hands it back once the handler
		// returns.
		msg := getDgram()[:n]
		copy(msg, buf[:n])
		s.dispatch(addr, msg)
	}
}

func (s *server) dispatch(addr net.Addr, msg []byte) {
	key := addr.String()

	s.mu.Lock()
	p, known := s.peers[key]
	s.mu.Unlock()

	if !known {
		// An unknown source address is unverified: it may be spoofed. Nothing
		// but a HELLO is even looked at, and a HELLO alone does not open a
		// session — it has to carry a cookie proving the sender receives at the
		// address it claims.
		//
		// Every path that turns the datagram away hands its buffer back: this
		// is the half of the traffic a flood would consist of, so it is the
		// half that must not leak buffers out of the pool.
		env, err := protocol.UnmarshalEnv(msg)
		if err != nil || env.Type != pb.MsgType_MSG_TYPE_HELLO {
			putDgram(msg)
			return
		}
		token := env.GetHello().GetAddrToken()
		if !s.cookies.valid(key, token) {
			if len(msg) < MinInitial {
				// Too small to answer without amplifying. Silence costs the
				// spoofer everything and us nothing.
				putDgram(msg)
				return
			}
			// Answer once with a fresh cookie and keep no state. A spoofed
			// address gets this single reply — smaller than what it sent — and
			// nothing more; the real owner of the address echoes it back.
			//
			// Counted rather than discarded. A cookie that never goes out is a
			// client that can never open a session, and because the server
			// keeps no state for an unverified sender there is nothing else
			// anywhere that would record the attempt: the symptom is players
			// who simply cannot connect over UDP, with every other counter in
			// the process looking healthy. Logged from the sweeper so a flood
			// of failures costs one line rather than one per datagram.
			if _, err := s.pc.WriteTo(s.cookies.issue(key), addr); err != nil {
				s.cookieFails.Add(1)
			}
			putDgram(msg)
			return
		}
		p = s.open(addr, key)
	}

	// Stamped here, on arrival, rather than when the handler gets to it: idle
	// eviction is about whether the client is still sending, not about how
	// quickly we processed what it sent.
	p.last.Store(time.Now().UnixNano())
	s.deliver(p, msg)
}

// deliver queues a datagram for its peer's handler goroutine.
//
// A full inbox means that peer is not keeping up, and the right answer on UDP
// is the same one the transport already gives: drop it. Blocking here would
// re-introduce exactly the head-of-line stall the inbox exists to remove.
func (s *server) deliver(p *peer, msg []byte) {
	select {
	case <-p.dead:
		putDgram(msg)
		return
	default:
	}
	select {
	case p.in <- msg:
	default:
		s.backlog.Add(1)
		putDgram(msg)
	}
}

func (s *server) readLoop(p *peer) {
	for {
		select {
		case <-p.dead:
			return
		case msg := <-p.in:
			// A select with both cases ready picks at random, so waking on a
			// queued datagram says nothing about whether the peer is still
			// alive. Check before handling: onClose has already run by then,
			// and a late HELLO would write a fresh presence record for a
			// session that no longer exists — leaving the player marked online
			// on a node they are not connected to.
			if p.gone.Load() {
				putDgram(msg)
				return
			}
			s.onMsg(p.conn, msg)
			// The handler is not allowed to keep the bytes — see Handler — so
			// the buffer goes back the moment it returns.
			putDgram(msg)
		}
	}
}

func (s *server) open(addr net.Addr, key string) *peer {
	c := session.NewConn(session.NewID(), s.sendBuf())
	if host, _, err := net.SplitHostPort(key); err == nil {
		c.RemoteIP = host
	}
	p := &peer{
		conn: c, addr: addr,
		in:   make(chan []byte, peerInbox),
		dead: make(chan struct{}),
	}
	p.last.Store(time.Now().UnixNano())

	s.mu.Lock()
	s.peers[key] = p
	s.mu.Unlock()

	s.hub.Add(c)
	safe.Go("udp.peer_write", func() { s.writeLoop(p) })
	safe.Go("udp.peer_read", func() { s.readLoop(p) })
	return p
}

func (s *server) writeLoop(p *peer) {
	for msg := range p.conn.Outbox() {
		if len(msg) > MaxDatagram {
			// Better to drop one snapshot than to rely on IP fragmentation.
			// If this ever fires in practice the fix is upstream: shrink the
			// snapshot (interest management), not raise the limit.
			s.oversize.Add(1)
			continue
		}
		if _, err := s.pc.WriteTo(msg, p.addr); err != nil {
			return
		}
	}
}

// sweep evicts peers that have gone quiet. UDP has no close notification, so
// without this the room would hold their seats forever.
func (s *server) sweep(idle time.Duration) {
	t := time.NewTicker(idle / 3)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			cutoff := time.Now().Add(-idle).UnixNano()
			s.mu.Lock()
			var stale []*peer
			for key, p := range s.peers {
				if p.last.Load() < cutoff {
					stale = append(stale, p)
					delete(s.peers, key)
				}
			}
			s.mu.Unlock()
			for _, p := range stale {
				s.drop(p)
			}
			if n := s.oversize.Swap(0); n > 0 {
				log.Printf("udp: dropped %d datagrams over %dB", n, MaxDatagram)
			}
			if n := s.backlog.Swap(0); n > 0 {
				log.Printf("udp: dropped %d datagrams from peers with a full inbox", n)
			}
			if n := s.cookieFails.Swap(0); n > 0 {
				log.Printf("udp: failed to send %d address-proof cookies; those clients cannot open a session", n)
			}
		}
	}
}

func (s *server) shutdown() {
	s.mu.Lock()
	peers := make([]*peer, 0, len(s.peers))
	for key, p := range s.peers {
		peers = append(peers, p)
		delete(s.peers, key)
	}
	s.mu.Unlock()
	for _, p := range peers {
		s.drop(p)
	}
}

// drop releases a peer exactly once. The sweeper and shutdown both remove from
// the map under the lock before calling here, so a double drop should not
// happen — but closing dead twice would panic, and a panic in the sweeper takes
// down every other peer with it.
func (s *server) drop(p *peer) {
	if !p.gone.CompareAndSwap(false, true) {
		return
	}
	close(p.dead)
	s.hub.Remove(p.conn)
	s.onClose(p.conn)
	p.conn.Close()
}
