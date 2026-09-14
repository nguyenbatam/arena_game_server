package udp

import (
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/session"
)

type recorder struct {
	mu     sync.Mutex
	msgs   [][]byte
	closed int
	conns  []*session.Conn
}

// onMsg copies, because Handler says msg is only valid for the duration of the
// call — the buffer goes back to the pool the moment this returns. A recorder
// that kept the slice would be reading whatever datagram landed in that buffer
// next, which is the bug this copy exists to not have in the test harness.
func (r *recorder) onMsg(c *session.Conn, msg []byte) {
	kept := append([]byte(nil), msg...)
	r.mu.Lock()
	r.msgs = append(r.msgs, kept)
	r.conns = append(r.conns, c)
	r.mu.Unlock()
}

func (r *recorder) onClose(*session.Conn) {
	r.mu.Lock()
	r.closed++
	r.mu.Unlock()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

func (r *recorder) closes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func (r *recorder) conn(i int) *session.Conn {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.conns[i]
}

func startServer(t *testing.T, idle time.Duration) (*net.UDPAddr, *recorder) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	go Serve(pc, session.NewHub(), session.FixedBuf(16), rec.onMsg, rec.onClose, idle)
	t.Cleanup(func() { _ = pc.Close() })
	return pc.LocalAddr().(*net.UDPAddr), rec
}

// hello builds a HELLO padded up to MinInitial, the way a real UDP client must
// send its first datagram.
func hello(token []byte) []byte {
	msg := protocol.Env(pb.MsgType_MSG_TYPE_HELLO, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Hello{Hello: &pb.Hello{
			Name: "udp", SessionId: "s1", AddrToken: token,
		}}
	})
	if len(msg) >= MinInitial {
		return msg
	}
	return protocol.Env(pb.MsgType_MSG_TYPE_HELLO, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Hello{Hello: &pb.Hello{
			Name: "udp", SessionId: "s1", AddrToken: token,
			Padding: make([]byte, MinInitial-len(msg)),
		}}
	})
}

// helloUnpadded is what an amplification attacker would send: as few bytes as
// possible, hoping for a bigger reply.
func helloUnpadded(token []byte) []byte {
	return protocol.Env(pb.MsgType_MSG_TYPE_HELLO, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Hello{Hello: &pb.Hello{SessionId: "s1", AddrToken: token}}
	})
}

// handshake performs the two-step address proof a real client does: send a
// bare HELLO, get a cookie back, echo it.
func handshake(t *testing.T, c *net.UDPConn) {
	t.Helper()
	if _, err := c.Write(hello(nil)); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("no cookie came back: %v", err)
	}
	if n != cookieLen {
		t.Fatalf("cookie is %d bytes, want %d", n, cookieLen)
	}
	if _, err := c.Write(hello(buf[:n])); err != nil {
		t.Fatal(err)
	}
}

func dial(t *testing.T, addr *net.UDPAddr) *net.UDPConn {
	t.Helper()
	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestHelloOpensSessionAndCarriesMessages(t *testing.T) {
	addr, rec := startServer(t, time.Minute)
	c := dial(t, addr)

	handshake(t, c)
	waitFor(t, "hello to be delivered", func() bool { return rec.count() >= 1 })

	// A datagram is already framed, so an input needs no length prefix.
	input := protocol.Env(pb.MsgType_MSG_TYPE_INPUT, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Input{Input: &pb.Input{Seq: 7, Mx: 1, AckTick: 3}}
	})
	if _, err := c.Write(input); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "input to be delivered", func() bool { return rec.count() >= 2 })

	env, err := protocol.UnmarshalEnv(rec.msgs[1])
	if err != nil {
		t.Fatalf("second datagram did not decode: %v", err)
	}
	if got := env.GetInput(); got == nil || got.Seq != 7 || got.AckTick != 3 {
		t.Fatalf("input round-trip wrong: %v", got)
	}
	if rec.conn(0) != rec.conn(1) {
		t.Error("datagrams from one address must map to one session")
	}
}

// An unverified source address must not be able to make the server talk: that
// is what turns a game server into a reflector for someone else's traffic.
func TestUnknownAddressCannotOpenSessionWithoutHello(t *testing.T) {
	addr, rec := startServer(t, time.Minute)
	c := dial(t, addr)

	notHello := protocol.Env(pb.MsgType_MSG_TYPE_INPUT, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Input{Input: &pb.Input{Seq: 1}}
	})
	if _, err := c.Write(notHello); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("garbage that is not protobuf at all")); err != nil {
		t.Fatal(err)
	}

	time.Sleep(150 * time.Millisecond)
	if n := rec.count(); n != 0 {
		t.Fatalf("server accepted %d datagrams from an address that never said hello", n)
	}
}

// UDP never reports a disconnect, so silence is the only signal there is.
func TestSilentPeerIsEvicted(t *testing.T) {
	addr, rec := startServer(t, 120*time.Millisecond)
	c := dial(t, addr)

	handshake(t, c)
	waitFor(t, "session to open", func() bool { return rec.count() >= 1 })
	waitFor(t, "silent peer to be swept", func() bool { return rec.closes() >= 1 })
}

// Oversized payloads are dropped rather than handed to IP fragmentation, where
// losing any one fragment loses the whole datagram.
func TestOversizedSendIsDroppedNotFragmented(t *testing.T) {
	addr, rec := startServer(t, time.Minute)
	c := dial(t, addr)
	handshake(t, c)
	waitFor(t, "session to open", func() bool { return rec.count() >= 1 })

	conn := rec.conn(0)
	conn.Send(make([]byte, MaxDatagram+1))
	conn.Send([]byte("small one gets through"))

	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 2048)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("expected the small datagram to arrive: %v", err)
	}
	if string(buf[:n]) != "small one gets through" {
		t.Fatalf("got %q, want the small datagram — the oversized one should have been dropped", buf[:n])
	}
}

// A HELLO without a valid cookie must not open a session. It gets exactly one
// small reply and nothing else — so an attacker who forges a victim's source
// address cannot turn this server into a firehose aimed at them.
func TestHelloWithoutCookieDoesNotOpenSession(t *testing.T) {
	addr, rec := startServer(t, time.Minute)
	c := dial(t, addr)

	sent := 0
	for i := 0; i < 5; i++ {
		msg := hello(nil)
		if _, err := c.Write(msg); err != nil {
			t.Fatal(err)
		}
		sent += len(msg)
	}
	time.Sleep(200 * time.Millisecond)

	if n := rec.count(); n != 0 {
		t.Fatalf("%d datagrams reached the app from an unverified address", n)
	}

	// Each unverified HELLO gets a cookie and nothing else, and the total sent
	// back must not exceed what came in.
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 2048)
	got := 0
	for {
		n, err := c.Read(buf)
		if err != nil {
			break
		}
		got += n
		if n != cookieLen {
			t.Fatalf("unverified sender got a %d-byte reply, want only a %d-byte cookie", n, cookieLen)
		}
	}
	if got > sent {
		t.Fatalf("replied with %dB to %dB of requests — that is an amplifier", got, sent)
	}
}

// The attacker's version: a minimal HELLO, hoping the bigger cookie comes back.
// It must get nothing at all.
func TestUnpaddedHelloIsIgnored(t *testing.T) {
	addr, rec := startServer(t, time.Minute)
	c := dial(t, addr)

	small := helloUnpadded(nil)
	if len(small) >= MinInitial {
		t.Fatalf("test is not exercising the small-datagram path: %d bytes", len(small))
	}
	if _, err := c.Write(small); err != nil {
		t.Fatal(err)
	}

	_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 2048)
	if n, err := c.Read(buf); err == nil {
		t.Fatalf("server replied %dB to an unpadded %dB datagram — free amplification", n, len(small))
	}
	if n := rec.count(); n != 0 {
		t.Fatalf("%d datagrams reached the app", n)
	}
}

// A cookie is bound to one address, so one stolen off the wire is useless from
// anywhere else.
func TestCookieIsBoundToItsAddress(t *testing.T) {
	c := newCookies()
	token := c.issue("1.2.3.4:5000")

	if !c.valid("1.2.3.4:5000", token) {
		t.Fatal("a freshly issued cookie must validate for its own address")
	}
	if c.valid("9.9.9.9:5000", token) {
		t.Fatal("cookie accepted from a different address — address proof is meaningless")
	}
	if c.valid("1.2.3.4:5001", token) {
		t.Fatal("cookie accepted from a different port")
	}

	tampered := append([]byte(nil), token...)
	tampered[len(tampered)-1] ^= 0xff
	if c.valid("1.2.3.4:5000", tampered) {
		t.Fatal("tampered cookie accepted — the MAC is not being checked")
	}
	if c.valid("1.2.3.4:5000", nil) || c.valid("1.2.3.4:5000", []byte("short")) {
		t.Fatal("malformed cookie accepted")
	}
}

func TestCookieExpires(t *testing.T) {
	c := newCookies()
	clock := time.Now()
	c.now = func() time.Time { return clock }

	token := c.issue("1.2.3.4:5000")
	clock = clock.Add(CookieLifetime + 5*time.Second)
	if c.valid("1.2.3.4:5000", token) {
		t.Fatal("expired cookie still accepted")
	}
}

// Two servers must not accept each other's cookies: the key is per-process and
// random, so a token minted elsewhere proves nothing here.
func TestCookieKeysAreNotShared(t *testing.T) {
	a, b := newCookies(), newCookies()
	token := a.issue("1.2.3.4:5000")
	if b.valid("1.2.3.4:5000", token) {
		t.Fatal("another server's cookie validated — the key is not random per process")
	}
}
