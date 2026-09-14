package tcp

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/session"
)

func serveOne(t *testing.T) (addr string, hub *session.Hub, got chan *session.Conn, closed chan *session.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	hub = session.NewHub()
	got = make(chan *session.Conn, 8)
	closed = make(chan *session.Conn, 8)
	go Serve(ln, hub, session.FixedBuf(32),
		func(c *session.Conn, _ []byte) {
			select {
			case got <- c:
			default:
			}
		},
		func(c *session.Conn) {
			select {
			case closed <- c:
			default:
			}
		})
	return ln.Addr().String(), hub, got, closed
}

func dialAndSend(t *testing.T, addr string, payload []byte) net.Conn {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	if _, err := writeFrame(nc, nil, payload); err != nil {
		t.Fatal(err)
	}
	return nc
}

// Every rate limiter in the gateway is keyed on RemoteIP, and allowRate treats
// an empty key as "no key, allow" — so a transport that forgets to set it does
// not lose a label, it turns rate limiting off entirely.
func TestConnCarriesRemoteIP(t *testing.T) {
	addr, _, got, _ := serveOne(t)
	dialAndSend(t, addr, []byte("hi"))

	select {
	case c := <-got:
		if c.RemoteIP == "" {
			t.Fatal("RemoteIP is empty: rate limiting is disabled for every TCP client")
		}
		if c.RemoteIP != "127.0.0.1" {
			t.Fatalf("RemoteIP = %q, want 127.0.0.1", c.RemoteIP)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message reached the handler")
	}
}

// Two clients from the same host must share a rate-limit key, and the id must
// not be shared.
func TestEachConnGetsItsOwnSessionID(t *testing.T) {
	addr, _, got, _ := serveOne(t)
	dialAndSend(t, addr, []byte("a"))
	dialAndSend(t, addr, []byte("b"))

	ids := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case c := <-got:
			if ids[c.ID()] {
				t.Fatalf("duplicate session id %q", c.ID())
			}
			ids[c.ID()] = true
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for connections")
		}
	}
}

func TestFrameRoundTrip(t *testing.T) {
	for _, payload := range [][]byte{{}, []byte("x"), bytes.Repeat([]byte("ab"), 1000)} {
		var buf bytes.Buffer
		if _, err := writeFrame(&buf, nil, payload); err != nil {
			t.Fatal(err)
		}
		got, err := readFrame(&buf, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("round trip changed %d bytes", len(payload))
		}
	}
}

// The header and body go out in one Write. Two writes is two syscalls per
// snapshot per client, and on a tick loop that is worth removing.
func TestFrameIsWrittenInOneCall(t *testing.T) {
	cw := &countingWriter{}
	if _, err := writeFrame(cw, nil, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if cw.n != 1 {
		t.Fatalf("writeFrame made %d Write calls, want 1", cw.n)
	}
}

type countingWriter struct{ n int }

func (c *countingWriter) Write(p []byte) (int, error) { c.n++; return len(p), nil }

// An oversized length prefix must be refused before the allocation, or a
// four-byte header buys an attacker an arbitrary allocation.
func TestOversizedFrameIsRejected(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], maxFrame+1)
	_, err := readFrame(bytes.NewReader(hdr[:]), nil)
	if err == nil {
		t.Fatal("oversized frame accepted")
	}
}

func TestTruncatedFrameIsAnError(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 64)
	_, err := readFrame(io.MultiReader(bytes.NewReader(hdr[:]), bytes.NewReader([]byte("short"))), nil)
	if err == nil {
		t.Fatal("truncated frame accepted")
	}
}

// Closing the socket must run onClose exactly once and take the conn out of the
// hub, or a disconnect leaves its seat and presence behind.
func TestDisconnectRemovesConnFromHub(t *testing.T) {
	addr, hub, got, closed := serveOne(t)
	nc := dialAndSend(t, addr, []byte("hi"))
	var c *session.Conn
	select {
	case c = <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}
	_ = nc.Close()

	select {
	case gone := <-closed:
		if gone != c {
			t.Fatal("onClose fired for a different conn")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("onClose never fired")
	}
	deadline := time.Now().Add(2 * time.Second)
	for hub.Get(c.ID()) != nil {
		if time.Now().After(deadline) {
			t.Fatal("conn still in the hub after disconnect")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !c.Closed() {
		t.Fatal("conn outbox left open")
	}
}

// Messages must reach the handler in order and intact across several frames.
func TestMultipleFramesArriveInOrder(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	hub := session.NewHub()
	var mu sync.Mutex
	var seen [][]byte
	done := make(chan struct{})
	go Serve(ln, hub, session.FixedBuf(32), func(_ *session.Conn, m []byte) {
		mu.Lock()
		seen = append(seen, append([]byte(nil), m...))
		if len(seen) == 3 {
			close(done)
		}
		mu.Unlock()
	}, func(*session.Conn) {})

	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	for _, m := range []string{"one", "two", "three"} {
		if _, err := writeFrame(nc, nil, []byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive three frames")
	}
	mu.Lock()
	defer mu.Unlock()
	for i, want := range []string{"one", "two", "three"} {
		if string(seen[i]) != want {
			t.Fatalf("frame %d = %q, want %q", i, seen[i], want)
		}
	}
}

// The server must write what the room hands it, framed.
func TestOutboxReachesTheWire(t *testing.T) {
	addr, _, got, _ := serveOne(t)
	nc := dialAndSend(t, addr, []byte("hi"))
	var c *session.Conn
	select {
	case c = <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}
	c.Send([]byte("snapshot"))

	_ = nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	msg, err := readFrame(nc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "snapshot" {
		t.Fatalf("got %q, want %q", msg, "snapshot")
	}
}
