package udp

import (
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/session"
)

// ---------------------------------------------------------------------------
// Per-peer delivery

// The socket has one receive queue shared by every client. Handling a datagram
// on the read loop makes the slowest handler the arrival rate for everyone: a
// HELLO doing its presence lookups stalls the loop, the kernel buffer fills,
// and datagrams are dropped for peers that did nothing wrong.
func TestOneStalledPeerDoesNotBlockTheOthers(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	var handled atomic.Int64
	var stalls atomic.Int64
	stalled := make(chan struct{})
	release := make(chan struct{})

	go Serve(pc, session.NewHub(), session.FixedBuf(32), func(c *session.Conn, msg []byte) {
		if len(msg) > 6 && string(msg[len(msg)-6:]) == "STALLM" {
			if stalls.Add(1) == 1 {
				close(stalled)
				<-release // stands in for onHello's Redis round trips
			}
			return
		}
		handled.Add(1)
	}, func(*session.Conn) {}, time.Minute)

	addr := pc.LocalAddr().(*net.UDPAddr)
	slow := dial(t, addr)
	handshake(t, slow)
	peers := make([]*net.UDPConn, 3)
	for i := range peers {
		peers[i] = dial(t, addr)
		handshake(t, peers[i])
	}
	waitFor(t, "handshakes", func() bool { return handled.Load() >= 4 })
	handled.Store(0)

	if _, err := slow.Write([]byte("xxSTALLM")); err != nil {
		t.Fatal(err)
	}
	<-stalled

	for i := 0; i < 30; i++ {
		for _, c := range peers {
			_, _ = c.Write([]byte("input"))
		}
	}
	time.Sleep(400 * time.Millisecond)
	got := handled.Load()
	close(release)

	if got == 0 {
		t.Fatalf("one stalled peer blocked all others: 0 of 90 datagrams handled")
	}
	t.Logf("handled %d/90 while one peer was stalled", got)
}

// Per-peer delivery must not let one peer's messages overtake each other: the
// handshake has to be seen before what follows it.
func TestOnePeerKeepsItsOwnOrder(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	var mu sync.Mutex
	var seen []string
	done := make(chan struct{})
	var once sync.Once

	go Serve(pc, session.NewHub(), session.FixedBuf(32), func(_ *session.Conn, msg []byte) {
		mu.Lock()
		seen = append(seen, string(msg))
		if len(seen) == 21 {
			once.Do(func() { close(done) })
		}
		mu.Unlock()
	}, func(*session.Conn) {}, time.Minute)

	c := dial(t, pc.LocalAddr().(*net.UDPAddr))
	handshake(t, c)
	for i := 0; i < 20; i++ {
		if _, err := c.Write([]byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		mu.Lock()
		t.Fatalf("only %d of 21 messages arrived", len(seen))
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < 21; i++ {
		if seen[i] != string([]byte{byte('a' + i - 1)}) {
			t.Fatalf("message %d out of order: %q", i, seen[i])
		}
	}
}

// A peer whose handler never returns must not keep its goroutine alive past
// eviction, and must not wedge the sweeper for everyone else.
func TestStalledPeerIsStillEvicted(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	release := make(chan struct{})
	defer close(release)
	var closed atomic.Int64

	go Serve(pc, session.NewHub(), session.FixedBuf(32), func(*session.Conn, []byte) {
		<-release
	}, func(*session.Conn) { closed.Add(1) }, 150*time.Millisecond)

	c := dial(t, pc.LocalAddr().(*net.UDPAddr))
	handshake(t, c)
	waitFor(t, "the stalled peer to be evicted", func() bool { return closed.Load() >= 1 })
}

// Overflow must drop rather than block, or the inbox reintroduces the stall it
// exists to remove.
func TestFullInboxDropsInsteadOfBlocking(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	release := make(chan struct{})
	var seen atomic.Int64
	go Serve(pc, session.NewHub(), session.FixedBuf(32), func(_ *session.Conn, msg []byte) {
		if seen.Add(1) == 1 {
			<-release
		}
	}, func(*session.Conn) {}, time.Minute)

	c := dial(t, pc.LocalAddr().(*net.UDPAddr))
	handshake(t, c)
	waitFor(t, "the handshake to stall the peer", func() bool { return seen.Load() >= 1 })

	// Far more than peerInbox; the writes must all return promptly.
	start := time.Now()
	for i := 0; i < 4*peerInbox; i++ {
		if _, err := c.Write([]byte("input")); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("sending past a full inbox took %s", el)
	}
	close(release)
}

// A second peer must get its own session and its own goroutine, not share one.
func TestPeersAreHandledIndependently(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	var mu sync.Mutex
	byConn := map[string]int{}
	go Serve(pc, session.NewHub(), session.FixedBuf(32), func(c *session.Conn, _ []byte) {
		mu.Lock()
		byConn[c.ID()]++
		mu.Unlock()
	}, func(*session.Conn) {}, time.Minute)

	addr := pc.LocalAddr().(*net.UDPAddr)
	for i := 0; i < 3; i++ {
		c := dial(t, addr)
		handshake(t, c)
		if _, err := c.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "three distinct peers", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(byConn) == 3
	})
}

// Dropping a peer twice must not panic: a panic in the sweeper would take every
// other peer down with it.
func TestDoubleDropIsSafe(t *testing.T) {
	s := &server{
		peers: map[string]*peer{}, cookies: newCookies(), done: make(chan struct{}),
		hub:   session.NewHub(),
		onMsg: func(*session.Conn, []byte) {}, onClose: func(*session.Conn) {},
	}
	p := &peer{
		conn: session.NewConn("p", 8),
		in:   make(chan []byte, 1),
		dead: make(chan struct{}),
	}
	s.drop(p)
	s.drop(p)
}

// Each peer now costs two goroutines, a writer and a reader. Eviction must give
// both back: at the CCU this server targets, leaking one per peer is thousands
// of goroutines that never exit.
func TestEvictedPeersReleaseTheirGoroutines(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	var closed atomic.Int64
	go Serve(pc, session.NewHub(), session.FixedBuf(32),
		func(*session.Conn, []byte) {},
		func(*session.Conn) { closed.Add(1) },
		150*time.Millisecond)

	addr := pc.LocalAddr().(*net.UDPAddr)
	// Let the server's own goroutines settle before taking the baseline.
	c0 := dial(t, addr)
	handshake(t, c0)
	waitFor(t, "the first peer to be evicted", func() bool { return closed.Load() >= 1 })
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()

	const peers = 25
	for i := 0; i < peers; i++ {
		c := dial(t, addr)
		handshake(t, c)
	}
	waitFor(t, "every peer to be evicted", func() bool { return closed.Load() >= peers+1 })

	deadline := time.Now().Add(3 * time.Second)
	for {
		runtime.GC()
		now := runtime.NumGoroutine()
		if now <= base+2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines still running after %d peers were evicted (baseline %d)",
				now, peers, base)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Eviction closes the session and reports it. Anything still queued for that
// peer must be discarded, not handed to the handler afterwards: onClose has
// already run, and a late HELLO would write a fresh presence record for a
// session that no longer exists.
func TestQueuedDatagramsAreDroppedAfterEviction(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	var afterClose atomic.Int64
	var evicted atomic.Bool
	hold := make(chan struct{})
	var once sync.Once

	go Serve(pc, session.NewHub(), session.FixedBuf(32), func(*session.Conn, []byte) {
		// Sampled on entry, not on exit: the datagram that parks the peer is
		// already inside the handler when eviction happens, and finishing late
		// is not the same as being started late.
		startedAfterClose := evicted.Load()
		once.Do(func() { <-hold }) // park the peer so its inbox backs up
		if startedAfterClose {
			afterClose.Add(1)
		}
	}, func(*session.Conn) { evicted.Store(true) }, 150*time.Millisecond)

	c := dial(t, pc.LocalAddr().(*net.UDPAddr))
	handshake(t, c)
	for i := 0; i < 10; i++ {
		if _, err := c.Write([]byte("queued")); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "the peer to be evicted", evicted.Load)
	close(hold)
	time.Sleep(200 * time.Millisecond)

	if n := afterClose.Load(); n != 0 {
		t.Fatalf("%d datagrams were handled after onClose ran", n)
	}
}
