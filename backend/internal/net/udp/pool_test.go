package udp

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/session"
)

// getDgram must always hand out a full-sized buffer, and putDgram must refuse
// anything that did not come from the pool — otherwise a short slice could be
// admitted and later handed out as if it had room for a whole datagram.
func TestDgramPoolRoundTrip(t *testing.T) {
	b := getDgram()
	if len(b) != MaxDatagram || cap(b) != MaxDatagram {
		t.Fatalf("got len %d cap %d, want %d of each", len(b), cap(b), MaxDatagram)
	}

	// A buffer handed back after being sliced down is still the same array.
	b[0] = 0xAB
	putDgram(b[:10])
	again := getDgram()
	if len(again) != MaxDatagram {
		t.Fatalf("recycled buffer came back with len %d, want %d", len(again), MaxDatagram)
	}

	// A foreign slice is dropped rather than admitted.
	putDgram(make([]byte, 8))
	for i := 0; i < 64; i++ {
		if got := getDgram(); len(got) != MaxDatagram || cap(got) != MaxDatagram {
			t.Fatalf("pool handed out len %d cap %d", len(got), cap(got))
		}
	}
}

// Many peers sending at once, each datagram naming the peer that sent it.
//
// This is what a buffer returned to the pool too early looks like from the
// outside: the read loop refills it with somebody else's datagram while it is
// still queued for its original peer, and that peer's goroutine then hands the
// handler bytes belonging to another connection. Nothing about the payload
// itself would look wrong — it is a complete, well-formed message — so the only
// thing that catches it is checking the payload against the connection it
// arrived on.
func TestPooledBuffersDoNotCrossBetweenPeers(t *testing.T) {
	const peers = 8
	const perPeer = 20

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })

	type seen struct {
		conn *session.Conn
		peer uint32
		seq  uint32
	}
	var mu sync.Mutex
	var got []seen
	done := make(chan struct{})
	var once sync.Once

	go Serve(pc, session.NewHub(), session.FixedBuf(64),
		func(c *session.Conn, msg []byte) {
			if len(msg) < 8 {
				return
			}
			// Read everything out before returning: the buffer is reclaimed
			// the instant this handler does.
			s := seen{conn: c,
				peer: binary.BigEndian.Uint32(msg[:4]),
				seq:  binary.BigEndian.Uint32(msg[4:8]),
			}
			mu.Lock()
			got = append(got, s)
			if len(got) >= peers*perPeer {
				once.Do(func() { close(done) })
			}
			mu.Unlock()
		},
		func(*session.Conn) {}, time.Minute)

	addr := pc.LocalAddr().(*net.UDPAddr)
	var wg sync.WaitGroup
	for p := 0; p < peers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			c := dial(t, addr)
			handshake(t, c)
			payload := make([]byte, 256)
			for i := 0; i < perPeer; i++ {
				binary.BigEndian.PutUint32(payload[:4], uint32(p))
				binary.BigEndian.PutUint32(payload[4:8], uint32(i))
				if _, err := c.Write(payload); err != nil {
					return
				}
			}
		}(p)
	}
	wg.Wait()

	// Not every datagram has to arrive: this is UDP, and a peer whose inbox
	// fills has its excess dropped on purpose — see peerInbox. What must hold
	// is that everything which did arrive arrived intact and on the right
	// connection.
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}

	mu.Lock()
	defer mu.Unlock()
	// Every datagram a peer sent must have arrived on that peer's own
	// connection, and no (peer, seq) may arrive twice.
	connOf := map[uint32]*session.Conn{}
	dup := map[[2]uint32]bool{}
	for _, s := range got {
		// The handshake HELLOs also reach the handler; they are protobuf, so
		// their first four bytes are not a peer index in range.
		if s.peer >= peers {
			continue
		}
		key := [2]uint32{s.peer, s.seq}
		if dup[key] {
			t.Fatalf("peer %d seq %d arrived twice: a buffer was recycled while still queued",
				s.peer, s.seq)
		}
		dup[key] = true
		if prev, ok := connOf[s.peer]; ok && prev != s.conn {
			t.Fatalf("peer %d's datagrams arrived on two different connections", s.peer)
		}
		connOf[s.peer] = s.conn
	}
	if len(connOf) != peers {
		t.Errorf("saw datagrams from %d peers, want %d", len(connOf), peers)
	}
	if len(dup) < peers*perPeer/2 {
		t.Errorf("only %d of %d datagrams arrived; too few to say much about the pool",
			len(dup), peers*perPeer)
	}
}
