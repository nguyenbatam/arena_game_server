package session

import (
	"sync"
	"testing"
)

// Conn's mutable fields are written by the room tick goroutine and read by the
// connection goroutine. Run under -race, this pins that they are guarded.
func TestConnFieldsAreRaceFree(t *testing.T) {
	c := NewConn("c1", 32)
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() { // stands in for the room's broadcast loop
		defer wg.Done()
		for i := uint32(1); ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			c.SetLastTick(i)
			c.BindRoom("r1", 3)
		}
	}()

	wg.Add(1)
	go func() { // stands in for the reader / close path
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = c.LastTick()
			_, _ = c.Room()
			_ = c.ID()
			_ = c.Name()
		}
	}()

	for i := 0; i < 2000; i++ {
		c.SetName("n")
		c.SetPlayerID(uint32(i))
	}
	close(stop)
	wg.Wait()
}

// Room id and seat are one fact. Read separately they can be observed from two
// different bindings, which would record a disconnect against the wrong seat.
func TestRoomReadsTheBindingAsAPair(t *testing.T) {
	c := NewConn("c1", 32)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20000; i++ {
			if i%2 == 0 {
				c.BindRoom("alpha", 1)
			} else {
				c.BindRoom("beta", 2)
			}
		}
	}()
	for i := 0; i < 20000; i++ {
		room, seat := c.Room()
		switch {
		case room == "" && seat == 0:
		case room == "alpha" && seat == 1:
		case room == "beta" && seat == 2:
		default:
			t.Fatalf("torn binding: room=%q seat=%d", room, seat)
		}
	}
	<-done
}

// LastTick is a high-water mark: the tick goroutine and a client-supplied
// resume hint both write it, and a stale value would resume from a tick the
// client has already thrown away.
func TestLastTickNeverGoesBackwards(t *testing.T) {
	c := NewConn("c1", 32)
	c.SetLastTick(100)
	c.SetLastTick(40)
	if got := c.LastTick(); got != 100 {
		t.Fatalf("LastTick moved back to %d", got)
	}
	c.SetLastTick(101)
	if got := c.LastTick(); got != 101 {
		t.Fatalf("LastTick did not advance: %d", got)
	}
}

func TestLastTickConcurrentWritersKeepTheMax(t *testing.T) {
	c := NewConn("c1", 32)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(base uint32) {
			defer wg.Done()
			for i := uint32(1); i <= 500; i++ {
				c.SetLastTick(base + i)
			}
		}(uint32(w) * 1000)
	}
	wg.Wait()
	if got := c.LastTick(); got != 7500 {
		t.Fatalf("LastTick = %d, want 7500", got)
	}
}

// Rekey moves the conn between hub keys and rewrites its id; a concurrent
// reader must never see it filed under neither key.
func TestRekeyIsAtomicForLookups(t *testing.T) {
	h := NewHub()
	c := NewConn("boot", 32)
	h.Add(c)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 5000; i++ {
			if h.Get("boot") == nil && h.Get("durable") == nil {
				t.Error("conn was reachable under neither key")
				return
			}
		}
	}()
	h.Rekey(c, "durable")
	<-done
	if h.Get("durable") != c || c.ID() != "durable" {
		t.Fatalf("rekey did not take: id=%s", c.ID())
	}
}
