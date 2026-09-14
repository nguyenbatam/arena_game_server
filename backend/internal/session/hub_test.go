package session

import (
	"sync"
	"testing"
)

func TestHubRekey(t *testing.T) {
	h := NewHub()
	c := NewConn("temp", 8)
	h.Add(c)
	h.Rekey(c, "p-fixed")
	if h.Get("temp") != nil || h.Get("p-fixed") != c || c.ID() != "p-fixed" {
		t.Fatalf("rekey failed id=%s", c.ID())
	}
}

func TestHubReplaceOnReconnect(t *testing.T) {
	h := NewHub()
	a := NewConn("p1", 8)
	b := NewConn("p1", 8)
	h.Add(a)
	h.Add(b)
	if h.Get("p1") != b {
		t.Fatal("expected new conn")
	}
	if !a.Closed() {
		t.Fatal("old conn should close on replace")
	}
	h.Remove(a)
	if h.Get("p1") != b {
		t.Fatal("remove stale must not drop replacement")
	}
	h.Remove(b)
	if h.Get("p1") != nil {
		t.Fatal("expected empty")
	}
}

func TestSendAfterCloseNoPanic(t *testing.T) {
	c := NewConn("p", 4)
	c.Close()
	c.Send([]byte("x"))
	var wg sync.WaitGroup
	d := NewConn("q", 4)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Send([]byte("x"))
		}()
	}
	d.Close()
	wg.Wait()
}

func TestHubConcurrentAddRemove(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := NewConn("x", 4)
			h.Add(c)
			h.Remove(c)
		}()
	}
	wg.Wait()
	if h.Count() != 0 {
		t.Fatalf("count %d", h.Count())
	}
}
