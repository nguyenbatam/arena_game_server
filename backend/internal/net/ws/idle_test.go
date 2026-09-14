package ws

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nguyenbatam/arena_game_server/internal/session"
)

// The read deadline used to be refreshed only by the pong handler, so liveness
// depended on a control frame rather than on the traffic the connection exists
// for. Browsers answer a ping without being asked to, which is why it went
// unnoticed — but a client that speaks the protocol and does not implement pong
// was dropped at the deadline in the middle of a match it was actively playing.

// countingServer reports how many frames reached the handler and when the
// connection was closed.
func countingServer(t *testing.T) (url string, frames *atomic.Int32, closed chan struct{}) {
	t.Helper()
	frames = &atomic.Int32{}
	closed = make(chan struct{})
	var once atomic.Bool
	hub := session.NewHub()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", Serve(hub, session.FixedBuf(32), nil, false,
		func(*session.Conn, []byte) { frames.Add(1) },
		func(*session.Conn) {
			if once.CompareAndSwap(false, true) {
				close(closed)
			}
		}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + srv.URL[4:] + "/ws", frames, closed
}

// deafClient dials and refuses to answer pings, which is the whole point: the
// only thing keeping it alive must be the frames it sends.
func deafClient(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c := dialWS(t, url, nil)
	c.SetPingHandler(func(string) error { return nil })
	// Something has to drive the client's read loop for control frames to be
	// processed at all; this also proves the server never closed on us.
	go func() {
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}()
	return c
}

func TestAClientThatSendsButNeverPongsStaysConnected(t *testing.T) {
	defer readWaitFor(readWaitFor(250 * time.Millisecond))

	url, frames, closed := countingServer(t)
	c := deafClient(t, url)

	// Keep sending across four idle windows. Pongs never happen, so if the
	// deadline is not refreshed by the frames themselves this cannot survive.
	deadline := time.After(time.Second)
	sent := 0
sending:
	for {
		select {
		case <-closed:
			t.Fatalf("connection closed after %d frames despite continuous traffic", sent)
		case <-deadline:
			break sending
		case <-time.After(50 * time.Millisecond):
			if err := c.WriteMessage(websocket.BinaryMessage, []byte("input")); err != nil {
				t.Fatalf("write failed after %d frames: %v", sent, err)
			}
			sent++
		}
	}

	if got := frames.Load(); int(got) < sent {
		t.Fatalf("server saw %d of %d frames", got, sent)
	}
	select {
	case <-closed:
		t.Fatal("connection was closed while it was still sending")
	default:
	}
}

// The other half: the window still has to close on a connection that genuinely
// has nothing to say. Refreshing on data must not turn the idle timeout off.
func TestASilentClientIsStillDroppedAtTheDeadline(t *testing.T) {
	defer readWaitFor(readWaitFor(200 * time.Millisecond))

	url, _, closed := countingServer(t)
	c := deafClient(t, url)
	// One frame, then silence. The refresh buys exactly one more window.
	if err := c.WriteMessage(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("a silent connection was never dropped; the idle window is not being enforced")
	}
}

// Pongs still refresh it. The change added a second way to prove liveness; it
// did not replace the one an idle-but-present client relies on — a client with
// nothing to say answers the server's ping and that has to be enough.
//
// Driven with an unsolicited pong rather than by waiting for the server's own
// ping, so the case does not depend on pingEvery: the handler under test is the
// same one either way.
func TestPongAloneKeepsAnOtherwiseSilentClientAlive(t *testing.T) {
	defer readWaitFor(readWaitFor(250 * time.Millisecond))

	url, frames, closed := countingServer(t)
	c := deafClient(t, url)

	deadline := time.After(time.Second)
ponging:
	for {
		select {
		case <-closed:
			t.Fatal("a client answering pings was dropped")
		case <-deadline:
			break ponging
		case <-time.After(50 * time.Millisecond):
			if err := c.WriteControl(websocket.PongMessage, nil, time.Now().Add(time.Second)); err != nil {
				t.Fatalf("pong failed: %v", err)
			}
		}
	}

	if got := frames.Load(); got != 0 {
		t.Fatalf("the client sent %d data frames; this case is meant to be pongs only", got)
	}
}

// The ping interval has to sit comfortably inside the idle window, or an idle
// client is dropped before it is ever asked to prove it is there.
func TestPingIntervalLeavesRoomInsideTheIdleWindow(t *testing.T) {
	if pingEvery*2 > DefaultReadWait {
		t.Fatalf("pingEvery=%s gives fewer than two chances inside readWait=%s", pingEvery, DefaultReadWait)
	}
	if writeWait >= pingEvery {
		t.Fatalf("writeWait=%s is not shorter than the ping interval %s", writeWait, pingEvery)
	}
}
