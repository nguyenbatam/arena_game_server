package ws

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nguyenbatam/arena_game_server/internal/session"
)

func wsServer(t *testing.T, origins []string, trustProxy bool) (url string, hub *session.Hub, got chan *session.Conn) {
	t.Helper()
	hub = session.NewHub()
	got = make(chan *session.Conn, 8)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", Serve(hub, session.FixedBuf(32), origins, trustProxy,
		func(c *session.Conn, _ []byte) {
			select {
			case got <- c:
			default:
			}
		},
		func(*session.Conn) {}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + srv.URL[4:] + "/ws", hub, got
}

func dialWS(t *testing.T, url string, hdr http.Header) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(url, hdr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// The session id must be minted by the server, never taken from the request.
// Hub.Add closes whatever is already filed under a key, so a client-supplied id
// is an unauthenticated eviction of whoever holds it — before HELLO, and so
// before any JWT check.
func TestQueryIDCannotEvictAnotherSession(t *testing.T) {
	url, hub, _ := wsServer(t, nil, false)

	victim := session.NewConn("victim-session", 32)
	hub.Add(victim)

	dialWS(t, url+"?id=victim-session", nil)
	time.Sleep(150 * time.Millisecond)

	if victim.Closed() {
		t.Fatal("an unauthenticated client closed another session by naming its id")
	}
	if hub.Get("victim-session") != victim {
		t.Fatal("the hub key was taken over by the attacker's connection")
	}
}

func TestServerMintsDistinctSessionIDs(t *testing.T) {
	url, _, got := wsServer(t, nil, false)
	a := dialWS(t, url+"?id=same", nil)
	b := dialWS(t, url+"?id=same", nil)
	if err := a.WriteMessage(websocket.BinaryMessage, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteMessage(websocket.BinaryMessage, []byte("x")); err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case c := <-got:
			if c.ID() == "same" {
				t.Fatal("server used the client-supplied id")
			}
			if ids[c.ID()] {
				t.Fatal("two connections share a session id")
			}
			ids[c.ID()] = true
		case <-time.After(2 * time.Second):
			t.Fatal("timed out")
		}
	}
}

func TestRemoteIPIsRecorded(t *testing.T) {
	url, _, got := wsServer(t, nil, false)
	c := dialWS(t, url, nil)
	if err := c.WriteMessage(websocket.BinaryMessage, []byte("x")); err != nil {
		t.Fatal(err)
	}
	select {
	case conn := <-got:
		if conn.RemoteIP != "127.0.0.1" {
			t.Fatalf("RemoteIP = %q, want 127.0.0.1", conn.RemoteIP)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

// A forwarded header is only believed when the deployment says a proxy is in
// front — otherwise any client picks its own rate-limit bucket.
func TestForwardedHeaderIsIgnoredWithoutTrustProxy(t *testing.T) {
	url, _, got := wsServer(t, nil, false)
	c := dialWS(t, url, http.Header{"X-Forwarded-For": {"9.9.9.9"}})
	if err := c.WriteMessage(websocket.BinaryMessage, []byte("x")); err != nil {
		t.Fatal(err)
	}
	select {
	case conn := <-got:
		if conn.RemoteIP == "9.9.9.9" {
			t.Fatal("X-Forwarded-For was trusted with TrustProxy off")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

func TestForwardedHeaderIsUsedWithTrustProxy(t *testing.T) {
	url, _, got := wsServer(t, nil, true)
	c := dialWS(t, url, http.Header{"X-Forwarded-For": {"9.9.9.9, 10.0.0.1"}})
	if err := c.WriteMessage(websocket.BinaryMessage, []byte("x")); err != nil {
		t.Fatal(err)
	}
	select {
	case conn := <-got:
		if conn.RemoteIP != "9.9.9.9" {
			t.Fatalf("RemoteIP = %q, want the leftmost forwarded address", conn.RemoteIP)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

func TestTokenQueryReachesTheConn(t *testing.T) {
	url, _, got := wsServer(t, nil, false)
	c := dialWS(t, url+"?token=abc123", nil)
	if err := c.WriteMessage(websocket.BinaryMessage, []byte("x")); err != nil {
		t.Fatal(err)
	}
	select {
	case conn := <-got:
		if conn.Token != "abc123" {
			t.Fatalf("Token = %q", conn.Token)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

func TestDisallowedOriginIsRefused(t *testing.T) {
	url, _, _ := wsServer(t, []string{"https://game.example"}, false)
	_, resp, err := websocket.DefaultDialer.Dial(url, http.Header{"Origin": {"https://evil.example"}})
	if err == nil {
		t.Fatal("upgrade succeeded from a disallowed origin")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %v", resp)
	}
}

func TestAllowedOriginPasses(t *testing.T) {
	url, _, _ := wsServer(t, []string{"https://game.example"}, false)
	dialWS(t, url, http.Header{"Origin": {"https://game.example"}})
}

func TestOriginAllowedMatrix(t *testing.T) {
	cases := []struct {
		name    string
		allowed []string
		origin  string
		want    bool
	}{
		{"no list falls back to same-origin", nil, "https://evil.example", false},
		{"no list admits the host it is served from", nil, "https://game.example", true},
		{"empty origin is a non-browser client", []string{"https://game.example"}, "", true},
		{"empty origin with no list either", nil, "", true},
		{"wildcard", []string{"*"}, "https://evil.example", true},
		{"exact match", []string{"https://game.example"}, "https://game.example", true},
		{"mismatch", []string{"https://game.example"}, "https://evil.example", false},
		{"one of several", []string{"https://a.example", "https://b.example"}, "https://b.example", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The request is addressed to game.example, so that is what
			// "same origin" means for the no-allowlist cases.
			r := httptest.NewRequest("GET", "https://game.example/ws", nil)
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if got := originAllowed(tc.allowed, r); got != tc.want {
				t.Fatalf("originAllowed = %v, want %v", got, tc.want)
			}
		})
	}
}

// Closing the outbox must end the write pump and the socket, not leak it.
func TestClosingOutboxEndsTheConnection(t *testing.T) {
	url, _, got := wsServer(t, nil, false)
	c := dialWS(t, url, nil)
	if err := c.WriteMessage(websocket.BinaryMessage, []byte("x")); err != nil {
		t.Fatal(err)
	}
	var conn *session.Conn
	select {
	case conn = <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
	conn.Send([]byte("bye-first"))
	_, msg, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "bye-first" {
		t.Fatalf("got %q", msg)
	}
	conn.Close()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := c.ReadMessage(); err == nil {
		t.Fatal("connection stayed open after the outbox closed")
	}
}

// With no allowlist configured, a cross-origin page must not be able to open a
// socket. This is the shipped default — ALLOWED_ORIGINS is unset unless a
// deployment sets it — so it is the case that matters most.
func TestNoAllowlistRefusesCrossOrigin(t *testing.T) {
	url, _, _ := wsServer(t, nil, false)
	_, resp, err := websocket.DefaultDialer.Dial(url, http.Header{"Origin": {"https://evil.example"}})
	if err == nil {
		t.Fatal("upgrade succeeded from a cross-origin page with no allowlist set")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %v", resp)
	}
}

// ...and the bundled web client, which this same process serves, still works.
func TestNoAllowlistAdmitsItsOwnPage(t *testing.T) {
	url, _, _ := wsServer(t, nil, false)
	host := strings.TrimPrefix(url, "ws://")
	host = host[:strings.IndexByte(host, '/')]
	dialWS(t, url, http.Header{"Origin": {"http://" + host}})
}

// A client that sends no Origin at all is not a browser — the load generator,
// a native client — and has nothing to check.
func TestNoAllowlistAdmitsANonBrowserClient(t *testing.T) {
	url, _, _ := wsServer(t, nil, false)
	dialWS(t, url, nil)
}

// "*" stays the way a deployment asks for any origin on purpose.
func TestWildcardStillAllowsAnyOrigin(t *testing.T) {
	url, _, _ := wsServer(t, []string{"*"}, false)
	dialWS(t, url, http.Header{"Origin": {"https://anywhere.example"}})
}
