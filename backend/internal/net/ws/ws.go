package ws

import (
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nguyenbatam/arena_game_server/internal/net/httputil"
	"github.com/nguyenbatam/arena_game_server/internal/safe"
	"github.com/nguyenbatam/arena_game_server/internal/session"
)

// writeBufPool lends out write buffers instead of giving every connection one
// of its own for life.
//
// Without a pool gorilla allocates WriteBufferSize bytes per connection at
// upgrade time and holds them until the socket closes (conn.go: `if writeBuf ==
// nil && writeBufferPool == nil { writeBuf = make(...) }`). At 4 KB and 10k
// concurrent players that is ~41 MB sitting idle, because a connection only
// needs the buffer while it is actually writing a frame — which at 20 Hz is a
// vanishing fraction of its life. The pool makes the resident cost track
// concurrent writers rather than concurrent connections.
//
// A sync.Pool is the right shape here and not merely the convenient one: it is
// already per-P, so the tick goroutines fanning out snapshots take their
// buffers without contending, and GC reclaims the pool under memory pressure
// instead of holding the high-water mark forever.
var writeBufPool = &sync.Pool{}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  512,
	WriteBufferSize: 4096,
	WriteBufferPool: writeBufPool,
}

// DefaultReadWait is how long a connection may go without sending anything.
// Clients send input at the tick rate, so silence really does mean gone. The
// TCP transport uses the same window for the same reason.
//
// It is refreshed on every frame that arrives, not only on a pong. Pongs alone
// were enough while every client was a browser — browsers answer a ping without
// being asked to — but it made the liveness check depend on a control frame
// rather than on the traffic the connection actually exists for, so a client
// that spoke the protocol correctly and did not implement pong was cut off at
// sixty seconds mid-match. A connection delivering input is alive by
// definition.
const DefaultReadWait = 60 * time.Second

// readWait holds the live value.
//
// Atomic because tests shorten it while connections are running, and a plain
// variable read from those goroutines is a data race — in the test binary only,
// but the race detector is right about it, and an int64 is cheaper than an
// explanation. internal/safe keeps its restart delay the same way.
var readWait atomic.Int64

func init() { readWait.Store(int64(DefaultReadWait)) }

func idleWindow() time.Duration { return time.Duration(readWait.Load()) }

// readWaitFor swaps the idle window and returns the previous one. Tests only:
// a minute is right in production and is a minute of dead time in a test that
// asserts what happens either side of the deadline.
func readWaitFor(d time.Duration) time.Duration {
	return time.Duration(readWait.Swap(int64(d)))
}

// maxFrame caps one message. Same ceiling the TCP transport sets.
const maxFrame = 1 << 16

// readScratch is how big a buffer each connection keeps for reading frames
// into. Sized for the traffic, not for maxFrame — see readFrame, and see
// tcp.readScratch, which states the same argument for the same reason.
const readScratch = 2048

// pingEvery is how often the server prompts an idle client. Comfortably inside
// readWait, so a client that has nothing to say still has two chances to prove
// it is there before the deadline.
const pingEvery = 20 * time.Second

// writeWait bounds one frame write, so a peer that stops reading cannot park
// this connection's writer goroutine forever.
const writeWait = 5 * time.Second

// originAllowed is the test a handshake has to pass before it becomes a socket.
//
// With an allowlist configured it is that list, and "*" is how a deployment
// says it genuinely wants any origin. With no allowlist it is same-origin,
// which is what the bundled web client needs — this process serves it — and
// what a browser can be held to.
//
// An empty list used to mean "allow anything", and Serve then handed the
// upgrader a CheckOrigin that returned true unconditionally, which also
// switched off the same-origin test gorilla would otherwise have applied. So
// the shipped default — ALLOWED_ORIGINS is unset unless someone sets it — let
// any page on the internet open a socket to the gateway. The bearer token
// keeps that from being an account takeover, and it still means a stranger's
// page spending this server's connection and rate-limit budget.
func originAllowed(allowed []string, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		// Not a browser: no origin to check, and no ambient credential for one
		// to abuse. The TCP and UDP transports have no such notion either.
		return true
	}
	if len(allowed) == 0 {
		return sameOrigin(origin, r.Host)
	}
	for _, o := range allowed {
		if o == "*" || o == origin {
			return true
		}
	}
	return false
}

// sameOrigin reports whether an Origin header names the host that is answering.
//
// This is the check gorilla makes when CheckOrigin is left nil. It is written
// out rather than inherited so that the default is a decision this file states,
// and so the pre-flight above and the upgrader below cannot drift apart.
func sameOrigin(origin, host string) bool {
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Host, host)
}

type Handler func(c *session.Conn, msg []byte)

func Serve(hub *session.Hub, sendBuf session.BufSize, allowedOrigins []string, trustProxy bool, onMsg Handler, onClose func(*session.Conn)) http.HandlerFunc {
	// One upgrader for every connection this handler accepts, rather than a
	// copy per request: it is read-only during Upgrade, and the only reason it
	// was being copied was to install the origin test, which does not vary.
	//
	// The test installed here is the same one the pre-flight below applies.
	// They used to differ — the pre-flight consulted the allowlist and the
	// upgrader's returned true for everything — which made the allowlist rest
	// entirely on a check outside the upgrader, with nothing to catch it if it
	// were ever moved or skipped.
	up := upgrader
	up.CheckOrigin = func(r *http.Request) bool { return originAllowed(allowedOrigins, r) }
	return func(w http.ResponseWriter, r *http.Request) {
		if !originAllowed(allowedOrigins, r) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// The session id is minted here, never taken from the request. A
		// client-supplied id is an unauthenticated write into the hub's key
		// space: Hub.Add closes whatever is already filed under that key, so
		// anyone who knows a victim's id could evict them before HELLO — and
		// before any JWT check — then inherit their presence and resume their
		// match. A client that wants to resume says so in HELLO, where
		// authenticate can actually verify the claim and rekey.
		c := session.NewConn(session.NewID(), sendBuf())
		c.RemoteIP = httputil.RealIP(r, trustProxy)
		c.Token = r.URL.Query().Get("token")
		hub.Add(c)
		defer func() {
			hub.Remove(c)
			onClose(c)
			c.Close()
			if err := conn.Close(); err != nil {
				logClose("ws.serve", c, err)
			}
		}()

		conn.SetReadLimit(maxFrame)
		// A deadline that cannot be set is not a detail to skip past: the read
		// that follows would then block with no deadline at all, and this
		// goroutine — plus the writer and the session behind it — is never
		// reclaimed. It only fails on a socket that is already finished, so
		// giving up is both the honest and the cheap answer. The pong handler
		// says the same thing by returning the error, which gorilla propagates
		// out of ReadMessage and into the loop below.
		if err := conn.SetReadDeadline(time.Now().Add(idleWindow())); err != nil {
			return
		}
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(idleWindow()))
		})

		safe.Go("ws.write", func() {
			writePump(conn, c)
			if err := conn.Close(); err != nil {
				logClose("ws.write", c, err)
			}
		})

		// One scratch buffer for the life of the connection. This goroutine is
		// the only reader and the handler is not allowed to keep what it is
		// given, so there is nobody to share it with. See readScratch.
		scratch := make([]byte, readScratch)
		for {
			_, r, err := conn.NextReader()
			if err != nil {
				return
			}
			data, err := readFrame(r, scratch)
			if err != nil {
				return
			}
			// A frame that arrived is proof of life, and a better one than a
			// pong: it is the traffic this connection is for. See readWait.
			if err := conn.SetReadDeadline(time.Now().Add(idleWindow())); err != nil {
				return
			}
			onMsg(c, data)
		}
	}
}

// readFrame reads one message into scratch, or into a buffer of its own when
// the frame does not fit. The returned slice is only valid until the next call
// — see Handler.
//
// This is the read half of the argument tcp.readScratch and udp.dgrams already
// make, arriving late at the transport that carries the most traffic.
// conn.ReadMessage, which this replaces, is NextReader followed by
// io.ReadAll — and io.ReadAll opens with make([]byte, 0, 512) and a chunk
// slice, so every frame cost two allocations and ~544 bytes to deliver an input
// message of a few dozen. At the tick rate and ten thousand players that is
// 400k allocations a second, all of it garbage before the next tick.
//
// Sized for the traffic rather than for maxFrame, for the reason tcp gives:
// keeping 64 KB per connection because one client once sent a large frame is
// how a read buffer becomes a memory leak. A frame that does not fit gets a
// buffer of its own and the scratch stays small.
func readFrame(r io.Reader, scratch []byte) ([]byte, error) {
	n, err := io.ReadFull(r, scratch)
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		// The ordinary case: the whole frame fit inside the scratch.
		return scratch[:n], nil
	case err != nil:
		return nil, err
	}
	// Filled the scratch exactly, so there may be more to come. The read limit
	// bounds what follows, so this cannot be grown without end by a client.
	rest, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(rest) == 0 {
		return scratch, nil
	}
	out := make([]byte, 0, len(scratch)+len(rest))
	out = append(out, scratch...)
	return append(out, rest...), nil
}

// logClose reports a socket that would not close.
//
// Worth a line rather than a discarded error because the two readings are very
// different: a peer that vanished is ordinary and is what this almost always
// is, while a repeated failure here is a descriptor this process is not getting
// back — and at ten thousand connections that is the file-descriptor ceiling,
// reached silently.
func logClose(where string, c *session.Conn, err error) {
	if errors.Is(err, net.ErrClosed) {
		// Already closed by the other half of the connection. Expected: the
		// read loop and the write pump both close on their way out.
		return
	}
	log.Printf("%s: close %s: %v", where, c.ID(), err)
}

func writePump(conn *websocket.Conn, c *session.Conn) {
	ticker := time.NewTicker(pingEvery)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-c.Outbox():
			if !ok {
				// A goodbye nobody is obliged to hear. The session is already
				// over — the caller closes the socket the moment this returns —
				// so a failure here changes nothing and is not worth a line in
				// the log; it is inspected only so that "we do not care" is
				// something this code says rather than something it implies.
				if err := conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye")); err != nil {
					return
				}
				return
			}
			// Without a deadline a peer that has stopped reading parks this
			// goroutine forever, which is the leak writeWait exists to prevent
			// — so failing to set one is a reason to give up the connection,
			// not to write anyway. The same holds for the ping below.
			if err := conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				return
			}
			if err := conn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			if err := conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				return
			}
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
