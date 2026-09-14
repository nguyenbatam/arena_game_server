package tcp

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/safe"
	"github.com/nguyenbatam/arena_game_server/internal/session"
)

// writeWait bounds a single frame write. Without it a client that stops reading
// parks this connection's writer goroutine forever: the send channel drops
// messages under backpressure, but the goroutine blocked in Write is never
// reclaimed, so a few thousand dead peers leak a few thousand goroutines.
const writeWait = 10 * time.Second

// readWait is how long a connection may stay silent. Clients send input at the
// tick rate, so silence means gone.
const readWait = 60 * time.Second

// maxFrame caps one message. Same ceiling the WebSocket transport sets.
const maxFrame = 1 << 16

// readScratch is how big a buffer each connection keeps for reading frames
// into.
//
// Every frame used to be read into an allocation of its own: one per message
// per connection, which at the tick rate and ten thousand players is 200k a
// second of garbage whose whole life is to be handed to the handler and
// unmarshalled. The write half already keeps a scratch buffer for exactly this
// reason — see writeFrame — and this is the read half of the same argument.
//
// Sized for the traffic and not for maxFrame. An input is tens of bytes and a
// HELLO a couple of hundred, so this covers the hot path many times over, and a
// frame that does not fit gets an allocation of its own rather than growing the
// buffer: keeping 64 KB per connection because one client once sent a large
// frame is how a read buffer turns into a memory leak — at ten thousand
// connections it is 640 MB of it.
const readScratch = 2048

// Handler is called with the bytes of one message.
//
// msg is only valid for the duration of the call: it is the connection's read
// buffer and the next frame is read over it. A handler that wants to keep the
// bytes must copy them. Unmarshalling is not keeping them — protobuf copies
// every string and bytes field out on the way in, which is what makes the reuse
// above safe and is pinned by TestUnmarshalDoesNotAliasTheReadBuffer.
type Handler func(c *session.Conn, msg []byte)

func Serve(ln net.Listener, hub *session.Hub, sendBuf session.BufSize, onMsg Handler, onClose func(*session.Conn)) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		safe.Go("tcp.conn", func() { handle(conn, hub, sendBuf, onMsg, onClose) })
	}
}

func handle(nc net.Conn, hub *session.Hub, sendBuf session.BufSize, onMsg Handler, onClose func(*session.Conn)) {
	defer nc.Close()
	c := session.NewConn(session.NewID(), sendBuf())
	// Every rate limiter in the gateway is keyed on this, and an empty key is
	// treated as "no key, allow" — so leaving it unset does not merely lose a
	// label, it switches rate limiting off for the whole transport.
	if host, _, err := net.SplitHostPort(nc.RemoteAddr().String()); err == nil {
		c.RemoteIP = host
	} else {
		c.RemoteIP = nc.RemoteAddr().String()
	}
	hub.Add(c)
	defer func() {
		hub.Remove(c)
		onClose(c)
		c.Close()
	}()

	safe.Go("tcp.write", func() {
		defer nc.Close()
		// One scratch buffer for the life of the connection. This goroutine is
		// the only writer, so the buffer it hands to writeFrame cannot be
		// observed by anyone else, and a snapshot every 50ms per connection is
		// an allocation per snapshot the transport does not need to make.
		var scratch []byte
		for msg := range c.Outbox() {
			// A deadline that cannot be set is a reason to give up the
			// connection, not to write anyway: without one the Write below
			// parks this goroutine forever against a peer that has stopped
			// reading, which is the leak writeWait exists to prevent.
			if err := nc.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				return
			}
			var err error
			if scratch, err = writeFrame(nc, scratch, msg); err != nil {
				return
			}
		}
	})

	// Buffered so a 4-byte header and its body are not two separate reads.
	br := bufio.NewReaderSize(nc, 4096)
	// One scratch buffer for the life of the connection. This goroutine is the
	// only reader and the handler is not allowed to keep what it is given, so
	// there is nobody to share it with. See readScratch.
	scratch := make([]byte, readScratch)
	for {
		// Same argument as the write half: an unbounded read is a goroutine and
		// a session this process never gets back. It only fails on a socket
		// that is already finished, so there is nothing to salvage.
		if err := nc.SetReadDeadline(time.Now().Add(readWait)); err != nil {
			return
		}
		msg, err := readFrame(br, scratch)
		if err != nil {
			return
		}
		onMsg(c, msg)
	}
}

// readFrame reads one message into scratch, or into a fresh buffer when the
// frame is too big for it. The returned slice is only valid until the next call
// — see Handler.
func readFrame(r io.Reader, scratch []byte) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxFrame {
		return nil, fmt.Errorf("frame too large: %d", n)
	}
	buf := scratch[:0]
	if int(n) > cap(buf) {
		buf = make([]byte, n)
	} else {
		buf = buf[:n]
	}
	_, err := io.ReadFull(r, buf)
	return buf, err
}

// writeFrame emits the header and body in one Write. Two writes is two
// syscalls per snapshot per client, and on a tick loop that is the difference
// worth removing.
//
// buf is the caller's scratch space, returned grown so the next frame can reuse
// it. Building the combined header+body in a fresh allocation each time cost
// one allocation per snapshot per connection — at 20 Hz and 10k connections,
// 200k a second of garbage whose only purpose was to be handed to Write and
// dropped. The caller owns the buffer and must be the connection's only writer.
func writeFrame(w io.Writer, buf, msg []byte) ([]byte, error) {
	n := 4 + len(msg)
	if cap(buf) < n {
		buf = make([]byte, n)
	}
	out := buf[:n]
	binary.BigEndian.PutUint32(out[:4], uint32(len(msg)))
	copy(out[4:], msg)
	_, err := w.Write(out)
	return out, err
}
