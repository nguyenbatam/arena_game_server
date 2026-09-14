package tcp

import (
	"bytes"
	"encoding/binary"
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
)

func framed(t *testing.T, payloads ...[]byte) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	for _, p := range payloads {
		if _, err := writeFrame(&buf, nil, p); err != nil {
			t.Fatal(err)
		}
	}
	return &buf
}

// Frames that fit are read into the connection's own buffer, over and over.
// The allocation this removes is one per message per connection — 200k a second
// at ten thousand players and the tick rate — so "the same backing array"
// rather than "equal bytes" is the assertion that matters.
func TestReadFrameReusesTheScratchBuffer(t *testing.T) {
	r := framed(t, []byte("first frame"), []byte("second"))
	scratch := make([]byte, readScratch)

	a, err := readFrame(r, scratch)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != "first frame" {
		t.Fatalf("got %q", a)
	}
	if &a[0] != &scratch[0] {
		t.Fatal("a frame that fits must be read into the scratch buffer, not a fresh allocation")
	}

	b, err := readFrame(r, scratch)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "second" {
		t.Fatalf("got %q", b)
	}
	if &b[0] != &scratch[0] {
		t.Fatal("the second frame allocated instead of reusing the buffer")
	}
	// And the length is the frame's, not whatever the previous one left behind.
	if len(b) != len("second") {
		t.Fatalf("len = %d, want %d", len(b), len("second"))
	}
}

// A frame too big for the scratch buffer gets its own allocation and is not
// kept. Growing the buffer instead would mean one 64 KB client costing every
// connection on the process 64 KB for the rest of its life.
func TestOversizeFrameGetsItsOwnBufferAndTheScratchIsUnchanged(t *testing.T) {
	big := bytes.Repeat([]byte("z"), readScratch+1)
	r := framed(t, big, []byte("small again"))
	scratch := make([]byte, readScratch)

	got, err := readFrame(r, scratch)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, big) {
		t.Fatalf("oversize frame came back wrong: %d bytes", len(got))
	}
	if &got[0] == &scratch[0] {
		t.Fatal("a frame larger than the scratch buffer must not be read into it")
	}
	if len(scratch) != readScratch || cap(scratch) != readScratch {
		t.Fatalf("the scratch buffer grew to len=%d cap=%d", len(scratch), cap(scratch))
	}

	next, err := readFrame(r, scratch)
	if err != nil {
		t.Fatal(err)
	}
	if string(next) != "small again" || &next[0] != &scratch[0] {
		t.Fatalf("the connection did not go back to its buffer after an oversize frame: %q", next)
	}
}

// An empty frame is legal on this wire and must stay legal: the reuse change
// turns `make([]byte, 0)` into a zero-length slice of the scratch buffer, and
// the two have to behave the same.
func TestEmptyFrameStillRoundTrips(t *testing.T) {
	r := framed(t, nil, []byte("after"))
	scratch := make([]byte, readScratch)

	got, err := readFrame(r, scratch)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("empty frame came back as %d bytes", len(got))
	}
	next, err := readFrame(r, scratch)
	if err != nil || string(next) != "after" {
		t.Fatalf("frame after an empty one: %q, %v", next, err)
	}
}

// Reusing the read buffer is only safe because unmarshalling copies. This is
// the assumption Handler's contract rests on, and it belongs to the protobuf
// runtime rather than to this package — which is exactly why it is pinned here
// instead of assumed: a build that started decoding lazily would alias the
// buffer, and the symptom would be envelopes whose strings turn into the next
// message's bytes, intermittently, under load.
func TestUnmarshalDoesNotAliasTheReadBuffer(t *testing.T) {
	raw := protocol.Env(pb.MsgType_MSG_TYPE_HELLO, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Hello{Hello: &pb.Hello{
			Name: "alice", SessionId: "sess-1", AccessToken: "token-1",
			AddrToken: []byte("addr-token"), ProtocolVersion: 1,
		}}
	})
	buf := make([]byte, len(raw))
	copy(buf, raw)

	env, err := protocol.UnmarshalEnv(buf)
	if err != nil {
		t.Fatal(err)
	}
	// The read loop's next frame lands here.
	for i := range buf {
		buf[i] = 0xff
	}

	h := env.GetHello()
	if h == nil {
		t.Fatal("hello went missing")
	}
	if h.Name != "alice" || h.SessionId != "sess-1" || h.AccessToken != "token-1" {
		t.Fatalf("strings aliased the buffer: %+v", h)
	}
	if string(h.AddrToken) != "addr-token" {
		t.Fatalf("bytes field aliased the buffer: %q", h.AddrToken)
	}
	if h.ProtocolVersion != 1 {
		t.Fatalf("protocol version = %d", h.ProtocolVersion)
	}
}

// The length prefix is still checked before anything is sized from it.
func TestOversizedLengthIsRefusedBeforeAllocating(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], maxFrame+1)
	if _, err := readFrame(bytes.NewReader(hdr[:]), make([]byte, readScratch)); err == nil {
		t.Fatal("oversized frame accepted")
	}
}
