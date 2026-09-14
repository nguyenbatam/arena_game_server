package ws

import (
	"bytes"
	"io"
	"testing"
)

// TestReadFrameHandlesEverySizeAroundTheScratch pins the three cases that
// differ, because only the first of them is exercised by an ordinary message
// and the other two are where a hand-rolled read goes wrong: a frame that ends
// exactly on the buffer boundary, and one that runs past it.
func TestReadFrameHandlesEverySizeAroundTheScratch(t *testing.T) {
	scratch := make([]byte, readScratch)
	for _, n := range []int{0, 1, 37, readScratch - 1, readScratch, readScratch + 1, 3 * readScratch} {
		want := make([]byte, n)
		for i := range want {
			// A pattern rather than zeroes, so a short read or a lost tail is
			// visible rather than passing as a correct buffer of nulls.
			want[i] = byte(i%251 + 1)
		}
		got, err := readFrame(bytes.NewReader(want), scratch)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("n=%d: got %d bytes, want %d (equal=%v)", n, len(got), n, bytes.Equal(got, want))
		}
	}
}

// TestReadFrameReusesTheScratchForOrdinaryTraffic is the whole point of the
// function: a message that fits must not allocate. An input at the tick rate is
// tens of bytes, so this is the case that runs 200k times a second at the load
// this server is sized for.
func TestReadFrameReusesTheScratchForOrdinaryTraffic(t *testing.T) {
	scratch := make([]byte, readScratch)
	msg := bytes.Repeat([]byte{7}, 64)
	// Reset rather than a fresh reader per run: bytes.NewReader allocates, and
	// measuring it here would report the test's own garbage as readFrame's.
	r := bytes.NewReader(nil)
	allocs := testing.AllocsPerRun(100, func() {
		r.Reset(msg)
		if _, err := readFrame(r, scratch); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 0 {
		t.Fatalf("readFrame allocated %.1f times for a %d-byte frame; the scratch is there so it does not", allocs, len(msg))
	}
}

// errReader fails partway through, which is what a socket that dies mid-frame
// looks like from in here.
type errReader struct {
	data []byte
	err  error
}

func (r *errReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

// TestReadFrameReportsAPartialFrame: a read error must not be returned as a
// short-but-valid message. The handler unmarshals whatever it is handed, and a
// truncated protobuf that happens to parse is worse than a closed connection.
func TestReadFrameReportsAPartialFrame(t *testing.T) {
	scratch := make([]byte, readScratch)
	// Fills the scratch exactly, so the function goes on to read the rest and
	// fails there — the path that cannot be reached with a smaller frame.
	r := &errReader{data: bytes.Repeat([]byte{1}, readScratch), err: io.ErrClosedPipe}
	if _, err := readFrame(r, scratch); err == nil {
		t.Fatal("want the read error, got a message")
	}
}
