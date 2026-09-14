package replay

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

func twoPlayers() []sim.Player { return []sim.Player{{ID: 1}, {ID: 2}} }

// record runs a scripted match through a Recorder and loads the file back.
func record(t *testing.T, script func(w *sim.World, rec *Recorder)) *Recording {
	t.Helper()
	dir := t.TempDir()
	store, err := NewStore(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	h := Header{RoomID: "roster", Seed: 77, TickRate: 20, MatchTicks: 300, Roster: twoPlayers()}
	rec := store.Begin(h)
	if rec == nil {
		t.Fatal("recording refused on an empty store")
	}
	w := sim.NewWorld(h.Seed, h.TickRate, h.MatchTicks, h.Roster)
	script(w, rec)
	if err := rec.Close(w.Snapshot().Tick, w.Checksum()); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(filepath.Join(dir, "roster.arnr"))
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

// A match somebody left must replay to the world that was actually played.
//
// A departure is a change to the simulation that no input carries, so a
// recording of inputs alone re-simulates a different match: the avatar stays in
// play, keeps being shot, and the checksum disagrees with the one the server
// stamped — a desync reported for a match that never desynced.
func TestAMatchWithADepartureStillVerifies(t *testing.T) {
	rec := record(t, func(w *sim.World, r *Recorder) {
		for tick := uint32(1); tick <= 200; tick++ {
			if tick == 40 {
				w.Depart(2)
				r.Depart(2)
			}
			in := map[sim.PlayerID]sim.Input{
				1: {MX: 1, Fire: tick%3 == 0, Aim: int16(tick % 360), Seq: tick},
			}
			w.Step(in)
			r.Frame(tick, in)
		}
	})

	if got, ok := rec.Verify(); !ok {
		t.Fatalf("a match with a departure replayed to %#016x, recorded %#016x", got, rec.Checksum)
	}
}

// The same, with the player coming back.
func TestADepartureAndRejoinRoundTrip(t *testing.T) {
	rec := record(t, func(w *sim.World, r *Recorder) {
		for tick := uint32(1); tick <= 200; tick++ {
			switch tick {
			case 30:
				w.Depart(2)
				r.Depart(2)
			case 90:
				w.Rejoin(2)
				r.Rejoin(2)
			}
			in := map[sim.PlayerID]sim.Input{1: {MX: 1, Fire: tick%4 == 0, Seq: tick}}
			w.Step(in)
			r.Frame(tick, in)
		}
	})

	var departs, rejoins int
	for _, f := range rec.Frames {
		for _, e := range f.Events {
			switch e.Kind {
			case EventDepart:
				departs++
			case EventRejoin:
				rejoins++
			}
		}
	}
	if departs != 1 || rejoins != 1 {
		t.Fatalf("read back %d departures and %d rejoins, want 1 and 1", departs, rejoins)
	}
	if got, ok := rec.Verify(); !ok {
		t.Fatalf("replay reached %#016x, recorded %#016x", got, rec.Checksum)
	}
}

// A tick where nothing was pressed but somebody left still has to be written.
// The frame was skipped entirely when the input map was empty, which would have
// dropped the departure on the floor.
func TestAFrameWithOnlyARosterEventIsStillWritten(t *testing.T) {
	rec := record(t, func(w *sim.World, r *Recorder) {
		for tick := uint32(1); tick <= 10; tick++ {
			if tick == 5 {
				w.Depart(2)
				r.Depart(2)
			}
			w.Step(nil) // nobody pressed anything, all match
			r.Frame(tick, nil)
		}
	})

	if len(rec.Frames) != 1 {
		t.Fatalf("wrote %d frames, want exactly the one carrying the departure", len(rec.Frames))
	}
	f := rec.Frames[0]
	if f.Tick != 5 || len(f.Events) != 1 || f.Events[0].PlayerID != 2 || f.Events[0].Kind != EventDepart {
		t.Fatalf("frame is %+v, want a lone departure of seat 2 on tick 5", f)
	}
	if got, ok := rec.Verify(); !ok {
		t.Fatalf("replay reached %#016x, recorded %#016x", got, rec.Checksum)
	}
}

// Events are applied before the tick they are recorded on, which is the order
// the room applies them in: it drains commands, then advances the world.
func TestEventsApplyBeforeTheTickTheyAreRecordedOn(t *testing.T) {
	rec := record(t, func(w *sim.World, r *Recorder) {
		for tick := uint32(1); tick <= 3; tick++ {
			if tick == 2 {
				w.Depart(2)
				r.Depart(2)
			}
			in := map[sim.PlayerID]sim.Input{1: {MX: 1, Seq: tick}}
			w.Step(in)
			r.Frame(tick, in)
		}
	})
	for _, f := range rec.Frames {
		if len(f.Events) == 0 {
			continue
		}
		if f.Tick != 2 {
			t.Fatalf("the departure was recorded on tick %d, want 2", f.Tick)
		}
	}
	if got, ok := rec.Verify(); !ok {
		t.Fatalf("replay reached %#016x, recorded %#016x", got, rec.Checksum)
	}
}

// v1 files still load. They were written by a build where leaving changed
// nothing, so no event list is exactly right for them — and reading one where
// there is none would consume the next frame's tick and corrupt everything
// after it.
func TestAVersionOneRecordingStillLoads(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString(Magic)
	put16 := func(v uint16) { buf.Write(binary.BigEndian.AppendUint16(nil, v)) }
	put32 := func(v uint32) { buf.Write(binary.BigEndian.AppendUint32(nil, v)) }
	put64 := func(v uint64) { buf.Write(binary.BigEndian.AppendUint64(nil, v)) }

	put16(1) // format v1
	put16(20)
	put32(300)
	put64(uint64(int64(77)))
	put16(2) // roster
	for _, id := range []uint32{1, 2} {
		put32(id)
		buf.WriteByte(0)
	}
	// Two frames, back to back, with no event count between them. A v2 reader
	// that does not gate on the version reads the second frame's tick as an
	// event count here and everything after it is nonsense.
	for _, tick := range []uint32{1, 2} {
		put32(tick)
		put16(1) // one input
		put32(1) // player 1
		buf.WriteByte(1)
		buf.WriteByte(0)
		buf.WriteByte(0)
		put16(0)
		put32(tick)
		buf.WriteByte(0)
	}
	put32(0xFFFFFFFF)
	put32(2)
	put64(0xDEADBEEF)

	rec, err := Read(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("a v1 recording no longer loads: %v", err)
	}
	if rec.Version != 1 {
		t.Fatalf("version = %d, want 1", rec.Version)
	}
	if len(rec.Frames) != 2 {
		t.Fatalf("read %d frames, want 2 — the frame boundary was misread", len(rec.Frames))
	}
	for i, f := range rec.Frames {
		if f.Tick != uint32(i+1) {
			t.Fatalf("frame %d is tick %d, want %d", i, f.Tick, i+1)
		}
		if len(f.Events) != 0 {
			t.Fatalf("frame %d invented %d events", i, len(f.Events))
		}
	}
	if rec.FinalTick != 2 || rec.Checksum != 0xDEADBEEF {
		t.Fatalf("trailer read back as tick=%d checksum=%#x", rec.FinalTick, rec.Checksum)
	}
}

// A format this build does not know is refused rather than misparsed.
func TestAnUnknownFormatIsRefused(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString(Magic)
	buf.Write(binary.BigEndian.AppendUint16(nil, Format+1))
	if _, err := Read(bytes.NewReader(buf.Bytes())); err == nil {
		t.Fatal("a recording from a newer build was accepted")
	}
}

// Recordings this build writes are v2, so the field is there to be read.
func TestRecordingsAreWrittenAtTheCurrentFormat(t *testing.T) {
	rec := record(t, func(w *sim.World, r *Recorder) {
		in := map[sim.PlayerID]sim.Input{1: {MX: 1, Seq: 1}}
		w.Step(in)
		r.Frame(1, in)
	})
	if rec.Version != Format {
		t.Fatalf("wrote format v%d, want v%d", rec.Version, Format)
	}
}
