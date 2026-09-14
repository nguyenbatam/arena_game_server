package replay

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

func header() Header {
	return Header{
		RoomID: "r-test-1", Seed: 99, TickRate: 20, MatchTicks: 100,
		Roster: []sim.Player{{ID: 1}, {ID: 2}, {ID: 1000, Bot: true}},
	}
}

// Record a match the way a room does, then re-simulate it from the file. This
// is the property the whole package rests on: inputs plus a seed reproduce the
// world exactly, so nothing else has to be stored.
func TestRecordedMatchReplaysToTheSameWorld(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir, 4)
	if err != nil {
		t.Fatal(err)
	}
	h := header()
	rec := s.Begin(h)
	if rec == nil {
		t.Fatal("Begin returned nil on an empty store")
	}

	w := sim.NewWorld(h.Seed, h.TickRate, h.MatchTicks, h.Roster)
	var last uint32
	for tick := uint32(1); tick <= 60; tick++ {
		in := map[sim.PlayerID]sim.Input{
			1: {MX: 1, MY: -1, Fire: tick%4 == 0, Aim: int16(tick % 360), Seq: tick},
			2: {MX: -1, Fire: tick%6 == 0, Aim: int16((tick * 7) % 360), Seq: tick, LagTicks: 3},
		}
		snap := w.Step(in)
		rec.Frame(snap.Tick, in)
		last = snap.Tick
	}
	if err := rec.Close(last, w.Checksum()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	loaded, err := Load(filepath.Join(dir, "r-test-1.arnr"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.FinalTick != last {
		t.Fatalf("final tick %d, want %d", loaded.FinalTick, last)
	}
	if len(loaded.Frames) != 60 {
		t.Fatalf("%d frames, want 60", len(loaded.Frames))
	}
	got, ok := loaded.Verify()
	if !ok {
		t.Fatalf("replay diverged: recorded %#x, replayed %#x", loaded.Checksum, got)
	}
}

// A replay that "verifies" whatever it is given would be worse than none: it
// would say a desync was fine.
func TestVerifyCatchesAWrongChecksum(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewStore(dir, 1)
	h := header()
	rec := s.Begin(h)
	rec.Frame(1, map[sim.PlayerID]sim.Input{1: {MX: 1, Seq: 1}})
	if err := rec.Close(1, 0xdeadbeef); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(filepath.Join(dir, "r-test-1.arnr"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := loaded.Verify(); ok {
		t.Fatal("a recording with a bogus checksum verified")
	}
}

// Recording every match at a thousand rooms a node would cost more memory than
// the simulations do, so the sample is bounded — and gives slots back.
func TestSampleIsBoundedAndReleased(t *testing.T) {
	s, _ := NewStore(t.TempDir(), 2)
	a, b := s.Begin(header()), s.Begin(header())
	if a == nil || b == nil {
		t.Fatal("the first two recordings were refused")
	}
	if c := s.Begin(header()); c != nil {
		t.Fatal("a third recording started past the sample limit")
	}
	if err := a.Close(1, 1); err != nil {
		t.Fatal(err)
	}
	if c := s.Begin(header()); c == nil {
		t.Fatal("closing a recording did not give its slot back")
	}
}

// Closing twice must not hand back two slots for one recording.
func TestDoubleCloseReleasesOneSlot(t *testing.T) {
	s, _ := NewStore(t.TempDir(), 1)
	r := s.Begin(header())
	_ = r.Close(1, 1)
	_ = r.Close(1, 1)
	if first := s.Begin(header()); first == nil {
		t.Fatal("the slot was not released")
	}
	if second := s.Begin(header()); second != nil {
		t.Fatal("a double Close released the slot twice")
	}
}

// An unset REPLAY_DIR disables recording, and every call has to stay safe.
func TestNilStoreIsInert(t *testing.T) {
	s, err := NewStore("", 4)
	if err != nil {
		t.Fatal(err)
	}
	if s != nil {
		t.Fatal("an empty dir produced a store")
	}
	if rec := s.Begin(header()); rec != nil {
		t.Fatal("a nil store handed out a recorder")
	}
	var rec *Recorder
	rec.Frame(1, map[sim.PlayerID]sim.Input{1: {}})
	if err := rec.Close(1, 1); err != nil {
		t.Fatalf("closing a nil recorder: %v", err)
	}
}

// Room ids are server-generated today. This is the check that keeps that from
// being load-bearing.
func TestRoomIdCannotEscapeTheReplayDirectory(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewStore(dir, 1)
	h := header()
	h.RoomID = "../../etc/passwd"
	rec := s.Begin(h)
	if err := rec.Close(1, 1); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("%d files written, want 1", len(entries))
	}
	if name := entries[0].Name(); filepath.Ext(name) != ".arnr" || name != "------etc-passwd.arnr" {
		t.Fatalf("wrote %q, which is not a name confined to the replay directory", name)
	}
}

func TestLoadRejectsGarbage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "junk.arnr")
	if err := os.WriteFile(path, []byte("this is not a replay"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("garbage loaded as a replay")
	}
}

// A replay file says how many ticks to simulate, and a file is not to be
// trusted about how much work to do: a corrupt final tick would otherwise turn
// Verify into a four-billion-step loop.
func TestVerifyIgnoresAnAbsurdFinalTick(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewStore(dir, 1)
	h := header() // MatchTicks: 100
	rec := s.Begin(h)
	rec.Frame(1, map[sim.PlayerID]sim.Input{1: {MX: 1, Seq: 1}})
	if err := rec.Close(0xFFFFFFFE, 123); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(filepath.Join(dir, "r-test-1.arnr"))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, ok := loaded.Verify(); ok {
			t.Error("a recording with a bogus final tick verified")
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Verify is still replaying — the final tick was taken at face value")
	}
}
