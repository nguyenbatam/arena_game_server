package room

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/replay"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

// The end-to-end shape: a real room records what it simulated, and the file it
// leaves behind re-simulates to the same world. If this holds, a bug report can
// arrive as a file rather than as a description of what someone thought they
// saw.
func TestRoomRecordsAReplayThatVerifies(t *testing.T) {
	dir := t.TempDir()
	store, err := replay.NewStore(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	roster := RosterFromIDs([]uint32{1, 2}, 1)
	rec := store.Begin(replay.Header{
		RoomID: "rec-room", Seed: 4242, TickRate: 60, MatchTicks: 40, Roster: roster,
	})
	if rec == nil {
		t.Fatal("recording was refused on an empty store")
	}

	ended := make(chan sim.Snapshot, 1)
	r := New(Params{
		ID: "rec-room", Seed: 4242, TickRate: 60, MatchTicks: 40,
		Roster: roster, Recorder: rec,
		OnEnd: func(s sim.Snapshot) { ended <- s },
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	// Feed real input while the match runs, so the recording has frames in it
	// rather than an empty world ticking over.
	go func() {
		for i := uint32(1); i <= 30; i++ {
			r.SubmitInput(&pb.Input{PlayerId: 1, Seq: i, Mx: 1, Aim: int32(i * 7 % 360), Fire: i%3 == 0})
			r.SubmitInput(&pb.Input{PlayerId: 2, Seq: i, My: -1, Aim: int32(i * 13 % 360)})
			time.Sleep(5 * time.Millisecond)
		}
	}()

	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the match never ended")
	}

	// Run's deferred close runs just after OnEnd, so give the file a moment.
	path := filepath.Join(dir, "rec-room.arnr")
	var loaded *replay.Recording
	for i := 0; i < 100; i++ {
		loaded, err = replay.Load(path)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("no replay was written: %v", err)
	}
	if len(loaded.Frames) == 0 {
		t.Fatal("the recording holds no input frames")
	}
	if got, ok := loaded.Verify(); !ok {
		t.Fatalf("replaying the match gave %#x, the server recorded %#x", got, loaded.Checksum)
	}
}

// A room that is stopped rather than finished must still leave its recording
// behind — and must give the sample slot back, or a node that restarts rooms
// stops recording after the first few.
func TestStoppedRoomStillClosesItsRecording(t *testing.T) {
	dir := t.TempDir()
	store, _ := replay.NewStore(dir, 1)
	roster := RosterFromIDs([]uint32{1, 2}, 0)
	rec := store.Begin(replay.Header{RoomID: "stopped", Seed: 1, TickRate: 60, MatchTicks: 100_000, Roster: roster})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := New(Params{ID: "stopped", Seed: 1, TickRate: 60, MatchTicks: 100_000, Roster: roster, Recorder: rec})
	go r.Run(ctx)
	time.Sleep(100 * time.Millisecond)
	r.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := replay.Load(filepath.Join(dir, "stopped.arnr")); err == nil {
			if next := store.Begin(replay.Header{RoomID: "next", Seed: 2, TickRate: 20, Roster: roster}); next == nil {
				t.Fatal("the sample slot was not released when the room stopped")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("a stopped room left no recording behind")
}
