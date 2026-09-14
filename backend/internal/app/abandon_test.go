package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/placement"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/replay"
	"github.com/nguyenbatam/arena_game_server/internal/room"
	"github.com/nguyenbatam/arena_game_server/internal/session"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

// watcher reads a connection's outbox and reports the HP of a seat as the
// snapshots describe it.
//
// The world belongs to the room's tick goroutine, so this is the only view a
// test is allowed to take. The watching connection never acks, so every message
// it is sent is a full snapshot and carries every field.
type watcher struct {
	msgs   chan []byte
	latest map[uint32]int32
}

func watch(c *session.Conn) *watcher {
	w := &watcher{msgs: make(chan []byte, 512), latest: map[uint32]int32{}}
	go func() {
		for msg := range c.Outbox() {
			select {
			case w.msgs <- msg:
			default: // a test that has stopped reading is not a failure
			}
		}
	}()
	return w
}

func (w *watcher) hp(t *testing.T, seat uint32) int32 {
	t.Helper()
	for {
		select {
		case msg := <-w.msgs:
			e, err := protocol.UnmarshalEnv(msg)
			if err != nil {
				continue
			}
			s := e.GetSnapshot()
			if s == nil || s.BaselineTick != 0 {
				continue
			}
			for _, p := range s.Players {
				w.latest[p.Id] = p.Hp
			}
		default:
			if v, ok := w.latest[seat]; ok {
				return v
			}
			return -1
		}
	}
}

// await polls until the seat's HP satisfies want, or the budget runs out.
func (w *watcher) await(t *testing.T, seat uint32, want func(int32) bool) int32 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := w.hp(t, seat)
		if want(got) || time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// seatTwo opens a long match on this node with two connected seats and joins
// both, returning a watcher on the seat that stays.
func seatTwo(t *testing.T, a *App, roomID string) (*watcher, *session.Conn) {
	t.Helper()
	c1 := session.NewConn("stay-"+roomID, 512)
	c2 := session.NewConn("goes-"+roomID, 512)
	w := watch(c1)
	drain(c2)
	a.hub.Add(c1)
	a.hub.Add(c2)

	req := &pb.RoomRequest{
		RoomId: roomID, Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Seats: []*pb.Seat{
			{ConnId: c1.ID(), PlayerId: 1},
			{ConnId: c2.ID(), PlayerId: 2},
		},
	}
	a.startRoom(placement.NewJob(req))
	a.joinRoom(c1, roomID, 1)
	a.joinRoom(c2, roomID, 2)

	r := a.rooms.Get(roomID)
	if r == nil {
		t.Fatal("the room did not start")
	}
	t.Cleanup(r.Stop)
	return w, c2
}

// A disconnect has to take the player out of the match, not merely off the
// mailing list.
//
// Unsubscribe stopped the snapshots and left the avatar standing: a motionless
// target worth a kill every RespawnTicks to whoever shot it, for the rest of
// the match — and recordResult writes those kills into the ladder, where
// nothing can tell a farmed one from a real one.
func TestADisconnectTakesTheAvatarOutOfTheMatch(t *testing.T) {
	a := testApp(t, time.Minute)
	w, leaver := seatTwo(t, a, "r-abandon")

	a.onClose(leaver)

	if got := w.await(t, 2, func(hp int32) bool { return hp == 0 }); got != 0 {
		t.Fatalf("the departed seat is still in play with %d HP", got)
	}
}

// And a reconnect inside the grace window puts them back, or the fix would
// simply have moved the bug: every player who dropped for a second would be out
// of a match they could still play.
func TestAReconnectPutsTheAvatarBackInTheMatch(t *testing.T) {
	a := testApp(t, time.Minute)
	w, leaver := seatTwo(t, a, "r-return")

	a.hub.Remove(leaver)
	a.onClose(leaver)
	if got := w.await(t, 2, func(hp int32) bool { return hp == 0 }); got != 0 {
		t.Fatalf("setup: the seat never departed, HP %d", got)
	}

	// A reconnect comes back under the same durable id — which is what the seat
	// map in the room is keyed on, and what makes it the same player rather
	// than a stranger asking for somebody else's seat.
	back := session.NewConn(leaver.ID(), 512)
	drain(back)
	a.hub.Add(back)
	a.joinRoom(back, "r-return", 2)

	if got := w.await(t, 2, func(hp int32) bool { return hp > 0 }); got <= 0 {
		t.Fatalf("the reconnected seat did not come back: HP %d", got)
	}
}

// A connection that has been replaced must not carry its successor's seat out
// of the match with it. onClose already declines to touch anything for a
// superseded conn; this pins that the departure follows the same rule.
func TestASupersededConnDoesNotRemoveTheSeatItLostTo(t *testing.T) {
	a := testApp(t, time.Minute)
	w, old := seatTwo(t, a, "r-superseded")

	// A reconnect files a new conn under the same durable id, which is what
	// makes the old one superseded.
	replacement := session.NewConn(old.ID(), 512)
	drain(replacement)
	a.hub.Add(replacement)
	a.joinRoom(replacement, "r-superseded", 2)

	// The dead socket only now notices.
	a.onClose(old)

	// Long enough that a departure would have landed and its linger expired.
	time.Sleep(time.Second)
	if got := w.hp(t, 2); got <= 0 {
		t.Fatalf("a late close of a replaced connection removed the live seat: HP %d", got)
	}
}

// A handoff is not a departure: the player is on their way to another node with
// the same seat, and taking their avatar out would end their match for them.
func TestAHandoffDoesNotRemoveTheAvatar(t *testing.T) {
	a := testApp(t, time.Minute)
	w, moving := seatTwo(t, a, "r-handoff")

	moving.SetHandoff(true)
	a.onClose(moving)

	time.Sleep(time.Second)
	if got := w.hp(t, 2); got <= 0 {
		t.Fatalf("a handoff took the seat out of the match: HP %d", got)
	}
}

// The gateway has to actually hand the room its netcode settings. testApp
// builds a config.Static with everything zeroed, so without a test that sets
// them the wiring from config to room.Params is never exercised — and a field
// that is read from the wrong place looks exactly like one that works.
func TestTheGatewayPassesItsNetcodeSettingsToTheRoom(t *testing.T) {
	a := testApp(t, time.Minute)
	a.cfg.WarmupTimeout = 150 * time.Millisecond
	a.cfg.SnapshotRate = 20

	c := session.NewConn("solo", 512)
	w := watch(c)
	a.hub.Add(c)

	req := &pb.RoomRequest{
		RoomId: "r-wired", Seed: 1, TickRate: 60, MatchTicks: 1 << 20,
		Seats: []*pb.Seat{
			{ConnId: c.ID(), PlayerId: 1},
			{ConnId: "never-arrives", PlayerId: 2},
		},
	}
	a.startRoom(placement.NewJob(req))
	r := a.rooms.Get("r-wired")
	if r == nil {
		t.Fatal("the room did not start")
	}
	t.Cleanup(r.Stop)
	a.joinRoom(c, "r-wired", 1)

	// The second seat never turns up, so the warmup ends on its budget rather
	// than on the join — which is the half that proves the timeout was passed
	// through rather than ignored.
	if got := w.await(t, 1, func(hp int32) bool { return hp > 0 }); got <= 0 {
		t.Fatalf("the match never started: seat HP %d", got)
	}
	if last := r.LastTick(); last == 0 {
		t.Fatal("the room reported no broadcast tick")
	}
}

// Shutdown has to wait for the rooms it stopped, or everything a match does on
// its way out is lost to the process exiting.
//
// Room.Stop only closes a channel. The tick loop then unwinds and runs its
// deferred work — closeRecorder writes the replay file — on the room's own
// goroutine, which nothing used to wait for. Observed against the real server:
// a SIGTERM during a match left zero-byte .arnr files behind, because
// os.WriteFile had created the file and the process died before the bytes
// landed. cmd/replay reports one of those as an error, which is worse than the
// recording simply not existing.
func TestShutdownWaitsForRoomsToUnwind(t *testing.T) {
	a := testApp(t, time.Minute)
	a.cfg.DrainTimeout = 50 * time.Millisecond
	a.cfg.ShutdownTimeout = 5 * time.Second

	dir := t.TempDir()
	store, err := replay.NewStore(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	roster := room.RosterFromIDs([]uint32{1}, 1)
	rec := store.Begin(replay.Header{
		RoomID: "r-shutdown", Seed: 1, TickRate: 50, MatchTicks: 1 << 20, Roster: roster,
	})
	if rec == nil {
		t.Fatal("recording refused on an empty store")
	}

	a.rooms.Start(context.Background(), room.Params{
		ID: "r-shutdown", Seed: 1, TickRate: 50, MatchTicks: 1 << 20,
		Roster: roster, Recorder: rec,
	})
	if a.rooms.Count() != 1 {
		t.Fatal("the room did not start")
	}
	// Let it simulate a little, so the recording has something in it.
	time.Sleep(100 * time.Millisecond)

	if err := a.shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// By the time shutdown returns the room must already be gone, not merely
	// asked to stop.
	if n := a.rooms.Count(); n != 0 {
		t.Fatalf("shutdown returned with %d rooms still unwinding", n)
	}
	info, err := os.Stat(filepath.Join(dir, "r-shutdown.arnr"))
	if err != nil {
		t.Fatalf("no recording was written: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("the recording is zero bytes: the process would have exited mid-write")
	}
	if _, err := replay.Load(filepath.Join(dir, "r-shutdown.arnr")); err != nil {
		t.Fatalf("the recording shutdown left behind does not load: %v", err)
	}
}

// A node must get its room slot back when the last player walks out, not when
// the match clock would have run down.
//
// With the avatar despawned but the match still running, a room of bots plays
// out the remaining MATCH_SECONDS for an audience of nobody — counted against
// MAX_ROOMS the whole time, so placement keeps refusing matches that have
// players in them on account of one that does not.
func TestAnAbandonedMatchFreesTheNodeImmediately(t *testing.T) {
	a := testApp(t, time.Minute)

	ended := make(chan sim.Snapshot, 1)
	c := session.NewConn("walks-out", 512)
	drain(c)
	a.hub.Add(c)

	req := &pb.RoomRequest{
		// Long enough that the match clock is plainly not what ends it.
		RoomId: "r-empty", Seed: 1, TickRate: 100, MatchTicks: 1 << 20,
		Seats: []*pb.Seat{{ConnId: c.ID(), PlayerId: 1}}, Bots: 3,
	}
	a.rooms.Start(context.Background(), room.Params{
		ID: req.RoomId, Seed: req.Seed, TickRate: int(req.TickRate), MatchTicks: req.MatchTicks,
		Roster: room.RosterFromIDs([]uint32{1}, 3),
		Seats:  map[string]uint32{c.ID(): 1},
		OnEnd:  func(s sim.Snapshot) { ended <- s },
	})
	a.joinRoom(c, req.RoomId, 1)

	a.hub.Remove(c)
	a.onClose(c)

	select {
	case snap := <-ended:
		if !snap.Ended {
			t.Fatal("the room reported an end that was not an end")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the match kept running with nobody in it")
	}

	deadline := time.Now().Add(2 * time.Second)
	for a.rooms.Count() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := a.rooms.Count(); n != 0 {
		t.Fatalf("%d rooms still held after the last player left", n)
	}
}
