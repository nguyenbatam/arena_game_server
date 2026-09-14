package room

import (
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/config"
	"github.com/nguyenbatam/arena_game_server/internal/net/udp"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

// defaultRoomSize mirrors the ROOM_SIZE default in internal/config. Kept here
// as a literal on purpose: this file is about what the wire format can carry,
// which must not quietly change because someone edited a config default.
const defaultRoomSize = 8

// minSupportedRoomSize is the room size the encoding must keep serving without
// interest management. It is a floor on the threshold, not a target: if adding
// a field to PlayerSnap pushes the break-even point below this, the wire format
// grew enough to matter and someone should know.
const minSupportedRoomSize = 24

// midMatchSnapshot runs a room far enough in that projectiles are in flight and
// everyone has moved, which is the realistic worst case for snapshot size —
// measuring at spawn would flatter the numbers.
func midMatchSnapshot(t *testing.T, players int) (full, delta int) {
	t.Helper()
	roster := make([]sim.Player, 0, players)
	for i := 1; i <= players; i++ {
		roster = append(roster, sim.Player{ID: sim.PlayerID(i), Bot: i > 2})
	}
	r := New(Params{ID: "size", Seed: 1, TickRate: 20, MatchTicks: 100000, Roster: roster})

	pending := make(map[sim.PlayerID]sim.Input, players)
	var prev *pb.Snapshot
	for tick := 0; tick < 40; tick++ {
		for i := 1; i <= players; i++ {
			pending[sim.PlayerID(i)] = sim.Input{
				MX: int8(i%3) - 1, MY: int8((i/2)%3) - 1,
				Fire: (tick+i)%6 == 0, Aim: int16((tick * i) % 360), Seq: uint32(tick + 1),
			}
		}
		snap := r.world.Step(pending)
		clear(pending)

		cur := r.encodeState(snap)
		full = protocol.Sizeof(cur)
		if prev != nil {
			delta = protocol.Sizeof(deltaSnapshot(prev, cur))
		}
		prev = cur
	}
	return full, delta
}

// A snapshot has to fit in one datagram. Past that the UDP transport drops it
// rather than fragment — losing one fragment loses the whole packet — so this
// is a hard wall, not a bandwidth preference.
func TestSnapshotFitsOneDatagramAtDefaultRoomSize(t *testing.T) {
	full, _ := midMatchSnapshot(t, defaultRoomSize)
	if full > udp.MaxDatagram {
		t.Fatalf("snapshot is %dB at %d players, over the %dB datagram limit",
			full, defaultRoomSize, udp.MaxDatagram)
	}
	if full > udp.MaxDatagram/2 {
		t.Errorf("snapshot is %dB, more than half the %dB budget at only %d players — "+
			"headroom for projectile-heavy moments is thin",
			full, udp.MaxDatagram, defaultRoomSize)
	}
}

// This is the answer to "when do we need interest management": the room size
// where a full snapshot stops fitting one datagram. Everything about family D —
// per-client relevance sets, per-client encoding — is the price of going past
// it, so it is worth knowing exactly where it sits.
func TestSnapshotSizeByRoomSize(t *testing.T) {
	sizes := []int{8, 16, 24, 32, 48, 64, 100, 150}

	threshold := 0
	t.Logf("%6s %11s %12s %10s %14s", "players", "full/tick", "delta/tick", "B/player", "egress/room")
	for _, n := range sizes {
		full, delta := midMatchSnapshot(t, n)

		// Every client receives the state of every other, so room egress grows
		// with the square of the roster. That quadratic, not the per-snapshot
		// size, is what eventually forces interest management.
		egressKBs := full * 20 * n / 1024

		note := ""
		if full > udp.MaxDatagram {
			note = "  ← vượt MTU"
			if threshold == 0 {
				threshold = n
			}
		}
		t.Logf("%6d %10dB %11dB %10.1f %10d KB/s%s",
			n, full, delta, float64(full)/float64(n), egressKBs, note)
	}

	if threshold == 0 {
		t.Fatalf("no room size up to %d exceeded the datagram limit — the measurement is not exercising the wall",
			sizes[len(sizes)-1])
	}
	if threshold <= minSupportedRoomSize {
		t.Fatalf("snapshots stop fitting a datagram at %d players, at or below the %d this server must support — "+
			"the wire format grew; shrink it or add interest management",
			threshold, minSupportedRoomSize)
	}
	t.Logf("interest management becomes mandatory somewhere between %d and %d players",
		sizes[indexOf(sizes, threshold)-1], threshold)
}

func indexOf(xs []int, v int) int {
	for i, x := range xs {
		if x == v {
			return i
		}
	}
	return 0
}

// A worst-case message — a full room, mid-match, plus a full burst of events —
// still has to fit one datagram.
//
// This is what sets MaxSnapshotEvents. A delta owes every event since the
// client's last ack, so the events are the one part of a message whose size a
// client can grow by going quiet, and a message that outgrows udp.MaxDatagram
// is dropped rather than fragmented — costing the state as well as the events.
// The cap is measured here rather than guessed, and this fails if either the
// cap or the per-player size moves enough to matter.
func TestSnapshotWithAFullEventBurstFitsOneDatagram(t *testing.T) {
	const players = config.MaxRoomSize

	full, delta := midMatchSnapshot(t, players)

	// A burst of the largest events the wire can carry: high ids, a non-zero
	// HP, and a tick number big enough to need every byte of its varint.
	burst := make([]*pb.GameEvent, MaxSnapshotEvents)
	for i := range burst {
		burst[i] = &pb.GameEvent{
			Tick:   ^uint32(0),
			Kind:   pb.GameEventKind_GAME_EVENT_KIND_KILL,
			Actor:  ^uint32(0),
			Target: ^uint32(0),
			Hp:     -1 << 30,
		}
	}
	burstBytes := protocol.Sizeof(&pb.Snapshot{Events: burst}) - protocol.Sizeof(&pb.Snapshot{})

	t.Logf("at %d players: full %dB, delta %dB, %d events at worst %dB",
		players, full, delta, MaxSnapshotEvents, burstBytes)

	for _, c := range []struct {
		name string
		size int
	}{
		{"full snapshot + burst", full + burstBytes},
		{"delta + burst", delta + burstBytes},
	} {
		if got := c.size; got > udp.MaxDatagram {
			t.Errorf("%s is %dB at %d players, over the %dB datagram limit — "+
				"lower MaxSnapshotEvents (currently %d) or shrink the message",
				c.name, got, players, udp.MaxDatagram, MaxSnapshotEvents)
		}
	}
}
