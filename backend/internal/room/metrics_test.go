package room

import (
	"testing"

	"github.com/nguyenbatam/arena_game_server/internal/metrics"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
	dto "github.com/prometheus/client_model/go"
)

// The broadcast path charges arena_snapshot_bytes_total through counters
// resolved once at startup rather than through WithLabelValues per send. That
// is a pure speed change and it must stay invisible from the outside: the
// series still has to be the one the vector exposes, under the same two labels.
// A typo in either string would silently start a third series and leave the
// dashboards reading zero — which nothing else here would catch, because a
// counter that is never incremented looks exactly like a quiet server.

func bytesFor(t *testing.T, kind string) float64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.SnapshotBytes.WithLabelValues(kind).Write(&m); err != nil {
		t.Fatalf("read arena_snapshot_bytes_total{kind=%q}: %v", kind, err)
	}
	return m.GetCounter().GetValue()
}

func TestSnapshotBytesAreChargedUnderBothEncodings(t *testing.T) {
	fullBefore := bytesFor(t, "full")
	deltaBefore := bytesFor(t, "delta")

	roster := []sim.Player{{ID: 1}, {ID: 2}}
	r := New(Params{ID: "bytes", Seed: 3, TickRate: 20, MatchTicks: 1 << 20, Roster: roster})
	var got [][]byte
	r.Subscribe(1, func(msg []byte) { got = append(got, msg) })

	pending := map[sim.PlayerID]sim.Input{}

	// First tick: the subscriber has acked nothing, so it is owed a full
	// snapshot and only the "full" series may move.
	r.broadcast(r.world.Step(pending))
	if bytesFor(t, "full") <= fullBefore {
		t.Error("a full snapshot was sent but arena_snapshot_bytes_total{kind=\"full\"} did not move")
	}
	if got, want := bytesFor(t, "delta"), deltaBefore; got != want {
		t.Errorf("delta bytes moved to %v on a full-snapshot tick; want %v", got, want)
	}

	// Ack it, and the next tick is a delta.
	sub := r.currentSubs().byID[1]
	sub.ack.Store(r.LastTick())

	fullAfter := bytesFor(t, "full")
	r.broadcast(r.world.Step(pending))
	if bytesFor(t, "delta") <= deltaBefore {
		t.Error("a delta was sent but arena_snapshot_bytes_total{kind=\"delta\"} did not move")
	}
	if got := bytesFor(t, "full"); got != fullAfter {
		t.Errorf("full bytes moved to %v on a delta tick; want %v", got, fullAfter)
	}
}

// The resolved children have to be the vector's own children, not copies: a
// child obtained twice for the same labels is the same counter, and that is
// what makes holding one at package level equivalent to looking it up per call.
func TestResolvedCountersAreTheVectorsOwnChildren(t *testing.T) {
	before := bytesFor(t, "full")
	metrics.SnapshotBytesFull.Add(7)
	if got := bytesFor(t, "full"); got != before+7 {
		t.Errorf("SnapshotBytesFull wrote to a different series: %v -> %v", before, got)
	}

	before = bytesFor(t, "delta")
	metrics.SnapshotBytesDelta.Add(11)
	if got := bytesFor(t, "delta"); got != before+11 {
		t.Errorf("SnapshotBytesDelta wrote to a different series: %v -> %v", before, got)
	}
}
