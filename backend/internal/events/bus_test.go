package events

import (
	"context"
	"errors"
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/segmentio/kafka-go"
)

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

// The asynchronous writer returns before the broker has seen anything, so a
// failed delivery can only be observed here. Before the completion callback
// existed it could not be observed at all: the error went to the writer's
// logger and the caller had already counted the event as published.
func TestFailedDeliveryIsCountedAndNotReportedAsPublished(t *testing.T) {
	k := NewKafka([]string{"127.0.0.1:1"}, "test")
	t.Cleanup(func() { _ = k.Close() })

	dropped := counterValue(t, metrics.EventsDropped)
	published := counterValue(t, metrics.EventsPublished)

	k.completion([]kafka.Message{{}, {}}, errors.New("broker unreachable"))

	if got := counterValue(t, metrics.EventsDropped) - dropped; got != 2 {
		t.Fatalf("dropped rose by %v, want 2", got)
	}
	if got := counterValue(t, metrics.EventsPublished) - published; got != 0 {
		t.Fatalf("published rose by %v on a failed delivery", got)
	}
}

func TestSuccessfulDeliveryIsCountedOnce(t *testing.T) {
	k := NewKafka([]string{"127.0.0.1:1"}, "test")
	t.Cleanup(func() { _ = k.Close() })

	published := counterValue(t, metrics.EventsPublished)
	dropped := counterValue(t, metrics.EventsDropped)

	k.completion([]kafka.Message{{}, {}, {}}, nil)

	if got := counterValue(t, metrics.EventsPublished) - published; got != 3 {
		t.Fatalf("published rose by %v, want 3", got)
	}
	if got := counterValue(t, metrics.EventsDropped) - dropped; got != 0 {
		t.Fatalf("dropped rose by %v on a successful delivery", got)
	}
}

// A writer that has been closed rejects the write synchronously. That path has
// to count too, or a shutdown race would lose events silently.
func TestSynchronousRejectionIsCounted(t *testing.T) {
	k := NewKafka([]string{"127.0.0.1:1"}, "test")
	_ = k.Close()

	dropped := counterValue(t, metrics.EventsDropped)
	err := k.Publish(context.Background(), &pb.Event{
		Type: pb.EventType_EVENT_TYPE_MATCH_ENDED, RoomId: "r1",
	})
	if err == nil {
		t.Fatal("publishing on a closed writer reported success")
	}
	if got := counterValue(t, metrics.EventsDropped) - dropped; got != 1 {
		t.Fatalf("dropped rose by %v, want 1", got)
	}
}

// The in-memory bus is what every non-Kafka deployment runs, including the
// whole test suite: it has to account for what it handled, or the counter reads
// zero forever and looks like a broken pipeline.
func TestLogBusCountsWhatItHandled(t *testing.T) {
	published := counterValue(t, metrics.EventsPublished)
	var bus Bus = LogBus{}
	if err := bus.Publish(context.Background(), &pb.Event{
		Type: pb.EventType_EVENT_TYPE_MATCH_STARTED, RoomId: "r1",
	}); err != nil {
		t.Fatal(err)
	}
	if got := counterValue(t, metrics.EventsPublished) - published; got != 1 {
		t.Fatalf("published rose by %v, want 1", got)
	}
}

// A timestamp is stamped on the way out so consumers can order events without
// trusting the producer to have done it.
func TestPublishStampsATimestamp(t *testing.T) {
	k := NewKafka([]string{"127.0.0.1:1"}, "test")
	t.Cleanup(func() { _ = k.Close() })
	e := &pb.Event{Type: pb.EventType_EVENT_TYPE_MATCH_ENDED, RoomId: "r1"}
	_ = k.Publish(context.Background(), e)
	if e.Ts == 0 {
		t.Fatal("event went out with no timestamp")
	}
}
