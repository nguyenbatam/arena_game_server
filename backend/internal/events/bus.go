package events

import (
	"context"
	"log"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/metrics"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type Bus interface {
	Publish(ctx context.Context, e *pb.Event) error
	Close() error
}

type LogBus struct{}

func (LogBus) Publish(_ context.Context, e *pb.Event) error {
	log.Printf("event type=%s room=%s tick=%d", e.GetType(), e.GetRoomId(), e.GetTick())
	metrics.EventsPublished.Inc()
	return nil
}

func (LogBus) Close() error { return nil }

type KafkaBus struct {
	w *kafka.Writer
}

func NewKafka(brokers []string, topic string) *KafkaBus {
	k := &KafkaBus{}
	k.w = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		// Asynchronous on purpose: publishing a match result must never be
		// something a tick waits on. The cost is that WriteMessages returns
		// before the broker has seen anything and cannot report a failure —
		// which is why delivery is accounted for in the completion callback
		// below rather than at the call site.
		Async:        true,
		BatchTimeout: 20 * time.Millisecond,
		Completion:   k.completion,
	}
	return k
}

// completion is where an asynchronous write finally succeeds or fails.
//
// Without it the failure has nowhere to go: the writer logs it at best, the
// caller has long since moved on, and the only counter in the system says the
// event was published. This is still not at-least-once delivery — a broker
// outage loses these events, and the fix for that is an outbox, which is
// written up in the README as a decision rather than an oversight. It is the
// difference between losing events and losing them silently.
func (k *KafkaBus) completion(msgs []kafka.Message, err error) {
	if err != nil {
		metrics.EventsDropped.Add(float64(len(msgs)))
		log.Printf("events: %d message(s) not delivered: %v", len(msgs), err)
		return
	}
	metrics.EventsPublished.Add(float64(len(msgs)))
}

func (k *KafkaBus) Publish(ctx context.Context, e *pb.Event) error {
	if e.Ts == 0 {
		e.Ts = time.Now().UnixMilli()
	}
	b, err := proto.Marshal(e)
	if err != nil {
		metrics.EventsDropped.Inc()
		return err
	}
	key := e.RoomId
	if key == "" {
		key = e.PlayerId
	}
	if err := k.w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: b}); err != nil {
		// Reached only when the writer rejects the message outright — a closed
		// writer, or a context already done. The asynchronous failures arrive
		// at completion instead.
		metrics.EventsDropped.Inc()
		return err
	}
	return nil
}

func (k *KafkaBus) Close() error { return k.w.Close() }
