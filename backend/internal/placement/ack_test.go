package placement

import (
	"context"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/redistest"
)

// Ack must retire the delivery it was given, not a re-encoding of it.
//
// It used to re-marshal the request to name the list entry, which reads as
// equivalent and is not: protobuf makes no promise that an equal message
// encodes to identical bytes. Anything that perturbs the encoding — a library
// version, a field the handler touched on its way through — made LREM match
// nothing, and the job then sat in the processing list until the reaper
// decided it was abandoned and delivered the same match a second time.
//
// A caller mutating the request stands in for that drift here, because it is
// the one form of it a test can produce deterministically.
func TestAckRetiresTheDeliveryEvenIfTheRequestChanged(t *testing.T) {
	rdb := redistest.Client(t)
	q := NewRedis(rdb)
	ctx := context.Background()

	if err := q.Enqueue(ctx, "gs-1", &pb.RoomRequest{
		RoomId: "m1", Seed: 7, TickRate: 20, MatchTicks: 1800,
		Seats: []*pb.Seat{{ConnId: "c1", PlayerId: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	job, err := q.Take(ctx, "gs-1")
	if err != nil || job == nil {
		t.Fatalf("Take: %v %v", job, err)
	}

	// Whatever the handler did to the request on its way through, the delivery
	// is still the delivery.
	job.Req.Seats = append(job.Req.Seats, &pb.Seat{ConnId: "c2", PlayerId: 2})
	job.Req.TickRate = 60

	if err := q.Ack(ctx, "gs-1", job); err != nil {
		t.Fatal(err)
	}

	// Nothing left in flight, so nothing for the reaper to resurrect.
	n, err := rdb.LLen(ctx, procKey("gs-1")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d job(s) still in the processing list after Ack", n)
	}
	reaped, err := q.ReapStale(ctx, "gs-1", time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if reaped != 0 {
		t.Errorf("reaper resurrected %d acked job(s)", reaped)
	}
}

// The memory queue has no processing list, so Ack has nothing to retire — but
// it must still accept the Job shape and stay a no-op rather than an error.
func TestMemoryAckAcceptsAJob(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	if err := q.Enqueue(ctx, "gs-1", &pb.RoomRequest{RoomId: "m1"}); err != nil {
		t.Fatal(err)
	}
	job, err := q.Take(ctx, "gs-1")
	if err != nil || job == nil || job.Req.RoomId != "m1" {
		t.Fatalf("Take: %+v %v", job, err)
	}
	if err := q.Ack(ctx, "gs-1", job); err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(ctx, "gs-1", nil); err != nil {
		t.Errorf("Ack(nil): %v", err)
	}
}

// A gameserver that took a job and died must have it returned — and acking the
// redelivery must retire it, even though that is a different Job value than the
// first delivery produced.
//
// The redelivery is where the token matters most. It is a fresh pop with its
// own bytes, so an Ack that named the entry by re-encoding the request would be
// matching against whatever the first delivery happened to encode to, on a node
// that may not even be the one that took it the second time.
func TestAckingARedeliveredJobRetiresIt(t *testing.T) {
	rdb := redistest.Client(t)
	q := NewRedis(rdb)
	ctx := context.Background()

	if err := q.Enqueue(ctx, "gs-1", &pb.RoomRequest{
		RoomId: "r-abandoned", Seed: 3, TickRate: 20,
		Seats: []*pb.Seat{{ConnId: "c1", PlayerId: 1}},
	}); err != nil {
		t.Fatal(err)
	}

	// Taken by a node that then dies without acking.
	if _, err := q.Take(ctx, "gs-1"); err != nil {
		t.Fatal(err)
	}
	ageOut(t, rdb, "r-abandoned")

	n, err := q.ReapStale(ctx, "gs-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reaped %d, want the one abandoned job", n)
	}

	// Redelivered, and this time acked.
	back, err := q.Take(ctx, "gs-1")
	if err != nil || back == nil || back.Req.RoomId != "r-abandoned" {
		t.Fatalf("the abandoned job did not come back: %+v %v", back, err)
	}
	if err := q.Ack(ctx, "gs-1", back); err != nil {
		t.Fatal(err)
	}

	if n, err := rdb.LLen(ctx, procKey("gs-1")).Result(); err != nil || n != 0 {
		t.Fatalf("%d job(s) still in flight after acking the redelivery (%v)", n, err)
	}
	ageOut(t, rdb, "r-abandoned")
	if n, err := q.ReapStale(ctx, "gs-1", time.Minute); err != nil || n != 0 {
		t.Fatalf("the reaper resurrected the acked redelivery: %d (%v)", n, err)
	}
}
