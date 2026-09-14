package turn

import (
	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"google.golang.org/protobuf/proto"
)

// Stored events are protobuf, same as every other blob this repo puts in Redis.
func encodeEvent(e Event) ([]byte, error) { return proto.Marshal(e.toPB()) }

func decodeEvent(b []byte) (Event, error) {
	var m pb.TurnEvent
	if err := proto.Unmarshal(b, &m); err != nil {
		return Event{}, err
	}
	return Event{
		Seq: m.Seq, Kind: kindFromPB(m.Kind), PlayerID: m.PlayerId,
		Card: m.Card, HandCount: m.HandCount, Hand: m.Hand,
		Winner: m.Winner, DeadlineMS: m.DeadlineUnixMs,
	}, nil
}

func kindFromPB(k pb.TurnEventKind) Kind {
	switch k {
	case pb.TurnEventKind_TURN_EVENT_KIND_DEALT:
		return KindDealt
	case pb.TurnEventKind_TURN_EVENT_KIND_PLAYED:
		return KindPlayed
	case pb.TurnEventKind_TURN_EVENT_KIND_TRICK:
		return KindTrick
	case pb.TurnEventKind_TURN_EVENT_KIND_TIMEOUT:
		return KindTimeout
	case pb.TurnEventKind_TURN_EVENT_KIND_ENDED:
		return KindEnded
	}
	return 0
}
