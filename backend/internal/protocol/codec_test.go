package protocol

import (
	"encoding/json"
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"google.golang.org/protobuf/proto"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	raw := MatchFound("r-1", "", 2, 99, 20)
	e, err := UnmarshalEnv(raw)
	if err != nil {
		t.Fatal(err)
	}
	if e.Type != pb.MsgType_MSG_TYPE_MATCH_FOUND {
		t.Fatalf("type %v", e.Type)
	}
	mf := e.GetMatchFound()
	if mf.GetRoomId() != "r-1" || mf.GetYourId() != 2 || mf.GetSeed() != 99 {
		t.Fatalf("%+v", mf)
	}
}

func TestInputBoundToSession(t *testing.T) {
	env := &pb.Envelope{Type: pb.MsgType_MSG_TYPE_INPUT, Payload: &pb.Envelope_Input{Input: &pb.Input{
		PlayerId: 99, Seq: 3, Mx: 1,
	}}}
	in := InputFromEnv(env, 2)
	if in == nil || in.PlayerId != 2 {
		t.Fatalf("must bind to session seat, got %+v", in)
	}
}

func TestProtobufSmallerThanJSON(t *testing.T) {
	snap := &pb.Snapshot{
		Tick:   1200,
		RoomId: "r-99",
		Players: []*pb.PlayerSnap{
			{Id: 1, X: 1_234_000, Y: 980_000, Aim: 214, Hp: 75, Score: 3},
			{Id: 2, X: 800_000, Y: 1_100_000, Aim: 10, Hp: 100, Score: 1, Bot: true},
		},
		Projectiles: []*pb.ProjSnap{{Id: 9, X: 900_000, Y: 900_000}},
	}
	env := &pb.Envelope{Type: pb.MsgType_MSG_TYPE_SNAPSHOT, Payload: &pb.Envelope_Snapshot{Snapshot: snap}}
	pbBytes, err := proto.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	jsonBytes, err := json.Marshal(map[string]any{
		"t": "snapshot", "tick": snap.Tick, "room_id": snap.RoomId,
		"players": []map[string]any{
			{"id": 1, "x": 1234000, "y": 980000, "aim": 214, "hp": 75, "score": 3},
			{"id": 2, "x": 800000, "y": 1100000, "aim": 10, "hp": 100, "score": 1, "bot": true},
		},
		"projectiles": []map[string]any{{"id": 9, "x": 900000, "y": 900000}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pbBytes) >= len(jsonBytes) {
		t.Fatalf("protobuf %d should be < json %d", len(pbBytes), len(jsonBytes))
	}
	t.Logf("snapshot protobuf=%d json=%d ratio=%.2f", len(pbBytes), len(jsonBytes), float64(len(pbBytes))/float64(len(jsonBytes)))
}
