package protocol

import (
	"testing"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"google.golang.org/protobuf/proto"
)

// UnmarshalEnv is where bytes from the internet first become structure, and
// every transport funnels into it. Table tests only ever cover the shapes
// someone thought of; a fuzzer covers the ones that arrive.
//
// The property under test is not "it parses" — most inputs are not valid
// protobuf and should be rejected. It is that nothing here panics, and that
// what does parse survives a round trip. A panic in this function is a remote
// crash of the whole gateway, which is exactly the class of bug the recover in
// onMessage now contains rather than fixes.
func FuzzUnmarshalEnv(f *testing.F) {
	seeds := [][]byte{
		Env(pb.MsgType_MSG_TYPE_HELLO, func(e *pb.Envelope) {
			e.Payload = &pb.Envelope_Hello{Hello: &pb.Hello{Name: "seed", ProtocolVersion: 1}}
		}),
		Env(pb.MsgType_MSG_TYPE_INPUT, func(e *pb.Envelope) {
			e.Payload = &pb.Envelope_Input{Input: &pb.Input{PlayerId: 1, Seq: 2, Mx: 1, My: -1, Fire: true, Aim: 90, AckTick: 7}}
		}),
		Env(pb.MsgType_MSG_TYPE_JOIN_ROOM, func(e *pb.Envelope) {
			e.Payload = &pb.Envelope_JoinRoom{JoinRoom: &pb.JoinRoom{RoomId: "r", YourId: 3, LastAckTick: 9}}
		}),
		Env(pb.MsgType_MSG_TYPE_TURN_PLAY, func(e *pb.Envelope) {
			e.Payload = &pb.Envelope_TurnPlay{TurnPlay: &pb.TurnPlay{MatchId: "m", Card: 5, TurnNumber: 2, IdemKey: "k"}}
		}),
		{}, {0x00}, {0xff, 0xff, 0xff, 0xff}, {0x08, 0x96, 0x01},
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		env, err := UnmarshalEnv(data)
		if err != nil {
			return
		}
		if env == nil {
			t.Fatal("UnmarshalEnv returned no error and no envelope")
		}

		// Every accessor the gateway reaches for, on a message that may have
		// any combination of fields set or missing.
		_ = MustType(env)
		_ = AcceptVersion(env.GetHello().GetProtocolVersion())
		_ = env.GetHello().GetName()
		_ = env.GetHello().GetSessionId()
		_ = env.GetJoinRoom().GetRoomId()
		_ = env.GetJoinRoom().GetLastAckTick()
		_ = env.GetInput().GetAckTick()
		_ = env.GetPing().GetNonce()
		_ = env.GetTurnPlay().GetCard()
		_ = env.GetTurnSync().GetSinceSeq()
		_ = InputFromEnv(env, 1)

		// Anything that parsed must re-encode; a message the server can read
		// but not write back is a message it cannot log, forward or replay.
		if _, err := proto.Marshal(env); err != nil {
			t.Fatalf("parsed envelope will not re-marshal: %v", err)
		}
	})
}
