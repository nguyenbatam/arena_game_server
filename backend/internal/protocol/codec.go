package protocol

import (
	"fmt"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"google.golang.org/protobuf/proto"
)

func Marshal(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		return nil
	}
	return b
}

func UnmarshalEnv(b []byte) (*pb.Envelope, error) {
	var e pb.Envelope
	if err := proto.Unmarshal(b, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

func Env(t pb.MsgType, set func(*pb.Envelope)) []byte {
	e := &pb.Envelope{Type: t}
	set(e)
	return Marshal(e)
}

func Welcome(sessionID string, yourID uint32, tickRate int) []byte {
	return Env(pb.MsgType_MSG_TYPE_WELCOME, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Welcome{Welcome: &pb.Welcome{
			YourId: yourID, TickRate: int32(tickRate), MapW: 2000000, MapH: 2000000, SessionId: sessionID,
			ProtocolVersion: Version,
		}}
	})
}

func Queued() []byte {
	return Env(pb.MsgType_MSG_TYPE_QUEUED, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Queued{Queued: &pb.Queued{}}
	})
}

func MatchFound(roomID, host string, yourID uint32, seed int64, tickRate int) []byte {
	return Env(pb.MsgType_MSG_TYPE_MATCH_FOUND, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_MatchFound{MatchFound: &pb.MatchFound{
			RoomId: roomID, Host: host, YourId: yourID, Seed: seed, TickRate: int32(tickRate),
			MapW: 2000000, MapH: 2000000,
		}}
	})
}

func Redirect(roomID, host string, yourID uint32) []byte {
	return Env(pb.MsgType_MSG_TYPE_REDIRECT, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Redirect{Redirect: &pb.Redirect{RoomId: roomID, Host: host, YourId: yourID}}
	})
}

func Pong(nonce uint64, ts uint32) []byte {
	return Env(pb.MsgType_MSG_TYPE_PONG, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Pong{Pong: &pb.Pong{Nonce: nonce, TsMs: ts}}
	})
}

func Err(code pb.ErrorCode, msg string) []byte {
	return Env(pb.MsgType_MSG_TYPE_ERROR, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Error{Error: &pb.Error{Code: code, Message: msg}}
	})
}

// SnapshotEncoder marshals snapshots into the wire envelope without rebuilding
// that envelope for every message.
//
// Snapshot below allocates three things per call: the envelope, the oneof
// wrapper that carries the payload, and the output bytes. Only the last is
// something the caller actually receives — the other two are scaffolding that
// is dead again the moment Marshal returns. On the tick path that scaffolding
// is the majority of what is allocated: after the room stopped rebuilding its
// per-player messages, these two were two of the three remaining allocations
// per broadcast. Measured on an M4, the parallel broadcast benchmark went from
// 3 allocations and ~122 ns/op to 1 and ~102 ns/op.
//
// One encoder belongs to one goroutine — a room's tick loop — and is not safe
// for concurrent use. It must not be copied once used, and vet's copylocks
// check enforces that through the embedded message state.
//
// The bytes it returns are freshly allocated and belong to the caller. The
// encoder retains only the two messages it writes through, and the snapshot it
// was handed stays the caller's: it is read during Marshal and not referenced
// afterwards, so the caller is free to overwrite it on the next tick.
type SnapshotEncoder struct {
	env  pb.Envelope
	wrap pb.Envelope_Snapshot
}

// Marshal encodes s as a SNAPSHOT envelope. It is Snapshot, reusing this
// encoder's scaffolding.
func (e *SnapshotEncoder) Marshal(s *pb.Snapshot) []byte {
	e.wrap.Snapshot = s
	e.env.Type = pb.MsgType_MSG_TYPE_SNAPSHOT
	e.env.Payload = &e.wrap
	return Marshal(&e.env)
}

// Snapshot encodes s as a SNAPSHOT envelope, building the envelope each time.
// For callers that send one message rather than one per tick forever; the tick
// path uses SnapshotEncoder.
func Snapshot(s *pb.Snapshot) []byte {
	return Env(pb.MsgType_MSG_TYPE_SNAPSHOT, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_Snapshot{Snapshot: s}
	})
}

func TurnUpdate(up *pb.TurnUpdate) []byte {
	return Env(pb.MsgType_MSG_TYPE_TURN_UPDATE, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_TurnUpdate{TurnUpdate: up}
	})
}

func InputFromEnv(e *pb.Envelope, playerID uint32) *pb.Input {
	in := e.GetInput()
	if in == nil {
		return nil
	}
	in.PlayerId = playerID
	return in
}

func Sizeof(m proto.Message) int { return proto.Size(m) }

func MustType(e *pb.Envelope) pb.MsgType {
	if e == nil {
		return pb.MsgType_MSG_TYPE_UNSPECIFIED
	}
	return e.Type
}

func TickRateOr(v int32, def int) int {
	switch v {
	case 20, 30, 60:
		return int(v)
	default:
		return def
	}
}

func RoleString(r pb.Role) string {
	switch r {
	case pb.Role_ROLE_GATEWAY:
		return "gateway"
	case pb.Role_ROLE_MATCHMAKER:
		return "matchmaker"
	case pb.Role_ROLE_GAME_SERVER:
		return "gameserver"
	case pb.Role_ROLE_ALL:
		return "all"
	default:
		return fmt.Sprintf("%d", r)
	}
}

// Version is the wire contract this build speaks, and MinVersion the oldest a
// client may still speak to it.
//
// Without a version on the wire there is no way to turn an incompatible client
// away: it connects, the first message it cannot parse looks like a bug, and
// the player is left with a game that desyncs halfway into a match rather than
// a message telling them to update. Every shipped game carries this, and it is
// cheapest to add before there is a fleet of old clients to be compatible with.
//
// A client that sends nothing is from before the field existed and is treated
// as version 1 — the contract it was built against.
const (
	Version    uint32 = 1
	MinVersion uint32 = 1
)

// AcceptVersion reports whether this server will talk to a client speaking v.
//
// Newer is refused as firmly as older: a client built against a later contract
// may send fields this build will silently ignore, and silently ignoring an
// input is worse than refusing the connection.
func AcceptVersion(v uint32) bool {
	if v == 0 {
		v = 1
	}
	return v >= MinVersion && v <= Version
}
