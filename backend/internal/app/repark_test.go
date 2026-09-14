package app

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/session"
	"github.com/nguyenbatam/arena_game_server/internal/turn"
)

// brokenCreate is a store that has every match fail at the moment it is
// written — the window where the opponent has already been claimed.
type brokenCreate struct{ turn.Store }

func (brokenCreate) Create(context.Context, *turn.State, []turn.Event) ([]turn.Event, error) {
	return nil, errors.New("store down")
}

// A claim that cannot be turned into a match must put the opponent back in the
// pairing slot.
//
// Claim consumes the park, so without this the first player is neither playing
// nor waiting: nobody holds their connection to tell them, and they sit on a
// queue screen for the rest of their session while every later arrival parks
// behind them.
func TestFailedMatchCreateReparksTheOpponent(t *testing.T) {
	a := testApp(t, time.Minute)
	a.turn = turn.NewService(turn.Options{
		Store:     brokenCreate{turn.NewMemoryStore(turn.TTL{})},
		Deadlines: turn.NewMemoryDeadlines(),
		Notify:    a.turnNotify,
		TurnLimit: time.Minute,
	})

	c1 := session.NewConn("turn-1", 64)
	c2 := session.NewConn("turn-2", 64)
	join(t, a, c1, "turn-1")
	join(t, a, c2, "turn-2")

	joinMsg := protocol.Env(pb.MsgType_MSG_TYPE_TURN_JOIN, func(e *pb.Envelope) {
		e.Payload = &pb.Envelope_TurnJoin{TurnJoin: &pb.TurnJoin{}}
	})
	a.onMessage(c1, joinMsg)
	if e := nextOfType(t, c1, pb.MsgType_MSG_TYPE_QUEUED); e == nil {
		t.Fatal("first player should be parked")
	}

	// The second arrival claims the first, then fails to start the match.
	a.onMessage(c2, joinMsg)
	if e := nextOfType(t, c2, pb.MsgType_MSG_TYPE_ERROR); e == nil {
		t.Fatal("the player who tried to start the match should have been told")
	}

	// The claimed player is waiting again, so a third arrival is paired with
	// them rather than parking behind a slot nobody is in.
	other, err := a.turnPair.Claim(context.Background(), "turn-3", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if other != "turn-1" {
		t.Errorf("claimed %q, want the reparked %q", other, "turn-1")
	}
}
