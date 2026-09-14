package notify

import (
	"context"
	"log"
	"sync"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/safe"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// Bus delivers a message to the node holding a particular player's connection.
//
// Both payloads exist for the same reason: the node that decides something is
// not the node the player is attached to. A matchmaker forms a match and some
// gateway has to tell the player; a gateway applies a turn and the opponent's
// gateway has to tell them. Neither sender knows where the recipient is, so
// both broadcast and every node drops what is not its own — the same trade the
// arena already makes for assignments.
type Bus interface {
	Publish(ctx context.Context, a *pb.Assignment) error
	Subscribe(ctx context.Context, handler func(*pb.Assignment)) error
	// PublishTurn routes a turn update to the player named in it. nodeID is
	// where that player is believed to be connected; empty means "unknown",
	// and the update goes to every gateway so it is never simply lost.
	PublishTurn(ctx context.Context, nodeID string, p *pb.TurnPush) error
	// SubscribeTurn listens for this node's own updates and for the broadcast
	// fallback.
	SubscribeTurn(ctx context.Context, nodeID string, handler func(*pb.TurnPush)) error
}

type Memory struct {
	mu       sync.Mutex
	handler  func(*pb.Assignment)
	turnHand func(*pb.TurnPush)
}

func NewMemory() *Memory { return &Memory{} }

func (m *Memory) Publish(_ context.Context, a *pb.Assignment) error {
	m.mu.Lock()
	h := m.handler
	m.mu.Unlock()
	if h != nil {
		h(a)
	}
	return nil
}

func (m *Memory) Subscribe(_ context.Context, handler func(*pb.Assignment)) error {
	m.mu.Lock()
	m.handler = handler
	m.mu.Unlock()
	return nil
}

func (m *Memory) PublishTurn(_ context.Context, _ string, p *pb.TurnPush) error {
	m.mu.Lock()
	h := m.turnHand
	m.mu.Unlock()
	if h != nil {
		h(p)
	}
	return nil
}

func (m *Memory) SubscribeTurn(_ context.Context, _ string, handler func(*pb.TurnPush)) error {
	m.mu.Lock()
	m.turnHand = handler
	m.mu.Unlock()
	return nil
}

const (
	channel = "mm:results"
	// turnChannel is the fallback: every gateway receives it and drops what is
	// not its own. Addressed delivery uses turnChannel + ":" + nodeID.
	//
	// The difference is not cosmetic at scale. Broadcasting costs Redis one
	// send per gateway per message, so egress carries a factor of the fleet
	// size — and the fleet grows with the player count, which makes the cost
	// grow with the square of it. Every gateway also decodes every message to
	// discard almost all of them. Addressing the node turns both back into a
	// constant, and presence already records which node a player is on.
	turnChannel = "turn:push"
)

func turnChannelFor(nodeID string) string {
	if nodeID == "" {
		return turnChannel
	}
	return turnChannel + ":" + nodeID
}

type Redis struct {
	rdb *redis.Client
}

func NewRedis(rdb *redis.Client) *Redis { return &Redis{rdb: rdb} }

func (r *Redis) publish(ctx context.Context, ch string, m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return r.rdb.Publish(ctx, ch, b).Err()
}

// subscribe runs one channel's delivery loop. Both subscriptions want exactly
// this and differ only in what they decode, so the loop — and the ctx and
// closed-channel handling that is easy to get subtly wrong twice — lives once.
func (r *Redis) subscribe(ctx context.Context, deliver func([]byte), chans ...string) error {
	sub := r.rdb.Subscribe(ctx, chans...)
	msgs := sub.Channel()
	safe.Go("notify.subscribe", func() {
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				// One malformed or unexpected payload must not take the
				// subscription down with it: this loop is how assignments and
				// turn pushes reach players, and a dead one is a gateway that
				// goes quiet without saying so.
				payload := []byte(msg.Payload)
				safe.Do("notify.deliver", func() { deliver(payload) })
			}
		}
	})
	return nil
}

func (r *Redis) Publish(ctx context.Context, a *pb.Assignment) error {
	return r.publish(ctx, channel, a)
}

func (r *Redis) Subscribe(ctx context.Context, handler func(*pb.Assignment)) error {
	return r.subscribe(ctx, func(b []byte) {
		var a pb.Assignment
		if err := proto.Unmarshal(b, &a); err != nil {
			log.Printf("notify decode: %v", err)
			return
		}
		handler(&a)
	}, channel)
}

func (r *Redis) PublishTurn(ctx context.Context, nodeID string, p *pb.TurnPush) error {
	return r.publish(ctx, turnChannelFor(nodeID), p)
}

// SubscribeTurn takes both channels on one subscription: the node's own, and
// the broadcast for updates whose recipient could not be located.
func (r *Redis) SubscribeTurn(ctx context.Context, nodeID string, handler func(*pb.TurnPush)) error {
	chans := []string{turnChannel}
	if own := turnChannelFor(nodeID); own != turnChannel {
		chans = append(chans, own)
	}
	return r.subscribe(ctx, func(b []byte) {
		var p pb.TurnPush
		if err := proto.Unmarshal(b, &p); err != nil {
			log.Printf("notify decode turn: %v", err)
			return
		}
		handler(&p)
	}, chans...)
}
