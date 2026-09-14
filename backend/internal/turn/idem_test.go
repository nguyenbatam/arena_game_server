package turn

import (
	"context"
	"strconv"
	"testing"
)

// Idempotency keys come from the client, and two clients pick from the same
// small space. Storing them unscoped let one player's key match the other's,
// and the second player's legal move was then swallowed as a duplicate: no
// events, no error, the move simply never happened.
func TestIdemKeyIsScopedToItsPlayer(t *testing.T) {
	st := NewState("m", 7, [2]string{"alice", "bob"})
	first := st.Players[st.Turn]
	second := st.Players[1-st.Turn]

	if _, err := st.Apply(Move{PlayerID: first, Card: st.LowestCard(st.Seat(first)), IdemKey: "1"}); err != nil {
		t.Fatalf("first move: %v", err)
	}
	before := len(st.Hands[st.Seat(second)])
	evs, err := st.Apply(Move{PlayerID: second, Card: st.LowestCard(st.Seat(second)), IdemKey: "1"})
	if err != nil {
		t.Fatalf("second move: %v", err)
	}
	if len(evs) == 0 {
		t.Fatal("second player's move was swallowed as a duplicate of the first player's key")
	}
	if got := len(st.Hands[st.Seat(second)]); got != before-1 {
		t.Fatalf("second player still holds %d cards, want %d", got, before-1)
	}
}

// The same player replaying the same key is still a no-op — the property the
// scoping must not break.
func TestSamePlayerRetryIsStillIdempotent(t *testing.T) {
	st := NewState("m", 7, [2]string{"alice", "bob"})
	me := st.Players[st.Turn]
	card := st.LowestCard(st.Seat(me))

	evs, err := st.Apply(Move{PlayerID: me, Card: card, IdemKey: "retry-me"})
	if err != nil || len(evs) == 0 {
		t.Fatalf("first apply: %v evs=%d", err, len(evs))
	}
	hand := len(st.Hands[st.Seat(me)])

	evs, err = st.Apply(Move{PlayerID: me, Card: card, IdemKey: "retry-me"})
	if err != nil {
		t.Fatalf("retry returned %v, want nil", err)
	}
	if len(evs) != 0 {
		t.Fatalf("retry produced %d events, want 0", len(evs))
	}
	if got := len(st.Hands[st.Seat(me)]); got != hand {
		t.Fatalf("retry spent a card: %d -> %d", hand, got)
	}
}

// Both players number their own submissions from 1, which is what a real
// client does — so every key one sends also exists on the other side. Each
// player must still get all three of their moves.
func TestFullMatchWithCollidingIdemKeys(t *testing.T) {
	st := NewState("m", 21, [2]string{"alice", "bob"})
	sent := map[string]int{}
	for i := 0; i < 2*HandSize; i++ {
		if st.Ended {
			break
		}
		me := st.Players[st.Turn]
		sent[me]++
		key := strconv.Itoa(sent[me]) // "1", "2", "3" — from both players
		card := st.LowestCard(st.Seat(me))
		evs, err := st.Apply(Move{PlayerID: me, Card: card, IdemKey: key})
		if err != nil {
			t.Fatalf("move %d by %s (key %q): %v", i, me, key, err)
		}
		if len(evs) == 0 {
			t.Fatalf("move %d by %s (key %q) was swallowed", i, me, key)
		}
	}
	if !st.Ended {
		t.Fatal("match did not reach an ending")
	}
	if len(st.Hands[0]) != 0 || len(st.Hands[1]) != 0 {
		t.Fatalf("cards left over: %v %v", st.Hands[0], st.Hands[1])
	}
}

// Through the service, where the key round-trips via the store's JSON encoding.
func TestIdemKeyScopingSurvivesTheStore(t *testing.T) {
	svc := NewService(Options{Store: NewMemoryStore(TTL{}), Deadlines: NewMemoryDeadlines()})
	ctx := context.Background()
	st, err := svc.Create(ctx, "m1", 7, [2]string{"alice", "bob"})
	if err != nil {
		t.Fatal(err)
	}
	first := st.Players[st.Turn]
	if err := svc.Play(ctx, "m1", Move{PlayerID: first, Card: st.LowestCard(st.Seat(first)), IdemKey: "1"}); err != nil {
		t.Fatalf("first play: %v", err)
	}
	cur, _, err := svc.store.Get(ctx, "m1")
	if err != nil {
		t.Fatal(err)
	}
	second := cur.Players[cur.Turn]
	if second == first {
		t.Fatal("turn did not pass")
	}
	if err := svc.Play(ctx, "m1", Move{PlayerID: second, Card: cur.LowestCard(cur.Seat(second)), IdemKey: "1"}); err != nil {
		t.Fatalf("second play: %v", err)
	}
	after, _, err := svc.store.Get(ctx, "m1")
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Hands[after.Seat(second)]) != HandSize-1 {
		t.Fatalf("second player's move did not land: hand=%v", after.Hands[after.Seat(second)])
	}
}
