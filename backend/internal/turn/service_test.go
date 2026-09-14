package turn

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
)

const (
	alice = "acct-alice"
	bob   = "acct-bob"
)

type capture struct {
	mu sync.Mutex
	by map[string][]*pb.TurnUpdate
}

func (c *capture) notify(pid string, up *pb.TurnUpdate) {
	c.mu.Lock()
	if c.by == nil {
		c.by = map[string][]*pb.TurnUpdate{}
	}
	c.by[pid] = append(c.by[pid], up)
	c.mu.Unlock()
}

func (c *capture) last(pid string) *pb.TurnUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.by[pid]
	if len(l) == 0 {
		return nil
	}
	return l[len(l)-1]
}

func newSvc(t *testing.T, limit time.Duration) (*Service, *capture, *MemoryStore, *MemoryDeadlines) {
	t.Helper()
	cap := &capture{}
	store, dl := NewMemoryStore(TTL{}), NewMemoryDeadlines()
	s := NewService(Options{
		Store: store, Deadlines: dl,
		Notify: cap.notify, TurnLimit: limit,
		NetworkGrace: -1, // tests drive the clock; no padding
	})
	return s, cap, store, dl
}

func mustCreate(t *testing.T, s *Service) *State {
	t.Helper()
	st, err := s.Create(context.Background(), "m1", 99, [2]string{alice, bob})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return st
}

// The whole point of family C plus hidden information: one logged event becomes
// different bytes for each recipient.
func TestDealIsProjectedPerViewer(t *testing.T) {
	s, cap, _, _ := newSvc(t, time.Minute)
	st := mustCreate(t, s)

	for _, pid := range []string{alice, bob} {
		up := cap.last(pid)
		if up == nil {
			t.Fatalf("player %s got no update", pid)
		}
		var ownHand, otherHand int
		for _, e := range up.Events {
			if e.Kind != pb.TurnEventKind_TURN_EVENT_KIND_DEALT {
				continue
			}
			if e.PlayerId == pid {
				ownHand = len(e.Hand)
			} else {
				otherHand = len(e.Hand)
				if e.HandCount != HandSize {
					t.Errorf("player %s should still learn the opponent holds %d cards, got %d",
						pid, HandSize, e.HandCount)
				}
			}
		}
		if ownHand != HandSize {
			t.Errorf("player %s saw %d of their own cards, want %d", pid, ownHand, HandSize)
		}
		if otherHand != 0 {
			t.Errorf("player %s saw %d of the opponent's cards — the hand must stay hidden", pid, otherHand)
		}
	}

	// And the state projection agrees with the events.
	if got := cap.last(alice).State; len(got.YourHand) != HandSize || got.OpponentHandCount != HandSize {
		t.Fatalf("state projection leaked or lost cards: %+v", got)
	}
	if st.Hands[0] == nil || st.Hands[1] == nil {
		t.Fatal("the stored state must keep both real hands")
	}
}

func TestTurnOrderAndLegality(t *testing.T) {
	s, _, store, _ := newSvc(t, time.Minute)
	mustCreate(t, s)
	ctx := context.Background()

	st, _, _ := store.Get(ctx, "m1")
	first := st.Players[st.Turn]
	other := st.Players[st.Opponent(st.Turn)]

	if err := s.Play(ctx, "m1", Move{PlayerID: other, Card: st.Hands[st.Opponent(st.Turn)][0]}); !errors.Is(err, ErrNotYourTurn) {
		t.Fatalf("playing out of turn: got %v, want ErrNotYourTurn", err)
	}
	if err := s.Play(ctx, "m1", Move{PlayerID: first, Card: DeckHigh + 5}); !errors.Is(err, ErrBadCardValue) {
		t.Fatalf("out-of-range card: got %v, want ErrBadCardValue", err)
	}

	held := st.Hands[st.Turn][0]
	var notHeld uint32
	for v := uint32(1); v <= DeckHigh; v++ {
		if !st.holds(st.Turn, v) {
			notHeld = v
			break
		}
	}
	if err := s.Play(ctx, "m1", Move{PlayerID: first, Card: notHeld}); !errors.Is(err, ErrCardNotHeld) {
		t.Fatalf("card not in hand: got %v, want ErrCardNotHeld", err)
	}
	if err := s.Play(ctx, "m1", Move{PlayerID: first, Card: held}); err != nil {
		t.Fatalf("legal move rejected: %v", err)
	}
}

// A retried submission must not spend a second card.
func TestIdempotentRetry(t *testing.T) {
	s, _, store, _ := newSvc(t, time.Minute)
	mustCreate(t, s)
	ctx := context.Background()

	st, _, _ := store.Get(ctx, "m1")
	mover := st.Players[st.Turn]
	card := st.Hands[st.Turn][0]
	move := Move{PlayerID: mover, Card: card, IdemKey: "abc-123"}

	if err := s.Play(ctx, "m1", move); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if err := s.Play(ctx, "m1", move); err != nil {
		t.Fatalf("retry should be a silent no-op, got %v", err)
	}

	after, _, _ := store.Get(ctx, "m1")
	if after.TurnNumber != 2 {
		t.Fatalf("turn counter = %d after one move plus a retry, want 2 (starts at 1)", after.TurnNumber)
	}
}

// The race the CAS exists for: a real move and the deadline sweeper both trying
// to spend the same turn.
func TestMoveAndTimeoutCannotBothApply(t *testing.T) {
	s, _, store, _ := newSvc(t, time.Minute)
	mustCreate(t, s)
	ctx := context.Background()

	st, _, _ := store.Get(ctx, "m1")
	mover := st.Players[st.Turn]
	card := st.Hands[st.Turn][len(st.Hands[st.Turn])-1] // deliberately not the lowest

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = s.Play(ctx, "m1", Move{PlayerID: mover, Card: card}) }()
	go func() { defer wg.Done(); _ = s.ExpireTurn(ctx, "m1", st.TurnNumber) }()
	wg.Wait()

	after, _, _ := store.Get(ctx, "m1")
	if after.TurnNumber != 2 {
		t.Fatalf("turn counter = %d, want 2: exactly one of the move and the timeout may apply", after.TurnNumber)
	}
	if len(after.Hands[st.Turn]) != HandSize-1 {
		t.Fatalf("player spent %d cards on one turn", HandSize-len(after.Hands[st.Turn]))
	}
}

// Timing out plays the weakest card, never a good one — otherwise going away
// would be a winning strategy.
func TestTimeoutPlaysTheWeakestCard(t *testing.T) {
	s, _, store, _ := newSvc(t, time.Minute)
	mustCreate(t, s)
	ctx := context.Background()

	before, _, _ := store.Get(ctx, "m1")
	seat := before.Turn
	lowest := before.LowestCard(seat)

	if err := s.ExpireTurn(ctx, "m1", before.TurnNumber); err != nil {
		t.Fatalf("ExpireTurn: %v", err)
	}
	after, _, _ := store.Get(ctx, "m1")
	if after.holds(seat, lowest) {
		t.Fatal("auto-play did not spend the lowest card")
	}
	if after.TableCard != lowest {
		t.Fatalf("table shows %d, want the lowest card %d", after.TableCard, lowest)
	}
}

// The match does not wait for an absent player; the clock keeps running.
func TestDeadlineSweepAutoPlays(t *testing.T) {
	s, _, store, dl := newSvc(t, 10*time.Millisecond)
	mustCreate(t, s)
	ctx := context.Background()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.sweepOnce(ctx)
		st, _, _ := store.Get(ctx, "m1")
		if st.TurnNumber > 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	due, _ := dl.PopDue(ctx, time.Now(), 10)
	t.Fatalf("deadline never fired (pending=%+v)", due)
}

func TestSyncReturnsDiffThenFullResyncOnGap(t *testing.T) {
	s, _, store, _ := newSvc(t, time.Minute)
	mustCreate(t, s)
	ctx := context.Background()

	up, err := s.Sync(ctx, "m1", alice, 0)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if up.FullResync {
		t.Fatal("a fresh cursor inside the window should diff, not resync")
	}
	if len(up.Events) == 0 {
		t.Fatal("expected the deal events")
	}

	// Push the window past the client's cursor.
	stale := up.CurrentSeq
	filler := make([]Event, LogWindow+5)
	for i := range filler {
		filler[i] = Event{Kind: KindPlayed, PlayerID: alice, Card: 1}
	}
	cur, ver, err := store.Get(ctx, "m1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, cur, ver, filler); err != nil {
		t.Fatal(err)
	}

	up2, err := s.Sync(ctx, "m1", alice, stale)
	if err != nil {
		t.Fatalf("Sync after trim: %v", err)
	}
	if !up2.FullResync {
		t.Fatal("a cursor older than the retained window must force a full resync")
	}
	if up2.State == nil {
		t.Fatal("a full resync must carry the state")
	}
	if len(up2.Events) != 0 {
		t.Fatal("a full resync must not also ship a partial event list")
	}
}

// Play the whole match out and check it terminates with a consistent result.
func TestFullMatchReachesAnEnding(t *testing.T) {
	s, _, store, _ := newSvc(t, time.Minute)
	mustCreate(t, s)
	ctx := context.Background()

	for i := 0; i < HandSize*2+2; i++ {
		st, _, err := store.Get(ctx, "m1")
		if err != nil {
			t.Fatal(err)
		}
		if st.Ended {
			total := st.Scores[0] + st.Scores[1]
			if total != HandSize {
				t.Fatalf("tricks awarded = %d, want %d", total, HandSize)
			}
			if len(st.Hands[0]) != 0 || len(st.Hands[1]) != 0 {
				t.Fatal("match ended with cards still in hand")
			}
			return
		}
		if err := s.Play(ctx, "m1", Move{
			PlayerID:   st.Players[st.Turn],
			Card:       st.Hands[st.Turn][0],
			TurnNumber: st.TurnNumber,
		}); err != nil {
			t.Fatalf("move %d: %v", i, err)
		}
	}
	t.Fatal("match never ended")
}

// A stale turn_number means the client acted on a view of the match that has
// already moved on.
func TestStaleTurnNumberRejected(t *testing.T) {
	s, _, store, _ := newSvc(t, time.Minute)
	mustCreate(t, s)
	ctx := context.Background()

	st, _, _ := store.Get(ctx, "m1")
	mover := st.Players[st.Turn]
	if err := s.Play(ctx, "m1", Move{
		PlayerID: mover, Card: st.Hands[st.Turn][0], TurnNumber: 7777,
	}); !errors.Is(err, ErrStaleTurn) {
		t.Fatalf("got %v, want ErrStaleTurn", err)
	}
}

// A deadline armed for a turn that has since been played must be ignored, not
// cashed in against whoever is on the clock now.
func TestStaleDeadlineDoesNotStealTheNextTurn(t *testing.T) {
	s, _, store, _ := newSvc(t, time.Minute)
	mustCreate(t, s)
	ctx := context.Background()

	before, _, _ := store.Get(ctx, "m1")
	armedFor := before.TurnNumber

	// The player moves in time.
	if err := s.Play(ctx, "m1", Move{
		PlayerID: before.Players[before.Turn], Card: before.Hands[before.Turn][0],
	}); err != nil {
		t.Fatalf("Play: %v", err)
	}
	mid, _, _ := store.Get(ctx, "m1")

	// The old deadline fires late.
	if err := s.ExpireTurn(ctx, "m1", armedFor); err != nil {
		t.Fatalf("stale expiry should be a silent no-op, got %v", err)
	}

	after, _, _ := store.Get(ctx, "m1")
	if after.TurnNumber != mid.TurnNumber {
		t.Fatalf("stale deadline advanced the turn from %d to %d", mid.TurnNumber, after.TurnNumber)
	}
	if len(after.Hands[after.Turn]) != len(mid.Hands[mid.Turn]) {
		t.Fatal("stale deadline spent a card belonging to the next player")
	}
}

// OnEnd is the only hook a match result can hang off: the winner is decided
// inside Apply and nothing outside this package watches the log. So it has to
// fire exactly once, at the end and not before, with the final state.
//
// Once matters as much as at-all. The compare-and-swap is what guarantees it —
// a move that loses the race never reaches the callback — and a caller writing
// that result into a database is relying on that.
func TestOnEndFiresOnceWithTheFinalState(t *testing.T) {
	var mu sync.Mutex
	var ended []*State
	store, dl := NewMemoryStore(TTL{}), NewMemoryDeadlines()
	s := NewService(Options{
		Store: store, Deadlines: dl, NetworkGrace: -1, TurnLimit: time.Minute,
		OnEnd: func(st *State) {
			mu.Lock()
			ended = append(ended, st)
			mu.Unlock()
		},
	})
	mustCreate(t, s)
	ctx := context.Background()

	moves := 0
	for i := 0; i < HandSize*2+2; i++ {
		st, _, err := store.Get(ctx, "m1")
		if err != nil {
			t.Fatal(err)
		}
		if st.Ended {
			break
		}
		mu.Lock()
		fired := len(ended)
		mu.Unlock()
		if fired != 0 {
			t.Fatalf("OnEnd fired after %d moves, with the match still running", moves)
		}
		if err := s.Play(ctx, "m1", Move{
			PlayerID: st.Players[st.Turn], Card: st.Hands[st.Turn][0], TurnNumber: st.TurnNumber,
		}); err != nil {
			t.Fatalf("move %d: %v", i, err)
		}
		moves++
	}

	mu.Lock()
	defer mu.Unlock()
	if len(ended) != 1 {
		t.Fatalf("OnEnd fired %d times, want exactly 1", len(ended))
	}
	final := ended[0]
	if !final.Ended {
		t.Fatal("OnEnd was handed a state that is not finished")
	}
	if final.MatchID != "m1" {
		t.Fatalf("OnEnd match id = %q", final.MatchID)
	}
	if total := final.Scores[0] + final.Scores[1]; total != HandSize {
		t.Fatalf("OnEnd state has %d tricks, want %d — it is not the final state", total, HandSize)
	}
	if final.Winner != "" && final.Winner != alice && final.Winner != bob {
		t.Fatalf("OnEnd winner = %q", final.Winner)
	}
}

// A match that was already over does not end a second time: Apply rejects the
// move before anything is committed, so a duplicate submission cannot pay a
// reward twice even before the store's idempotency key is involved.
func TestOnEndDoesNotFireForAMoveOnAFinishedMatch(t *testing.T) {
	var fired int
	var mu sync.Mutex
	store, dl := NewMemoryStore(TTL{}), NewMemoryDeadlines()
	s := NewService(Options{
		Store: store, Deadlines: dl, NetworkGrace: -1, TurnLimit: time.Minute,
		OnEnd: func(*State) { mu.Lock(); fired++; mu.Unlock() },
	})
	mustCreate(t, s)
	ctx := context.Background()

	var last Move
	for i := 0; i < HandSize*2+2; i++ {
		st, _, _ := store.Get(ctx, "m1")
		if st.Ended {
			break
		}
		last = Move{PlayerID: st.Players[st.Turn], Card: st.Hands[st.Turn][0], TurnNumber: st.TurnNumber}
		if err := s.Play(ctx, "m1", last); err != nil {
			t.Fatalf("move %d: %v", i, err)
		}
	}
	// Replay the winning move at a stale turn number, the way a client that
	// lost its answer would.
	_ = s.Play(ctx, "m1", last)
	// And let the deadline sweeper look at the finished match.
	if err := s.ExpireTurn(ctx, "m1", 0); err != nil {
		t.Fatalf("ExpireTurn on a finished match: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if fired != 1 {
		t.Fatalf("OnEnd fired %d times, want 1", fired)
	}
}
