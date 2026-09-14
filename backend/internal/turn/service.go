package turn

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/metrics"
)

// DefaultTurnLimit is how long a player has to move. Real products pad this:
// the client shows a shorter clock than the server enforces, so a move sent
// just before the buzzer still counts after its flight time.
const DefaultTurnLimit = 20 * time.Second

// NetworkGrace is that pad. The server waits this much longer than the clock
// the client draws.
const NetworkGrace = 2 * time.Second

// Notifier pushes an update to one player. The gateway supplies it; the service
// does not know or care whether that player is on WebSocket, TCP or UDP.
type Notifier func(playerID string, msg *pb.TurnUpdate)

type Service struct {
	store     Store
	deadlines Deadlines
	notify    Notifier
	onEnd     func(*State)
	limit     time.Duration
	grace     time.Duration
	opTimeout time.Duration
	now       func() time.Time
}

type Options struct {
	Store     Store
	Deadlines Deadlines
	Notify    Notifier
	TurnLimit time.Duration
	// NetworkGrace pads the server clock past the one the client draws. Set it
	// negative to disable entirely; zero takes the default.
	NetworkGrace time.Duration
	// OnEnd is called once, on the node that applied the move that ended the
	// match, with the final state. It is the only hook a match result can hang
	// off: the winner is decided inside Apply and nothing outside this package
	// watches the log.
	//
	// Called synchronously on the write path, which makes it the caller's job
	// to decide whether the work belongs there. A gateway writing the result to
	// a database hands it to a goroutine; a test counting endings does not.
	// Exactly-once is not promised either — the store commit succeeded, so a
	// process that dies here has ended the match without reporting it, and
	// anything durable downstream has to be idempotent on the match id.
	OnEnd func(*State)
	// OpTimeout bounds one store call made by the deadline sweeper. Nobody is
	// waiting on that loop, so an unbounded call there does not park a player —
	// it parks the sweeper, and with it every deadline queued behind this one.
	// Zero leaves the calls unbounded, which is what the in-memory store wants.
	OpTimeout time.Duration
	Now       func() time.Time
}

func NewService(o Options) *Service {
	s := &Service{
		store: o.Store, deadlines: o.Deadlines,
		notify: o.Notify, onEnd: o.OnEnd, limit: o.TurnLimit, grace: o.NetworkGrace,
		opTimeout: o.OpTimeout, now: o.Now,
	}
	if s.limit <= 0 {
		s.limit = DefaultTurnLimit
	}
	switch {
	case s.grace == 0:
		s.grace = NetworkGrace
	case s.grace < 0:
		s.grace = 0
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.notify == nil {
		s.notify = func(string, *pb.TurnUpdate) {}
	}
	return s
}

// Create deals a match, writes the opening events and arms the first deadline.
func (s *Service) Create(ctx context.Context, matchID string, seed int64, players [2]string) (*State, error) {
	st := NewState(matchID, seed, players)
	deadline := s.now().Add(s.limit + s.grace)
	st.DeadlineMS = deadline.UnixMilli()

	// One DEALT event per player: each carries a real hand, and projection
	// decides which recipient is allowed to see it.
	events := make([]Event, 0, 2)
	for seat, pid := range st.Players {
		events = append(events, Event{
			Kind: KindDealt, PlayerID: pid,
			Hand:      append([]uint32(nil), st.Hands[seat]...),
			HandCount: uint32(len(st.Hands[seat])), DeadlineMS: st.DeadlineMS,
		})
	}
	// Armed before the match is written, not after.
	//
	// The other order looks more natural and leaves a match nothing will ever
	// move: the store commit has succeeded, so the match is real and live, but
	// a failed Arm returns an error and no deadline is ever queued for it.
	// Nothing else counts down in this mode, so the first player to walk away
	// strands the other one until the match hits LiveTTL — 24 hours, by
	// default. Arming first cannot do that: the deadline is queued against a
	// match that may not exist, and that case is already the ordinary one,
	// because an entry for a turn that has since been played is deliberately
	// left to be rejected on pop. sweepOnce treats ErrNoMatch as expected.
	if err := s.deadlines.Arm(ctx, matchID, st.TurnNumber, deadline); err != nil {
		return nil, err
	}
	stored, err := s.store.Create(ctx, st, events)
	if err != nil {
		return nil, err
	}
	metrics.TurnMatches.Inc()
	s.fanout(ctx, st, stored)
	return st, nil
}

// Play applies a player's move.
func (s *Service) Play(ctx context.Context, matchID string, m Move) error {
	return s.apply(ctx, matchID, m)
}

// apply is the single write path: a player's move and the timeout worker's
// auto-play both come through here.
//
// The compare-and-swap is what makes the race safe. A move arriving at the same
// instant the deadline fires produces two writers holding the same version;
// exactly one CAS succeeds, the loser re-reads and finds the turn already
// advanced. Both outcomes are correct — what must never happen is both applying.
func (s *Service) apply(ctx context.Context, matchID string, m Move) error {
	const attempts = 4
	for i := 0; i < attempts; i++ {
		st, version, err := s.store.Get(ctx, matchID)
		if err != nil {
			return err
		}

		events, err := st.Apply(m)
		if err != nil {
			metrics.TurnRejects.WithLabelValues(rejectReason(err)).Inc()
			return err
		}
		if len(events) == 0 {
			// Idempotent replay of a move already applied.
			return nil
		}

		var deadline time.Time
		if !st.Ended {
			deadline = s.now().Add(s.limit + s.grace)
			st.DeadlineMS = deadline.UnixMilli()
			events[len(events)-1].DeadlineMS = st.DeadlineMS
		} else {
			st.DeadlineMS = 0
		}

		// Armed before the commit, for the reason Create gives at length: a
		// match whose deadline was never queued has nothing counting down, and
		// the first player to walk away strands the other until LiveTTL — 24
		// hours, by default.
		//
		// This used to arm afterwards and discard the error, which is the one
		// ordering that can produce exactly that: the commit has succeeded, so
		// the move is durable and the turn has advanced, and the only thing
		// that would ever have moved it again failed silently. Arming first
		// turns the same failure into a move that did not happen, which the
		// caller can report and the client can retry.
		//
		// The cost is an entry armed against a turn that may never become
		// current, when the commit below loses its CAS or fails outright. That
		// case is already the ordinary one and is already handled: ExpireTurn
		// compares the turn it was armed for against the live one and declines,
		// and sweepOnce counts that as expected rather than as an error.
		if !st.Ended {
			if err := s.deadlines.Arm(ctx, matchID, st.TurnNumber, deadline); err != nil {
				metrics.TurnDeadlineArmFailures.Inc()
				return fmt.Errorf("turn: arm deadline for %s: %w", matchID, err)
			}
		}

		// One call, one atomic write: the new state and the events that
		// produced it land together or not at all. A move that loses the race
		// leaves nothing behind — no phantom events, no state without a log
		// entry to explain it.
		stored, err := s.store.Commit(ctx, st, version, events)
		if err != nil {
			if errors.Is(err, ErrVersionConflict) && i < attempts-1 {
				// Someone wrote first. Re-read and re-validate rather than
				// clobber: on retry Apply rejects this move if the turn moved on.
				metrics.TurnConflicts.Inc()
				continue
			}
			return err
		}

		// Nothing to clear when the match ends: the outstanding entry names a
		// turn that will never be current again, so it is rejected on pop.
		origin := "player"
		if m.Auto {
			origin = "timeout"
		}
		metrics.TurnMoves.WithLabelValues(origin).Inc()

		s.fanout(ctx, st, stored)
		// After the fanout: the players learning the match is over is worth
		// more than the bookkeeping, and the CAS above is what guarantees this
		// runs for exactly one writer. A move that lost the race never gets
		// here, so a match cannot report its ending twice.
		if st.Ended && s.onEnd != nil {
			s.onEnd(st)
		}
		return nil
	}
	return ErrVersionConflict
}

// Sync answers a client cursor: a diff when it can, the whole state when the
// cursor is too old to diff from.
// The log is read before the state, and the order is the whole correctness
// argument rather than a style choice.
//
// The two reads are separate round trips, so a move can land between them, and
// which way the pair is skewed decides what that costs. Reading the state first
// skews it the fatal way: the reply then carries a state from before the move
// and a current_seq from after it, and the client acks a cursor for an event it
// was never shown. On the full-resync path — where the events are deliberately
// not sent — that event is gone for good, and if it was the one that ended the
// match the client sits on a finished game waiting for a turn.
//
// This way round the skew is harmless: the state is never older than
// current_seq, so a client at worst holds a state slightly ahead of its cursor
// and is handed a couple of events it has already applied on its next sync.
// Replaying an event it has is something every client on this protocol already
// does — that is what a cursor is for. Losing one is not.
func (s *Service) Sync(ctx context.Context, matchID string, viewer string, since uint64) (*pb.TurnUpdate, error) {
	events, current, gap, err := s.store.Since(ctx, matchID, since)
	if err != nil {
		return nil, err
	}
	st, _, err := s.store.Get(ctx, matchID)
	if err != nil {
		return nil, err
	}

	out := &pb.TurnUpdate{CurrentSeq: current}
	if gap {
		// The Telegram updates.tooLong case: stop replaying, take the state.
		metrics.TurnSyncs.WithLabelValues("full_resync").Inc()
		out.FullResync = true
		out.State = st.Project(viewer)
		return out, nil
	}
	metrics.TurnSyncs.WithLabelValues("diff").Inc()
	out.State = st.Project(viewer)
	for _, e := range events {
		out.Events = append(out.Events, e.Project(viewer).toPB())
	}
	return out, nil
}

// rejectReason keeps the metric label set small and stable — one bounded value
// per rule, never the raw error text.
func rejectReason(err error) string {
	switch {
	case errors.Is(err, ErrNotYourTurn):
		return "not_your_turn"
	case errors.Is(err, ErrStaleTurn):
		return "stale_turn"
	case errors.Is(err, ErrCardNotHeld):
		return "card_not_held"
	case errors.Is(err, ErrBadCardValue):
		return "bad_card"
	case errors.Is(err, ErrMatchOver):
		return "match_over"
	case errors.Is(err, ErrUnknownSeat):
		return "unknown_seat"
	default:
		return "other"
	}
}

// RunDeadlines sweeps expired turns until ctx is done. Runs on the matchmaker /
// turn role; several replicas may run it, which is why the pop is atomic.
func (s *Service) RunDeadlines(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sweepOnce(ctx)
		}
	}
}

// opCtx bounds one store call. Each gets its own budget rather than the whole
// pass sharing one: a sweep pops up to 64 deadlines and plays every one of them
// out, so a shared deadline would make the last match's timeout depend on how
// slow the first 63 were.
func (s *Service) opCtx(parent context.Context) (context.Context, context.CancelFunc) {
	if s.opTimeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, s.opTimeout)
}

func (s *Service) sweepOnce(parent context.Context) {
	popCtx, cancel := s.opCtx(parent)
	due, err := s.deadlines.PopDue(popCtx, s.now(), 64)
	cancel()
	if err != nil {
		log.Printf("turn: pop deadlines: %v", err)
		return
	}
	for _, d := range due {
		ctx, cancel := s.opCtx(parent)
		err := s.ExpireTurn(ctx, d.MatchID, d.TurnNumber)
		cancel()
		// Losing a race to a real move is the expected happy path, not an error.
		if err != nil &&
			!errors.Is(err, ErrMatchOver) && !errors.Is(err, ErrNoMatch) &&
			!errors.Is(err, ErrStaleTurn) && !errors.Is(err, ErrNotYourTurn) {
			log.Printf("turn: expire %s: %v", d.MatchID, err)
		}
	}
}

// ExpireTurn plays the weakest legal card for whoever is on the clock, but only
// if turnNumber is still the turn on the clock.
//
// That check is the whole reason a deadline carries its turn. A player who
// moves just before their timer runs out leaves an expired entry behind; acting
// on it blindly would spend the *next* player's turn, seconds into a timer that
// had barely started.
//
// The match itself does not pause for a missing player. Pausing would be an
// exploit: anyone losing could pull their network cable to stop the game.
func (s *Service) ExpireTurn(ctx context.Context, matchID string, turnNumber uint32) error {
	st, _, err := s.store.Get(ctx, matchID)
	if err != nil {
		return err
	}
	if st.Ended {
		return nil
	}
	if turnNumber != 0 && st.TurnNumber != turnNumber {
		// Already played. This deadline belongs to a turn that is over.
		return nil
	}
	card := st.LowestCard(st.Turn)
	if card == 0 {
		return nil
	}
	// Pin the turn this deadline was armed for. Without it the sweeper could
	// read the state a moment after a real move landed and auto-play the next
	// turn as well — one timeout consuming two turns, with the second player
	// losing a card they never had a chance to spend.
	return s.apply(ctx, matchID, Move{
		PlayerID:   st.Players[st.Turn],
		Card:       card,
		TurnNumber: st.TurnNumber,
		Auto:       true,
	})
}

// fanout pushes the new events to both players, each seeing their own
// projection of the same log entries.
func (s *Service) fanout(_ context.Context, st *State, events []Event) {
	for _, pid := range st.Players {
		up := &pb.TurnUpdate{CurrentSeq: st.Seq, State: st.Project(pid)}
		for _, e := range events {
			up.Events = append(up.Events, e.Project(pid).toPB())
		}
		s.notify(pid, up)
	}
}
