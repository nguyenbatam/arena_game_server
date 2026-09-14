package turn

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func redisFor(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func sampleState() *State {
	return NewState("r1", 5, [2]string{alice, bob})
}

// Both Store implementations must behave identically — otherwise switching
// REDIS_ADDR on changes the game's semantics, which is exactly the class of bug
// nobody finds until production.
func forEachStore(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { fn(t, NewMemoryStore(TTL{})) })
	t.Run("redis", func(t *testing.T) { fn(t, NewRedisStore(redisFor(t), TTL{})) })
}

func TestStoreCreateGetCommit(t *testing.T) {
	forEachStore(t, func(t *testing.T, store Store) {
		ctx := context.Background()
		st := sampleState()

		stored, err := store.Create(ctx, st, []Event{
			{Kind: KindDealt, PlayerID: alice, Hand: []uint32{1, 2, 3}, HandCount: 3},
			{Kind: KindDealt, PlayerID: bob, Hand: []uint32{4, 5, 6}, HandCount: 3},
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if len(stored) != 2 || stored[0].Seq != 1 || stored[1].Seq != 2 {
			t.Fatalf("sequence numbers wrong: %+v", stored)
		}

		got, version, err := store.Get(ctx, "r1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if version != 1 {
			t.Fatalf("version = %d, want 1", version)
		}
		if got.Hands[0][0] != st.Hands[0][0] || got.TurnNumber != st.TurnNumber {
			t.Fatalf("state did not round-trip: %+v vs %+v", got, st)
		}

		got.TurnNumber++
		next, err := store.Commit(ctx, got, version, []Event{{Kind: KindPlayed, PlayerID: alice, Card: 7}})
		if err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if len(next) != 1 || next[0].Seq != 3 {
			t.Fatalf("seq should continue at 3, got %+v", next)
		}

		after, version2, _ := store.Get(ctx, "r1")
		if version2 != 2 {
			t.Fatalf("version = %d, want 2", version2)
		}
		if after.TurnNumber != got.TurnNumber {
			t.Fatalf("committed state not visible: %d vs %d", after.TurnNumber, got.TurnNumber)
		}
	})
}

func TestStoreCommitRejectsStaleVersion(t *testing.T) {
	forEachStore(t, func(t *testing.T, store Store) {
		ctx := context.Background()
		st := sampleState()
		if _, err := store.Create(ctx, st, nil); err != nil {
			t.Fatal(err)
		}
		cur, version, _ := store.Get(ctx, "r1")

		if _, err := store.Commit(ctx, cur, version, []Event{{Kind: KindPlayed, Card: 1}}); err != nil {
			t.Fatalf("first commit: %v", err)
		}
		// Second writer still holding the old version must lose.
		if _, err := store.Commit(ctx, cur, version, []Event{{Kind: KindPlayed, Card: 2}}); !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("got %v, want ErrVersionConflict", err)
		}

		evs, _, _, _ := store.Since(ctx, "r1", 0)
		for _, e := range evs {
			if e.Card == 2 {
				t.Fatal("a losing commit left its events behind")
			}
		}
	})
}

// The reason state and log share a Commit: a rejected write must leave neither.
func TestFailedCommitWritesNothing(t *testing.T) {
	forEachStore(t, func(t *testing.T, store Store) {
		ctx := context.Background()
		if _, err := store.Create(ctx, sampleState(), nil); err != nil {
			t.Fatal(err)
		}
		cur, version, _ := store.Get(ctx, "r1")
		cur.TurnNumber = 99

		_, err := store.Commit(ctx, cur, version+7, []Event{{Kind: KindPlayed, Card: 9}})
		if !errors.Is(err, ErrVersionConflict) {
			t.Fatalf("got %v, want ErrVersionConflict", err)
		}

		after, v, _ := store.Get(ctx, "r1")
		if v != version {
			t.Fatalf("version moved to %d on a rejected commit", v)
		}
		if after.TurnNumber == 99 {
			t.Fatal("a rejected commit still wrote the state")
		}
		evs, current, _, _ := store.Since(ctx, "r1", 0)
		if len(evs) != 0 || current != 0 {
			t.Fatalf("a rejected commit still wrote events: %+v (seq %d)", evs, current)
		}
	})
}

func TestStoreSinceAndGap(t *testing.T) {
	forEachStore(t, func(t *testing.T, store Store) {
		ctx := context.Background()
		st := sampleState()
		if _, err := store.Create(ctx, st, []Event{{Kind: KindDealt, PlayerID: alice}}); err != nil {
			t.Fatal(err)
		}

		evs, current, gap, err := store.Since(ctx, "r1", 0)
		if err != nil {
			t.Fatal(err)
		}
		if gap || len(evs) != 1 || current != 1 {
			t.Fatalf("fresh cursor: evs=%d current=%d gap=%v", len(evs), current, gap)
		}

		evs, _, gap, _ = store.Since(ctx, "r1", 1)
		if gap || len(evs) != 0 {
			t.Fatalf("up-to-date cursor should return nothing, got %d (gap=%v)", len(evs), gap)
		}

		// Push the window well past the old cursor.
		cur, version, _ := store.Get(ctx, "r1")
		filler := make([]Event, LogWindow+50)
		for i := range filler {
			filler[i] = Event{Kind: KindPlayed, PlayerID: alice, Card: 1}
		}
		if _, err := store.Commit(ctx, cur, version, filler); err != nil {
			t.Fatal(err)
		}

		_, _, gap, _ = store.Since(ctx, "r1", 1)
		if !gap {
			t.Fatal("a cursor older than the retained window must report a gap")
		}
	})
}

func TestStoreMissingMatch(t *testing.T) {
	forEachStore(t, func(t *testing.T, store Store) {
		ctx := context.Background()
		if _, _, err := store.Get(ctx, "nope"); !errors.Is(err, ErrNoMatch) {
			t.Fatalf("Get: got %v, want ErrNoMatch", err)
		}
		if _, err := store.Commit(ctx, sampleState(), 1, nil); !errors.Is(err, ErrNoMatch) {
			t.Fatalf("Commit: got %v, want ErrNoMatch", err)
		}
	})
}

// Concurrent writers on one match: exactly one may win each version.
func TestStoreCommitIsSerialized(t *testing.T) {
	forEachStore(t, func(t *testing.T, store Store) {
		ctx := context.Background()
		if _, err := store.Create(ctx, sampleState(), nil); err != nil {
			t.Fatal(err)
		}
		cur, version, _ := store.Get(ctx, "r1")

		const writers = 8
		var wg sync.WaitGroup
		var mu sync.Mutex
		wins := 0
		wg.Add(writers)
		for i := 0; i < writers; i++ {
			go func(i int) {
				defer wg.Done()
				st := cur.Clone()
				st.TurnNumber = uint32(100 + i)
				if _, err := store.Commit(ctx, st, version, []Event{{Kind: KindPlayed, Card: uint32(i + 1)}}); err == nil {
					mu.Lock()
					wins++
					mu.Unlock()
				}
			}(i)
		}
		wg.Wait()

		if wins != 1 {
			t.Fatalf("%d writers committed against one version, want exactly 1", wins)
		}
		evs, _, _, _ := store.Since(ctx, "r1", 0)
		if len(evs) != 1 {
			t.Fatalf("log holds %d events, want 1 — losers wrote anyway", len(evs))
		}
	})
}

// ---------------------------------------------------------------------------

func forEachDeadlines(t *testing.T, fn func(t *testing.T, d Deadlines)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { fn(t, NewMemoryDeadlines()) })
	t.Run("redis", func(t *testing.T) { fn(t, NewRedisDeadlines(redisFor(t))) })
}

func TestDeadlinesPopOnlyWhatIsDue(t *testing.T) {
	forEachDeadlines(t, func(t *testing.T, d Deadlines) {
		ctx := context.Background()
		now := time.Now()

		if err := d.Arm(ctx, "past", 1, now.Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := d.Arm(ctx, "future", 1, now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}

		due, err := d.PopDue(ctx, now, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(due) != 1 || due[0].MatchID != "past" || due[0].TurnNumber != 1 {
			t.Fatalf("due = %+v, want just past#1", due)
		}
		// Popping must remove it, or the sweeper would auto-play forever.
		again, _ := d.PopDue(ctx, now, 10)
		if len(again) != 0 {
			t.Fatalf("same deadline popped twice: %+v", again)
		}
	})
}

// The turn number has to survive the round trip, or the sweeper cannot tell a
// timed-out turn from the one that replaced it.
func TestDeadlinesCarryTurnNumber(t *testing.T) {
	forEachDeadlines(t, func(t *testing.T, d Deadlines) {
		ctx := context.Background()
		now := time.Now()
		if err := d.Arm(ctx, "m#weird-id", 4242, now.Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		due, _ := d.PopDue(ctx, now, 10)
		if len(due) != 1 {
			t.Fatalf("expected one entry, got %+v", due)
		}
		// The id itself contains '#', so parsing must split on the last one.
		if due[0].MatchID != "m#weird-id" || due[0].TurnNumber != 4242 {
			t.Fatalf("round-trip lost data: %+v", due[0])
		}
	})
}

// Two sweeper replicas must not both claim one expired turn.
func TestDeadlinePopIsAtomic(t *testing.T) {
	forEachDeadlines(t, func(t *testing.T, d Deadlines) {
		ctx := context.Background()
		now := time.Now()
		const n = 40
		for i := 0; i < n; i++ {
			if err := d.Arm(ctx, "m"+string(rune('a'+i%26))+string(rune('0'+i/26)), uint32(i), now.Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
		}

		var mu sync.Mutex
		seen := map[string]int{}
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					due, err := d.PopDue(ctx, now, 5)
					if err != nil || len(due) == 0 {
						return
					}
					mu.Lock()
					for _, x := range due {
						seen[dueKey(x.MatchID, x.TurnNumber)]++
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		if len(seen) != n {
			t.Fatalf("claimed %d distinct deadlines, want %d", len(seen), n)
		}
		for k, c := range seen {
			if c != 1 {
				t.Fatalf("%s was claimed %d times — two sweepers would both auto-play", k, c)
			}
		}
	})
}

// A match is ephemeral. Without an expiry every match ever played would sit in
// the store forever — the leak that looks fine for a month and then fills the
// instance.
func TestMatchesExpire(t *testing.T) {
	t.Run("redis", func(t *testing.T) {
		rdb := redisFor(t)
		store := NewRedisStore(rdb, TTL{})
		ctx := context.Background()

		st := sampleState()
		if _, err := store.Create(ctx, st, []Event{{Kind: KindDealt, PlayerID: alice}}); err != nil {
			t.Fatal(err)
		}

		// State, cursor and log must expire together: a surviving cursor
		// pointing into a vanished log would report a phantom gap forever.
		for _, key := range []string{stateKey("r1"), seqKey("r1"), logKey("r1")} {
			ttl, err := rdb.TTL(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
			if ttl <= 0 {
				t.Fatalf("%s has no expiry (%v) — matches would accumulate forever", key, ttl)
			}
			if ttl > DefaultLiveTTL {
				t.Fatalf("%s expires in %v, longer than DefaultLiveTTL %v", key, ttl, DefaultLiveTTL)
			}
		}

		// Finishing a match shortens the window.
		cur, version, _ := store.Get(ctx, "r1")
		cur.Ended = true
		if _, err := store.Commit(ctx, cur, version, []Event{{Kind: KindEnded}}); err != nil {
			t.Fatal(err)
		}
		ttl, _ := rdb.TTL(ctx, stateKey("r1")).Result()
		if ttl > DefaultEndedTTL {
			t.Fatalf("finished match still holds a %v expiry, want at most %v", ttl, DefaultEndedTTL)
		}
	})

	t.Run("memory", func(t *testing.T) {
		store := NewMemoryStore(TTL{})
		ctx := context.Background()
		clock := time.Now()
		store.now = func() time.Time { return clock }

		if _, err := store.Create(ctx, sampleState(), nil); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.Get(ctx, "r1"); err != nil {
			t.Fatalf("match should be live: %v", err)
		}

		clock = clock.Add(DefaultLiveTTL + time.Minute)
		if _, _, err := store.Get(ctx, "r1"); !errors.Is(err, ErrNoMatch) {
			t.Fatalf("expired match still readable: %v", err)
		}

		// And the record is actually gone, not just hidden.
		store.mu.Lock()
		n := len(store.m)
		store.mu.Unlock()
		if n != 0 {
			t.Fatalf("%d expired records still held in memory", n)
		}
	})
}

// Creating a match reuses whatever keys its id maps to. If a previous match
// left a log and a cursor behind — same two players meeting again, an id
// derived from the pair — the new match must not inherit them: the state would
// reset to version 1 while the cursor kept counting, and a client syncing from
// zero would replay a game that is already over.
func TestStoreCreateDiscardsPreviousMatchAtSameID(t *testing.T) {
	forEachStore(t, func(t *testing.T, store Store) {
		ctx := context.Background()

		first := sampleState()
		if _, err := store.Create(ctx, first, []Event{
			{Kind: KindPlayed, PlayerID: alice, Card: 9},
			{Kind: KindEnded, Winner: alice},
		}); err != nil {
			t.Fatalf("Create first: %v", err)
		}

		second := sampleState()
		stored, err := store.Create(ctx, second, []Event{
			{Kind: KindDealt, PlayerID: alice, Hand: []uint32{1, 2, 3}, HandCount: 3},
		})
		if err != nil {
			t.Fatalf("Create second: %v", err)
		}
		if len(stored) != 1 || stored[0].Seq != 1 {
			t.Fatalf("second match must start its cursor at 1, got %+v", stored)
		}

		evs, current, gap, err := store.Since(ctx, "r1", 0)
		if err != nil {
			t.Fatalf("Since: %v", err)
		}
		if gap {
			t.Fatal("a cursor at zero on a fresh match is not a gap")
		}
		if current != 1 {
			t.Fatalf("current = %d, want 1", current)
		}
		if len(evs) != 1 || evs[0].Kind != KindDealt {
			t.Fatalf("the previous match's events leaked into the new log: %+v", evs)
		}
	})
}

// The expiry is what decides the Redis bill, so a deployment has to be able to
// size it against its own instance rather than inherit whatever this package
// thought was reasonable. Two stores in one process must also be able to
// disagree — the reason the window lives on the store and not on a package
// variable something has to mutate at startup.
func TestStoreHonoursConfiguredTTL(t *testing.T) {
	rdb := redisFor(t)
	short := NewRedisStore(rdb, TTL{Live: time.Hour, Ended: 90 * time.Second})

	st := NewState("ttl-1", 5, [2]string{alice, bob})
	ctx := context.Background()
	if _, err := short.Create(ctx, st, []Event{{Kind: KindDealt, PlayerID: alice, HandCount: 3}}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	live, err := rdb.TTL(ctx, stateKey("ttl-1")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if live > time.Hour || live < 30*time.Minute {
		t.Fatalf("live match expiry = %v, want the configured hour", live)
	}

	st.Ended = true
	if _, err := short.Commit(ctx, st, 1, []Event{{Kind: KindEnded, Winner: alice}}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	for _, key := range []string{stateKey("ttl-1"), seqKey("ttl-1"), logKey("ttl-1")} {
		got, err := rdb.TTL(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		// All three have to land on the same clock: a cursor outliving the log
		// it points into reports a permanent gap.
		if got > 90*time.Second || got <= 0 {
			t.Fatalf("%s expiry = %v, want the configured 90s", key, got)
		}
	}

	// An unset window falls back to the package default rather than to zero,
	// which would mean "never expires" to Redis.
	if got := (TTL{}).orDefaults(); got.Live != DefaultLiveTTL || got.Ended != DefaultEndedTTL {
		t.Fatalf("zero TTL did not take the defaults: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// Since
//
// The Redis half answers a cursor in one script rather than three sequential
// reads. That bought back two round trips per TURN_SYNC, but the property worth
// pinning is the one the round trips were hiding: the cursor and the log are
// now read at the same instant, so what comes back can never describe two.

// Every event returned has to be newer than the cursor and no newer than the
// current_seq sent with it. Read separately, the sequence was a moment older
// than the log, so a commit landing between the two reads produced events past
// the number the client was told to ack — and the client was handed them again
// on its next sync, forever, since its cursor could never catch up.
func TestSinceNeverReturnsEventsPastItsOwnCursor(t *testing.T) {
	forEachStore(t, func(t *testing.T, store Store) {
		ctx := context.Background()
		st := sampleState()
		if _, err := store.Create(ctx, st, []Event{{Kind: KindDealt, PlayerID: alice}}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 20; i++ {
			cur, version, err := store.Get(ctx, "r1")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Commit(ctx, cur, version, []Event{
				{Kind: KindPlayed, PlayerID: alice, Card: uint32(i%9 + 1)},
			}); err != nil {
				t.Fatal(err)
			}
		}

		for _, since := range []uint64{0, 1, 5, 15, 20} {
			evs, current, gap, err := store.Since(ctx, "r1", since)
			if err != nil {
				t.Fatalf("since=%d: %v", since, err)
			}
			if gap {
				t.Fatalf("since=%d reported a gap inside the window", since)
			}
			for _, e := range evs {
				if e.Seq <= since {
					t.Errorf("since=%d returned an event at seq %d, at or below the cursor", since, e.Seq)
				}
				if e.Seq > current {
					t.Errorf("since=%d returned seq %d past the current_seq %d it was sent with",
						since, e.Seq, current)
				}
			}
			if len(evs) > 0 && evs[len(evs)-1].Seq != current {
				t.Errorf("since=%d: last event is seq %d but current_seq is %d",
					since, evs[len(evs)-1].Seq, current)
			}
		}
	})
}

// Sequence numbers come back in order and without holes: the client applies
// them one after another, and a hole it cannot see is a move that silently
// never happened on its board.
func TestSinceReturnsAContiguousAscendingRun(t *testing.T) {
	forEachStore(t, func(t *testing.T, store Store) {
		ctx := context.Background()
		st := sampleState()
		if _, err := store.Create(ctx, st, []Event{
			{Kind: KindDealt, PlayerID: alice}, {Kind: KindDealt, PlayerID: bob},
		}); err != nil {
			t.Fatal(err)
		}
		cur, version, _ := store.Get(ctx, "r1")
		batch := make([]Event, 6)
		for i := range batch {
			batch[i] = Event{Kind: KindPlayed, PlayerID: bob, Card: uint32(i + 1)}
		}
		if _, err := store.Commit(ctx, cur, version, batch); err != nil {
			t.Fatal(err)
		}

		evs, current, gap, err := store.Since(ctx, "r1", 2)
		if err != nil || gap {
			t.Fatalf("since=2: err=%v gap=%v", err, gap)
		}
		if len(evs) != 6 {
			t.Fatalf("got %d events after cursor 2, want 6", len(evs))
		}
		for i, e := range evs {
			if want := uint64(i + 3); e.Seq != want {
				t.Fatalf("event %d has seq %d, want %d (got %v)", i, e.Seq, want, seqsOfEvents(evs))
			}
		}
		if current != 8 {
			t.Fatalf("current_seq = %d, want 8", current)
		}
	})
}

func seqsOfEvents(evs []Event) []uint64 {
	out := make([]uint64, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Seq)
	}
	return out
}

// A cursor past the window is answered without reading the range at all — the
// script decides it server-side, which is the whole reason the gap check moved
// into Lua. What must hold from out here is that the caller gets no events with
// the gap, so it cannot half-apply a diff it was told not to trust.
func TestSinceReturnsNoEventsAlongsideAGap(t *testing.T) {
	forEachStore(t, func(t *testing.T, store Store) {
		ctx := context.Background()
		st := sampleState()
		if _, err := store.Create(ctx, st, []Event{{Kind: KindDealt, PlayerID: alice}}); err != nil {
			t.Fatal(err)
		}
		cur, version, _ := store.Get(ctx, "r1")
		filler := make([]Event, LogWindow+50)
		for i := range filler {
			filler[i] = Event{Kind: KindPlayed, PlayerID: alice, Card: 1}
		}
		if _, err := store.Commit(ctx, cur, version, filler); err != nil {
			t.Fatal(err)
		}

		evs, current, gap, err := store.Since(ctx, "r1", 1)
		if err != nil {
			t.Fatal(err)
		}
		if !gap {
			t.Fatal("a cursor older than the retained window must report a gap")
		}
		if len(evs) != 0 {
			t.Fatalf("a gap came with %d events; the caller must not be able to half-apply them", len(evs))
		}
		if current == 0 {
			t.Fatal("a gap still has to name the sequence to resync to")
		}
	})
}

// An up-to-date cursor on a match with no log yet is up to date, not a gap.
// That distinction is the difference between a quiet client and one told to
// throw its state away and start again.
func TestSinceOnAMatchWithNoEvents(t *testing.T) {
	forEachStore(t, func(t *testing.T, store Store) {
		ctx := context.Background()
		if _, err := store.Create(ctx, sampleState(), nil); err != nil {
			t.Fatal(err)
		}
		evs, current, gap, err := store.Since(ctx, "r1", 0)
		if err != nil {
			t.Fatal(err)
		}
		if gap || len(evs) != 0 || current != 0 {
			t.Fatalf("empty log at cursor 0: evs=%d current=%d gap=%v", len(evs), current, gap)
		}
		if _, _, gap, _ = store.Since(ctx, "r1", 3); !gap {
			t.Fatal("a cursor ahead of an empty log cannot be diffed from and must report a gap")
		}
	})
}
