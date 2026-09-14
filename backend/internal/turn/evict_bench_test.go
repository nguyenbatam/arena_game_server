package turn

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkCommitWithManyLiveMatches measures the mode's only write path
// against a store that is actually holding matches.
//
// A move used to pay for a full walk of the match map, under the store lock, on
// every commit — so the cost of one player's turn was set by how busy the
// process was, and running N matches to completion was O(N²). This is the
// benchmark that shows it: raise the fill and the per-op time should not move.
func BenchmarkCommitWithManyLiveMatches(b *testing.B) {
	for _, fill := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("live=%d", fill), func(b *testing.B) {
			ctx := context.Background()
			store := NewMemoryStore(TTL{Live: time.Hour, Ended: time.Hour})
			for i := 0; i < fill; i++ {
				st := NewState(fmt.Sprintf("bg-%06d", i), int64(i), [2]string{alice, bob})
				if _, err := store.Create(ctx, st, nil); err != nil {
					b.Fatal(err)
				}
			}
			// One match played over and over, so the measurement is the commit
			// and not the deal.
			hot := NewState("hot", 1, [2]string{alice, bob})
			if _, err := store.Create(ctx, hot, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				st, version, err := store.Get(ctx, "hot")
				if err != nil {
					b.Fatal(err)
				}
				if _, err := store.Commit(ctx, st, version, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
