// Package redistest hands tests a throwaway in-process Redis.
//
// Every Redis-backed implementation in this repo has a memory twin selected by
// one env var, and the two are supposed to behave identically. Without a way to
// exercise the Redis side in tests, that promise — and every Lua script holding
// it up — goes unverified until production.
package redistest

import (
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Client starts a miniredis and returns a client wired to it, both torn down
// when the test ends.
func Client(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		// A test that closed the client itself is doing so on purpose — that is
		// how the "store is unreachable" branches get exercised — so a second
		// close reporting exactly that is expected. Anything else is a resource
		// this test did not give back, which is worth failing over.
		if err := rdb.Close(); err != nil && !errors.Is(err, redis.ErrClosed) {
			t.Errorf("redistest: close client: %v", err)
		}
	})
	return rdb
}
