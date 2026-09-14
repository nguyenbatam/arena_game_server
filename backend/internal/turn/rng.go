package turn

// splitmix64 — the same generator the arena sim uses, kept local so this
// package does not depend on the realtime simulation just to shuffle a deck.
type rng struct{ s uint64 }

func newRNG(seed uint64) *rng { return &rng{s: seed} }

func (r *rng) Uint64() uint64 {
	r.s += 0x9E3779B97F4A7C15
	z := r.s
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *rng) Uint32() uint32 { return uint32(r.Uint64() >> 32) }
