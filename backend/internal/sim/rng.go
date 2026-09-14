package sim

// RNG is a splitmix64 generator. Same seed + same call sequence => same stream
// on every machine. Do not use math/rand in the sim goroutine.
type RNG struct {
	s uint64
}

func NewRNG(seed uint64) RNG {
	if seed == 0 {
		seed = 0x9E3779B97F4A7C15
	}
	return RNG{s: seed}
}

func (r *RNG) Uint64() uint64 {
	r.s += 0x9E3779B97F4A7C15
	z := r.s
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *RNG) Uint32() uint32 {
	return uint32(r.Uint64())
}
