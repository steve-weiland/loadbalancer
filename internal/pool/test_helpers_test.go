package pool

import "math/rand"

// newDeterministicRng returns a small *rand.Rand for unit tests that need a
// stable sequence without disturbing the package-level RNG.
func newDeterministicRng() *rand.Rand {
	return rand.New(rand.NewSource(1))
}
