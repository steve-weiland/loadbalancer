package ewma

import "math"

// pow2neg returns 2^-x. Hot-path helper for the idle-decay calculation; using
// math.Exp(-x*ln2) is measurably faster than math.Pow(2, -x) on amd64/arm64.
func pow2neg(x float64) float64 {
	return math.Exp(-x * math.Ln2)
}
