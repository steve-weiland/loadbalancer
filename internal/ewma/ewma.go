// Package ewma implements an exponentially weighted moving average of request
// latency, with a lazy idle-decay rule that lets a recovered backend re-enter
// the P2C selection set without an explicit reset.
//
// Spec references: LB-30 (smoothing), LB-31 (idle decay).
//
// Design notes:
//
//   - First observation seeds the score with no smoothing (LB-30 final sentence).
//     Otherwise α=0.1 against a zero seed makes a backend look 10× faster than
//     it really is for ~20 requests after startup, which biases P2C toward
//     freshly-started backends.
//
//   - Decay is applied lazily on Value(): we don't run a background goroutine.
//     Decay halves the score per --ewma-alpha-derived "interval" of idleness.
//     The interval = 1 / α requests' worth of clock time, but since we don't
//     track request rate here, we use a simple geometric decay keyed on
//     wall-clock seconds. This is approximate; it's good enough for V2's
//     "recovered backend re-enters within ~10 requests" goal and avoids
//     coupling the EWMA package to the Pool's request rate.
package ewma

import (
	"sync"
	"time"
)

// Score is a thread-safe EWMA latency estimator. Zero value is NOT usable;
// construct via New.
type Score struct {
	alpha float64

	mu        sync.Mutex
	value     float64   // current smoothed latency, in seconds
	lastObs   time.Time // wall-clock time of the last Update
	seeded    bool      // true after the first Update
	decayHalf time.Duration
}

// New returns a Score with the given smoothing factor (0 < alpha <= 1) and a
// half-life for idle decay (typically the configured upstream timeout, so a
// backend that has been silent for 5s loses half its score). Both validated
// by the caller; this constructor will panic on invalid input.
func New(alpha float64, decayHalfLife time.Duration) *Score {
	if alpha <= 0 || alpha > 1 {
		panic("ewma: alpha must be in (0, 1]")
	}
	if decayHalfLife <= 0 {
		panic("ewma: decay half-life must be positive")
	}
	return &Score{alpha: alpha, decayHalf: decayHalfLife}
}

// Update folds a new latency observation into the score (LB-30).
func (s *Score) Update(latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sample := latency.Seconds()
	now := time.Now()

	if !s.seeded {
		s.value = sample
		s.seeded = true
		s.lastObs = now
		return
	}
	s.value = s.alpha*sample + (1-s.alpha)*s.value
	s.lastObs = now
}

// Value returns the current EWMA score, with idle decay applied (LB-31). A
// backend that has been silent for `decayHalf` since its last Update sees its
// score halved; for 2× decayHalf, quartered; and so on.
func (s *Score) Value() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.seeded {
		return 0
	}
	idle := time.Since(s.lastObs)
	if idle <= 0 {
		return time.Duration(s.value * float64(time.Second))
	}
	// Geometric decay: value * 0.5^(idle / decayHalf).
	// Implemented with math.Exp via the natural-log identity to avoid math.Pow's
	// slow path on hot reads.
	decayed := s.value * pow2neg(float64(idle)/float64(s.decayHalf))
	return time.Duration(decayed * float64(time.Second))
}

// Seeded reports whether at least one Update has been folded in. Callers can
// use this in tie-breaking logic ("prefer the unseeded backend to give it a
// chance"), though V2's P2C does not exploit that today.
func (s *Score) Seeded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seeded
}
