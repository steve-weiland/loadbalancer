// Package breaker implements the per-backend circuit breaker described in
// spec §3.4 (LB-32..36).
//
// State machine:
//
//	Closed   →  errors/window > threshold      →  Open
//	Open     →  resetTimeout elapsed           →  Half-open
//	Half-open → probe success                  →  Closed (reset timeout to base)
//	Half-open → probe failure                  →  Open (timeout doubled, capped)
//
// The breaker is updated *passively* from in-band request results (LB-38);
// it knows nothing about backends, HTTP, or background goroutines.
package breaker

import (
	"sync"
	"time"
)

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	}
	return "unknown"
}

// Config sets the breaker's tuning parameters. All four come from CLI flags
// at the cmd/lbserver layer (LB-50). Callers SHOULD validate ranges before
// constructing.
type Config struct {
	Window         int           // sliding window size; must be >= 1
	ErrorThreshold float64       // 0..1; if errors/window > threshold → Open
	ResetTimeout   time.Duration // initial Open → Half-open delay (LB-34)
	ResetCap       time.Duration // maximum Open → Half-open delay (LB-36)
}

// Breaker tracks state for a single backend.
//
// Concurrency: a single mutex guards everything. Allow / Record are called
// once per request on the proxy hot path; the critical sections are tiny
// (constant-time arithmetic on a small ring buffer), so this is fine. If
// profiles ever show contention here, swap the ring for an atomic counter
// pair.
type Breaker struct {
	cfg   Config
	now   func() time.Time // injectable clock for tests
	notif TransitionFunc

	mu             sync.Mutex
	state          State
	results        []bool // ring buffer of last N results; true = error
	results_idx    int
	results_filled int     // up to len(results)
	openedAt       time.Time
	currentTimeout time.Duration
	halfOpenInUse  bool // a probe is currently in flight
}

// TransitionFunc is fired on every state transition. Implementations MUST be
// fast and non-blocking (called under the breaker's mutex).
type TransitionFunc func(from, to State)

// New constructs a Breaker in the Closed state.
func New(cfg Config, opts ...Option) *Breaker {
	b := &Breaker{
		cfg:            cfg,
		now:            time.Now,
		state:          Closed,
		results:        make([]bool, cfg.Window),
		currentTimeout: cfg.ResetTimeout,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Option configures a Breaker at construction time.
type Option func(*Breaker)

// WithClock injects a clock for tests.
func WithClock(now func() time.Time) Option {
	return func(b *Breaker) { b.now = now }
}

// WithOnTransition installs a callback fired on every state transition.
func WithOnTransition(fn TransitionFunc) Option {
	return func(b *Breaker) { b.notif = fn }
}

// Allow returns true if the breaker permits a request through right now.
//
//   - Closed: always allow.
//   - Open: allow iff resetTimeout has elapsed since opening, in which case
//     transition to Half-open and admit the request as the probe.
//   - Half-open: allow exactly one probe at a time (LB-36). Subsequent callers
//     see false until the probe records its result.
//
// Allow MUST be paired with exactly one Record call describing the outcome.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case Closed:
		return true
	case Open:
		if b.now().Sub(b.openedAt) < b.currentTimeout {
			return false
		}
		// Reset timeout elapsed → Half-open and admit the probe.
		b.transition(HalfOpen)
		b.halfOpenInUse = true
		return true
	case HalfOpen:
		if b.halfOpenInUse {
			return false
		}
		b.halfOpenInUse = true
		return true
	}
	return false
}

// Record folds the outcome of one request into the breaker's state machine.
// success=true counts as a non-error; success=false as an error per LB-37
// (callers map "transport error", "TTFB timeout", and "5xx response" to
// success=false; 4xx maps to success=true).
func (b *Breaker) Record(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case HalfOpen:
		b.halfOpenInUse = false
		if success {
			// Probe succeeded → Closed, reset timeout to base, clear window.
			b.currentTimeout = b.cfg.ResetTimeout
			b.clearWindow()
			b.transition(Closed)
		} else {
			// Probe failed → back to Open with doubled timeout (capped).
			b.currentTimeout *= 2
			if b.currentTimeout > b.cfg.ResetCap {
				b.currentTimeout = b.cfg.ResetCap
			}
			b.openedAt = b.now()
			b.transition(Open)
		}
		return

	case Closed:
		b.recordResult(!success)
		if b.results_filled >= b.cfg.Window && b.errorRate() > b.cfg.ErrorThreshold {
			b.openedAt = b.now()
			b.transition(Open)
		}

	case Open:
		// Result recorded for an Open breaker is unusual (would mean a request
		// slipped through). Ignore it; the breaker's job is to gate selection.
	}
}

// State returns the current breaker state. For metrics use.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// --- internal helpers (call with mu held) ---

func (b *Breaker) recordResult(isError bool) {
	b.results[b.results_idx] = isError
	b.results_idx = (b.results_idx + 1) % b.cfg.Window
	if b.results_filled < b.cfg.Window {
		b.results_filled++
	}
}

func (b *Breaker) errorRate() float64 {
	if b.results_filled == 0 {
		return 0
	}
	errs := 0
	for i := 0; i < b.results_filled; i++ {
		if b.results[i] {
			errs++
		}
	}
	return float64(errs) / float64(b.results_filled)
}

func (b *Breaker) clearWindow() {
	for i := range b.results {
		b.results[i] = false
	}
	b.results_idx = 0
	b.results_filled = 0
}

func (b *Breaker) transition(to State) {
	from := b.state
	if from == to {
		return
	}
	b.state = to
	if b.notif != nil {
		b.notif(from, to)
	}
}
