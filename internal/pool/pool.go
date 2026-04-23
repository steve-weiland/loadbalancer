// Package pool owns the static slice of *backend.Backend and the V2
// power-of-two-choices selection algorithm.
//
// V1's atomic counter is gone. V2's selection (LB-02):
//
//  1. Pick two distinct backends uniformly at random from the EligiblePrimary
//     set. Route to the one with the lower EWMA score.
//  2. If only one EligiblePrimary backend exists, return it (LB-21).
//  3. If zero EligiblePrimary, return any EligibleProbe (Half-open) backend.
//  4. If neither exists, return ErrNoEligible (caller maps to 503 per LB-22).
//
// Pick is the entry point. PickAvoiding is the retry-loop variant: same
// algorithm, but excludes a previously-failed backend (LB-43).
package pool

import (
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"sync"

	"github.com/steve-weiland/loadbalancer/internal/backend"
)

var (
	ErrEmptyPool   = errors.New("pool: at least one backend required")
	ErrNoEligible  = errors.New("pool: no eligible backends")
)

// BackendFactory turns a parsed URL into a fully-constructed *Backend with
// its own EWMA and breaker. The cmd layer supplies this so the pool doesn't
// need to know about EWMA alpha, breaker windows, etc.
type BackendFactory func(*url.URL) *backend.Backend

type Pool struct {
	backends []*backend.Backend

	// rng is per-Pool to keep selection deterministic in tests; serialized
	// under mu because *rand.Rand is not safe for concurrent use.
	mu  sync.Mutex
	rng *rand.Rand
}

// New parses the URLs and constructs one Backend per URL via factory. Returns
// ErrEmptyPool if urls is empty (LB-07). The optional seed sets the RNG used
// for P2C; pass 0 to use a time-derived seed.
func New(urls []string, factory BackendFactory, seed int64) (*Pool, error) {
	if len(urls) == 0 {
		return nil, ErrEmptyPool
	}
	if factory == nil {
		return nil, errors.New("pool: factory must not be nil")
	}
	bs := make([]*backend.Backend, 0, len(urls))
	for _, raw := range urls {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("pool: parse %q: %w", raw, err)
		}
		if u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("pool: backend %q must include scheme and host", raw)
		}
		bs = append(bs, factory(u))
	}
	if seed == 0 {
		seed = rand.Int63()
	}
	return &Pool{
		backends: bs,
		rng:      rand.New(rand.NewSource(seed)),
	}, nil
}

// Backends returns a copy of the backend slice. Used at startup to register
// metric labels and at /metrics scrape time to read live state.
func (p *Pool) Backends() []*backend.Backend {
	out := make([]*backend.Backend, len(p.backends))
	copy(out, p.backends)
	return out
}

// EligibleCount returns the number of backends in EligiblePrimary OR
// EligibleProbe state. Drives lb_eligible_backends.
func (p *Pool) EligibleCount() int {
	n := 0
	for _, b := range p.backends {
		if b.Eligibility() != backend.IneligibleOpen {
			n++
		}
	}
	return n
}

// Pick returns the next backend per LB-02. Returns ErrNoEligible if none.
func (p *Pool) Pick() (*backend.Backend, error) {
	return p.PickAvoiding(nil)
}

// PickAvoiding returns the next backend per LB-02 / LB-43, excluding `avoid`
// if non-nil. Used by the proxy retry loop to ensure each retry hits a
// different backend than the one that just failed.
func (p *Pool) PickAvoiding(avoid *backend.Backend) (*backend.Backend, error) {
	primaries, probes := p.partition(avoid)

	switch len(primaries) {
	case 0:
		// No Closed backends. Try a Half-open probe (LB-06).
		if len(probes) == 0 {
			return nil, ErrNoEligible
		}
		// Just take the first probe; at most one will allow itself through.
		return probes[0], nil
	case 1:
		// Only one primary; route to it without invoking P2C (LB-21).
		return primaries[0], nil
	}

	// Two or more primaries → P2C: pick two distinct random indices, return
	// the one with the lower EWMA score.
	p.mu.Lock()
	a, b := pickTwo(p.rng, len(primaries))
	p.mu.Unlock()

	x, y := primaries[a], primaries[b]
	if x.EWMA.Value() <= y.EWMA.Value() {
		return x, nil
	}
	return y, nil
}

// partition splits the backend list into primary (Closed) and probe (Half-open)
// candidates, omitting `avoid`. Open backends are always omitted.
func (p *Pool) partition(avoid *backend.Backend) (primaries, probes []*backend.Backend) {
	primaries = make([]*backend.Backend, 0, len(p.backends))
	probes = make([]*backend.Backend, 0, len(p.backends))
	for _, b := range p.backends {
		if b == avoid {
			continue
		}
		switch b.Eligibility() {
		case backend.EligiblePrimary:
			primaries = append(primaries, b)
		case backend.EligibleProbe:
			probes = append(probes, b)
		}
	}
	return primaries, probes
}

// pickTwo returns two distinct indices in [0, n). n MUST be >= 2.
func pickTwo(rng *rand.Rand, n int) (int, int) {
	a := rng.Intn(n)
	b := rng.Intn(n - 1)
	if b >= a {
		b++
	}
	return a, b
}
