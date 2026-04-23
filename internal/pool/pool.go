// Package pool owns the static list of backends and the round-robin counter.
//
// V1: blind round-robin via a single shared atomic counter (LB-02). The pool is
// immutable for the process lifetime; nothing here knows or cares whether a
// backend is reachable (LB-04, LB-05).
package pool

import (
	"errors"
	"fmt"
	"net/url"
	"sync/atomic"
)

var ErrEmptyPool = errors.New("pool: at least one backend required")

type Pool struct {
	backends []*url.URL
	counter  atomic.Uint64
}

// New parses raw backend URLs and returns a Pool. Returns ErrEmptyPool if urls
// is empty (LB-07). Each URL must have a scheme and host.
func New(urls []string) (*Pool, error) {
	if len(urls) == 0 {
		return nil, ErrEmptyPool
	}
	parsed := make([]*url.URL, 0, len(urls))
	for _, raw := range urls {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("pool: parse %q: %w", raw, err)
		}
		if u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("pool: backend %q must include scheme and host", raw)
		}
		parsed = append(parsed, u)
	}
	return &Pool{backends: parsed}, nil
}

// Next returns the next backend in round-robin order. Safe for concurrent use.
//
// counter starts at 0 and is post-incremented, so the first call selects
// backends[0], the second backends[1], etc. (LB-02, LB-03).
func (p *Pool) Next() *url.URL {
	n := p.counter.Add(1) - 1
	return p.backends[n%uint64(len(p.backends))]
}

// Backends returns a copy of the configured backend list. Used at startup to
// pre-register Prometheus label sets.
func (p *Pool) Backends() []*url.URL {
	out := make([]*url.URL, len(p.backends))
	copy(out, p.backends)
	return out
}
