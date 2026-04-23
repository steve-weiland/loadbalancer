// Package backend defines the per-upstream domain object that owns the
// EWMA latency score and the circuit breaker. Pool composes []*Backend; the
// proxy never sees a raw *url.URL after V2.
//
// A Backend is allocated once at startup and lives for the lifetime of the
// process. The pool is static (LB-16); there is no add/remove path.
package backend

import (
	"net/url"

	"github.com/steve-weiland/loadbalancer/internal/breaker"
	"github.com/steve-weiland/loadbalancer/internal/ewma"
)

type Backend struct {
	URL     *url.URL
	EWMA    *ewma.Score
	Breaker *breaker.Breaker
}

// New constructs a Backend. The caller MUST pre-validate url, ewma, and
// breaker; this constructor performs no validation beyond nil checks.
func New(u *url.URL, e *ewma.Score, br *breaker.Breaker) *Backend {
	if u == nil || e == nil || br == nil {
		panic("backend: nil dependency")
	}
	return &Backend{URL: u, EWMA: e, Breaker: br}
}

// String returns the backend's URL as a string. Used for log fields, metric
// labels, and the access log's backend_chain.
func (b *Backend) String() string { return b.URL.String() }

// Eligibility reports how the backend may participate in selection.
type Eligibility int

const (
	// IneligibleOpen — breaker is Open; the backend is not selectable.
	IneligibleOpen Eligibility = iota
	// EligiblePrimary — breaker is Closed; freely selectable by P2C.
	EligiblePrimary
	// EligibleProbe — breaker is Half-open; the backend may be selected ONLY
	// when no primary candidate exists, and only one probe at a time.
	EligibleProbe
)

// Eligibility classifies the backend's selectability based on the current
// breaker state. The pool uses this to partition candidates before P2C.
//
// Note: this is a *snapshot* at call time — between Eligibility() returning
// EligibleProbe and the proxy actually issuing the request, another caller
// could have claimed the half-open probe. The pool's PickAvoiding handles
// that case by re-checking with breaker.Allow at dispatch.
func (b *Backend) Eligibility() Eligibility {
	switch b.Breaker.State() {
	case breaker.Closed:
		return EligiblePrimary
	case breaker.HalfOpen:
		return EligibleProbe
	default:
		return IneligibleOpen
	}
}
