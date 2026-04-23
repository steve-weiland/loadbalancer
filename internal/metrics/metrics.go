// Package metrics exposes the proxy's Prometheus collectors behind a small
// Observer interface so the proxy package does not import client_golang
// directly.
//
// V2 expands the surface from V1's two collectors to nine, covering retries,
// breaker state machines, EWMA scores, and the retry budget.
package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/steve-weiland/loadbalancer/internal/backend"
	"github.com/steve-weiland/loadbalancer/internal/breaker"
)

// RetryReason classifies why a retry was issued. Matches the `reason` label
// of lb_retries_total. Maps directly from the per-attempt outcome.
type RetryReason string

const (
	RetryReasonTransport RetryReason = "transport"
	RetryReason5xx       RetryReason = "5xx"
	RetryReasonTimeout   RetryReason = "timeout"
)

// Observer is the surface the proxy uses. The proxy must not import
// client_golang directly.
type Observer interface {
	// Observe records the *terminal* (post-retry) outcome of one client request.
	Observe(backend string, status int, latency time.Duration)
	// RecordRetry records that a retry was issued from one backend to another.
	RecordRetry(from, to string, reason RetryReason)
	// RecordBreakerTransition records a breaker state change for a backend.
	RecordBreakerTransition(backend string, from, to breaker.State)
	// RecordBudgetExhaustion records one TryConsume that returned false.
	RecordBudgetExhaustion()
}

// Noop discards observations. Useful in tests.
type Noop struct{}

func (Noop) Observe(string, int, time.Duration)                {}
func (Noop) RecordRetry(string, string, RetryReason)           {}
func (Noop) RecordBreakerTransition(string, breaker.State, breaker.State) {}
func (Noop) RecordBudgetExhaustion()                           {}

// PromObserver implements Observer using a fixed set of Prometheus collectors.
// Constructed once at startup; safe for concurrent use.
type PromObserver struct {
	requests           *prometheus.CounterVec
	latency            *prometheus.HistogramVec
	retries            *prometheus.CounterVec
	transitions        *prometheus.CounterVec
	budgetExhausted    prometheus.Counter
}

// New registers all V2 collectors and pre-creates label sets for every backend
// so the first request never pays the registration cost. Pass the live pool's
// backends so the gauges can read their current state on each /metrics scrape.
func New(reg prometheus.Registerer, backends []*backend.Backend) *PromObserver {
	requests := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lb_requests_total",
			Help: "Total proxied requests by terminal backend and upstream status.",
		},
		[]string{"backend", "status"},
	)
	latency := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lb_upstream_latency_seconds",
			Help:    "Upstream response latency in seconds (terminal attempt).",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"backend"},
	)
	retries := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lb_retries_total",
			Help: "Cross-backend retries by source, destination, and reason.",
		},
		[]string{"from_backend", "to_backend", "reason"},
	)
	transitions := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lb_breaker_transitions_total",
			Help: "Per-backend circuit-breaker state transitions.",
		},
		[]string{"backend", "from", "to"},
	)
	budgetExhausted := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lb_retry_budget_exhausted_total",
		Help: "Requests where retry was skipped because the per-second budget was exhausted.",
	})

	reg.MustRegister(requests, latency, retries, transitions, budgetExhausted)

	// Pre-register label sets for known backends so /metrics shows zero-valued
	// series before any traffic arrives.
	for _, b := range backends {
		latency.WithLabelValues(b.String())
	}

	// Gauges sourced from live backend state — registered as GaugeFuncs so
	// every scrape reads the current value rather than relying on a publish
	// path on every state change.
	registerGauges(reg, backends)

	return &PromObserver{
		requests:        requests,
		latency:         latency,
		retries:         retries,
		transitions:     transitions,
		budgetExhausted: budgetExhausted,
	}
}

func (m *PromObserver) Observe(b string, status int, latency time.Duration) {
	m.requests.WithLabelValues(b, strconv.Itoa(status)).Inc()
	m.latency.WithLabelValues(b).Observe(latency.Seconds())
}

func (m *PromObserver) RecordRetry(from, to string, reason RetryReason) {
	m.retries.WithLabelValues(from, to, string(reason)).Inc()
}

func (m *PromObserver) RecordBreakerTransition(b string, from, to breaker.State) {
	m.transitions.WithLabelValues(b, from.String(), to.String()).Inc()
}

func (m *PromObserver) RecordBudgetExhaustion() {
	m.budgetExhausted.Inc()
}

// registerGauges installs lb_breaker_state{backend}, lb_ewma_score_seconds{backend},
// and lb_eligible_backends as GaugeFuncs. Each is read fresh on /metrics scrape.
func registerGauges(reg prometheus.Registerer, backends []*backend.Backend) {
	// Per-backend breaker state and EWMA score: one GaugeFunc per backend per
	// metric. Using a closure over each backend keeps the source of truth
	// where it lives.
	for _, b := range backends {
		bb := b // capture
		reg.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name:        "lb_breaker_state",
				Help:        "Per-backend circuit-breaker state (0=Closed, 1=Open, 2=Half-open).",
				ConstLabels: prometheus.Labels{"backend": bb.String()},
			},
			func() float64 { return float64(bb.Breaker.State()) },
		))
		reg.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name:        "lb_ewma_score_seconds",
				Help:        "Per-backend EWMA latency score in seconds.",
				ConstLabels: prometheus.Labels{"backend": bb.String()},
			},
			func() float64 { return bb.EWMA.Value().Seconds() },
		))
	}

	// Pool-wide eligible-count gauge: closure over the static slice.
	bs := backends
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "lb_eligible_backends",
			Help: "Count of currently eligible (non-Open) backends.",
		},
		func() float64 {
			n := 0
			for _, b := range bs {
				if b.Eligibility() != backend.IneligibleOpen {
					n++
				}
			}
			return float64(n)
		},
	))
}
