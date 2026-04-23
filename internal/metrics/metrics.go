// Package metrics exposes the proxy's Prometheus collectors behind a small
// Observer interface so the proxy package does not import client_golang
// directly.
package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Observer is the surface the proxy uses to record one upstream call.
type Observer interface {
	Observe(backend string, status int, latency time.Duration)
}

// Noop discards observations. Useful in tests.
type Noop struct{}

func (Noop) Observe(string, int, time.Duration) {}

// PromObserver implements Observer using two Prometheus collectors:
//
//	lb_requests_total{backend, status}      counter
//	lb_upstream_latency_seconds{backend}    histogram
type PromObserver struct {
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
}

// New registers the collectors and pre-creates label sets for every backend so
// the first request never pays the registration cost.
func New(reg prometheus.Registerer, backends []string) *PromObserver {
	requests := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lb_requests_total",
			Help: "Total proxied requests by backend and upstream status code.",
		},
		[]string{"backend", "status"},
	)
	latency := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lb_upstream_latency_seconds",
			Help:    "Upstream response latency in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"backend"},
	)
	reg.MustRegister(requests, latency)

	for _, b := range backends {
		latency.WithLabelValues(b)
	}
	return &PromObserver{requests: requests, latency: latency}
}

func (m *PromObserver) Observe(backend string, status int, latency time.Duration) {
	m.requests.WithLabelValues(backend, strconv.Itoa(status)).Inc()
	m.latency.WithLabelValues(backend).Observe(latency.Seconds())
}
