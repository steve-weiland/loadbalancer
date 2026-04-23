// Package admin exposes /healthz and /metrics on a separate listener so
// observability endpoints cannot be starved by data-plane upstream issues.
package admin

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewMux returns a mux serving:
//
//	GET /healthz   200 ok        (LB-17; says nothing about backends)
//	GET /metrics   Prometheus    (LB-19)
func NewMux(reg prometheus.Gatherer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}
