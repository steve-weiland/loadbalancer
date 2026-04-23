// Package admin exposes /healthz and /metrics on a separate listener so
// observability endpoints cannot be starved by data-plane upstream issues.
package admin

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DrainCheck reports whether the proxy is in the drain phase. When it returns
// true, /healthz responds 503 with body "draining" (LB-27) so external load
// balancers and Kubernetes readiness probes stop sending new traffic.
//
// nil is treated as "never draining" — equivalent to the V2.0 behaviour.
type DrainCheck func() bool

// NewMux returns a mux serving:
//
//	GET /healthz   200 "ok"   (LB-17) or 503 "draining" (LB-27) when draining
//	GET /metrics   Prometheus (LB-19)
//
// drain may be nil; non-nil callers (cmd/lbserver) flip it to true at the
// start of SIGTERM handling.
func NewMux(reg prometheus.Gatherer, drain DrainCheck) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if drain != nil && drain() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("draining"))
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}
