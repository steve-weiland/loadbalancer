package admin

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestAdmin_HealthzAlwaysOK(t *testing.T) {
	mux := NewMux(prometheus.NewRegistry(), nil)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Errorf("body: got %q, want %q", body, "ok")
	}
}

// LB-27: while draining, /healthz returns 503 with body "draining" so external
// LBs stop sending traffic. The endpoint must still serve (not refuse).
func TestAdmin_HealthzDrainingReturns503(t *testing.T) {
	var draining atomic.Bool
	mux := NewMux(prometheus.NewRegistry(), draining.Load)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Initially: ok.
	resp, _ := http.Get(srv.URL + "/healthz")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Errorf("pre-drain: status=%d body=%q want 200 ok", resp.StatusCode, body)
	}

	// Flip drain → 503 draining.
	draining.Store(true)
	resp, _ = http.Get(srv.URL + "/healthz")
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("draining: status=%d, want 503", resp.StatusCode)
	}
	if string(body) != "draining" {
		t.Errorf("draining: body=%q, want %q", body, "draining")
	}
}

func TestAdmin_MetricsExposesRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_metric_total", Help: "x"})
	reg.MustRegister(c)
	c.Inc()

	mux := NewMux(reg, nil)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "test_metric_total 1") {
		t.Errorf("metrics output missing registered counter; body:\n%s", body)
	}
}
