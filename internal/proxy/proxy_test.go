package proxy

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steve-weiland/loadbalancer/internal/backend"
	"github.com/steve-weiland/loadbalancer/internal/breaker"
	"github.com/steve-weiland/loadbalancer/internal/ewma"
	"github.com/steve-weiland/loadbalancer/internal/metrics"
	"github.com/steve-weiland/loadbalancer/internal/pool"
	"github.com/steve-weiland/loadbalancer/internal/retrybudget"
)

func defaultBreakerCfg() breaker.Config {
	return breaker.Config{
		Window:         10,
		ErrorThreshold: 0.5,
		ResetTimeout:   100 * time.Millisecond,
		ResetCap:       1 * time.Second,
	}
}

func defaultFactory() pool.BackendFactory {
	return func(u *url.URL) *backend.Backend {
		return backend.New(u, ewma.New(0.1, time.Second), breaker.New(defaultBreakerCfg()))
	}
}

func newProxy(t *testing.T, urls []string, cfg Config) *Proxy {
	t.Helper()
	p, err := pool.New(urls, defaultFactory(), 42)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	// Tests default to a high budget so the retry path isn't budget-limited
	// unless a test specifically wants to exercise that.
	return New(p, cfg, retrybudget.New(1.0), nil, nil)
}

// ----- preserved behaviour from V1 (LB-08..13) -----

func TestProxy_ForwardsMethodPathBodyHeaders(t *testing.T) {
	type seen struct {
		Method string
		Path   string
		Query  string
		Body   string
		Custom string
	}
	var got seen
	bk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = seen{r.Method, r.URL.Path, r.URL.RawQuery, string(body), r.Header.Get("X-Custom")}
		_, _ = w.Write([]byte("ok"))
	}))
	defer bk.Close()

	p := newProxy(t, []string{bk.URL}, Config{UpstreamTimeout: 5 * time.Second, MaxRetries: 0})
	srv := httptest.NewServer(p)
	defer srv.Close()

	req, _ := http.NewRequest("PUT", srv.URL+"/foo/bar?x=1", strings.NewReader("hello"))
	req.Header.Set("X-Custom", "yes")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	want := seen{"PUT", "/foo/bar", "x=1", "hello", "yes"}
	if got != want {
		t.Errorf("forwarded: got %+v want %+v", got, want)
	}
}

func TestProxy_StripsHopByHopHeaders(t *testing.T) {
	var saw http.Header
	bk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		saw = r.Header.Clone()
	}))
	defer bk.Close()

	p := newProxy(t, []string{bk.URL}, Config{UpstreamTimeout: 5 * time.Second, MaxRetries: 0})
	srv := httptest.NewServer(p)
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/", nil)
	req.Header.Set("Connection", "X-Per-Hop, close")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("Proxy-Authorization", "Basic xyz")
	req.Header.Set("X-Per-Hop", "should-be-stripped")
	req.Header.Set("X-Keep-Me", "yes")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	for _, h := range []string{"Connection", "Keep-Alive", "Proxy-Authorization", "X-Per-Hop"} {
		if v := saw.Get(h); v != "" {
			t.Errorf("expected %q stripped, got %q", h, v)
		}
	}
	if v := saw.Get("X-Keep-Me"); v != "yes" {
		t.Errorf("end-to-end header lost: %q", v)
	}
}

func TestProxy_AppendsXForwardedFor(t *testing.T) {
	var saw http.Header
	bk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		saw = r.Header.Clone()
	}))
	defer bk.Close()

	p := newProxy(t, []string{bk.URL}, Config{UpstreamTimeout: 5 * time.Second, MaxRetries: 0})
	srv := httptest.NewServer(p)
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/", nil)
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	xff := saw.Get("X-Forwarded-For")
	if !strings.HasPrefix(xff, "10.0.0.1, ") {
		t.Errorf("X-Forwarded-For not appended: got %q", xff)
	}
	if proto := saw.Get("X-Forwarded-Proto"); proto != "http" {
		t.Errorf("X-Forwarded-Proto: got %q want http", proto)
	}
}

func TestProxy_ForwardsResponseUnchanged(t *testing.T) {
	bk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend-Header", "v1")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("body-bytes"))
	}))
	defer bk.Close()

	p := newProxy(t, []string{bk.URL}, Config{UpstreamTimeout: 5 * time.Second, MaxRetries: 0})
	srv := httptest.NewServer(p)
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("status: got %d want 418", resp.StatusCode)
	}
	if resp.Header.Get("X-Backend-Header") != "v1" {
		t.Error("response header lost")
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "body-bytes" {
		t.Errorf("body: got %q", body)
	}
}

func TestProxy_ConnRefusedReturns502(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	deadURL := "http://" + l.Addr().String()
	l.Close()

	p := newProxy(t, []string{deadURL}, Config{UpstreamTimeout: 1 * time.Second, MaxRetries: 0})
	srv := httptest.NewServer(p)
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status: got %d want 502", resp.StatusCode)
	}
	var body errorBody
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "upstream connection failed" {
		t.Errorf("error: got %q", body.Error)
	}
	if body.Backend != deadURL {
		t.Errorf("backend label: got %q want %q", body.Backend, deadURL)
	}
}

func TestProxy_SlowBackendReturns504(t *testing.T) {
	bk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer bk.Close()

	p := newProxy(t, []string{bk.URL}, Config{UpstreamTimeout: 50 * time.Millisecond, MaxRetries: 0})
	srv := httptest.NewServer(p)
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("status: got %d want 504", resp.StatusCode)
	}
}

// ----- V2-specific behaviour -----

// LB-40 + LB-41: GET 5xx → cross-backend retry succeeds.
func TestProxy_RetriesOn5xxAcrossBackends(t *testing.T) {
	var bk1Hits, bk2Hits atomic.Int64
	bk1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bk1Hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bk1.Close()
	bk2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bk2Hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	defer bk2.Close()

	// MaxRetries=2; budget high; force order by giving bk1 a worse EWMA after first attempt.
	p := newProxy(t, []string{bk1.URL, bk2.URL}, Config{UpstreamTimeout: time.Second, MaxRetries: 2, RetryBase: time.Microsecond, RetryCap: time.Millisecond})

	// Loop a few times to ensure retry happens regardless of which backend is
	// chosen first. The test asserts: every client-visible response is 200
	// (because ≥1 of the 2 backends works).
	srv := httptest.NewServer(p)
	defer srv.Close()
	for i := 0; i < 5; i++ {
		resp, err := http.Get(srv.URL + "/anything")
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(body) != "ok" {
			t.Errorf("iter %d: status=%d body=%q (bk1 hits=%d bk2 hits=%d)",
				i, resp.StatusCode, body, bk1Hits.Load(), bk2Hits.Load())
		}
	}
	if bk2Hits.Load() == 0 {
		t.Errorf("bk2 never reached; retry did not cross backends")
	}
}

// LB-41: POST never retries even on 5xx.
func TestProxy_NeverRetriesPOST(t *testing.T) {
	var hits atomic.Int64
	bk1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bk1.Close()
	bk2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	defer bk2.Close()

	p := newProxy(t, []string{bk1.URL, bk2.URL}, Config{UpstreamTimeout: time.Second, MaxRetries: 5, RetryBase: time.Microsecond, RetryCap: time.Millisecond})
	srv := httptest.NewServer(p)
	defer srv.Close()

	resp, _ := http.Post(srv.URL+"/", "text/plain", strings.NewReader("x"))
	resp.Body.Close()

	if hits.Load() != 1 {
		t.Errorf("POST hit %d backends; LB-41 forbids retry on POST, want exactly 1", hits.Load())
	}
}

// LB-22: when all breakers are Open, return 503.
func TestProxy_503WhenAllOpen(t *testing.T) {
	bk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bk.Close()

	p, err := pool.New([]string{bk.URL}, defaultFactory(), 42)
	if err != nil {
		t.Fatal(err)
	}
	// Trip the only backend's breaker.
	for _, b := range p.Backends() {
		for i := 0; i < 10; i++ {
			_ = b.Breaker.Allow()
			b.Breaker.Record(false)
		}
	}
	pr := New(p, Config{UpstreamTimeout: time.Second, MaxRetries: 2}, retrybudget.New(0.5), nil, nil)
	srv := httptest.NewServer(pr)
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d want 503", resp.StatusCode)
	}
	var body errorBody
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "no eligible backends" {
		t.Errorf("error: got %q", body.Error)
	}
}

// LB-44: when the retry budget is zero, no retry happens. Verified by
// observing the retry counter on the metrics observer.
func TestProxy_RespectsZeroBudget(t *testing.T) {
	bk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Single backend that always 500s. With MaxRetries=5 and budget>0
		// we'd see retries; with budget=0 we must see zero retries even
		// though there's no other backend to fail over to either.
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bk.Close()

	p, err := pool.New([]string{bk.URL}, defaultFactory(), 42)
	if err != nil {
		t.Fatal(err)
	}
	obs := &countingObserver{}
	pr := New(p,
		Config{UpstreamTimeout: time.Second, MaxRetries: 5, RetryBase: time.Microsecond, RetryCap: time.Millisecond},
		retrybudget.New(0), // zero budget — no retries permitted
		obs, nil,
	)
	srv := httptest.NewServer(pr)
	defer srv.Close()

	for i := 0; i < 5; i++ {
		resp, _ := http.Get(srv.URL + "/")
		resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Errorf("iter %d: status=%d, want 500 (no retry, no fallback)", i, resp.StatusCode)
		}
	}
	if got := obs.retries.Load(); got != 0 {
		t.Errorf("retries=%d, want 0 with budget=0", got)
	}
	if got := obs.exhaustions.Load(); got == 0 {
		t.Errorf("exhaustions=0, want >0 with budget=0")
	}
}

// countingObserver counts retry and exhaustion events for tests.
type countingObserver struct {
	requests    atomic.Int64
	retries     atomic.Int64
	transitions atomic.Int64
	exhaustions atomic.Int64
}

func (o *countingObserver) Observe(string, int, time.Duration) { o.requests.Add(1) }
func (o *countingObserver) RecordRetry(string, string, metrics.RetryReason) {
	o.retries.Add(1)
}
func (o *countingObserver) RecordBreakerTransition(string, breaker.State, breaker.State) {
	o.transitions.Add(1)
}
func (o *countingObserver) RecordBudgetExhaustion() { o.exhaustions.Add(1) }

// LB-06 (re-asserted): a connection failure does NOT cause the failed backend
// to re-enter the rotation immediately on the same request — but the next
// retry MUST go to a different backend (LB-43).
func TestProxy_RetryHitsDifferentBackend(t *testing.T) {
	var bk1Hits, bk2Hits atomic.Int64
	bk1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bk1Hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bk1.Close()
	bk2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bk2Hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	defer bk2.Close()

	p := newProxy(t, []string{bk1.URL, bk2.URL}, Config{UpstreamTimeout: time.Second, MaxRetries: 1, RetryBase: time.Microsecond, RetryCap: time.Millisecond})
	srv := httptest.NewServer(p)
	defer srv.Close()

	for i := 0; i < 4; i++ {
		resp, _ := http.Get(srv.URL + "/")
		resp.Body.Close()
	}
	// Each request: at most 2 attempts. Both backends should have seen traffic.
	if bk1Hits.Load() == 0 || bk2Hits.Load() == 0 {
		t.Errorf("expected both backends hit; bk1=%d bk2=%d", bk1Hits.Load(), bk2Hits.Load())
	}
}

// LB-32..36: many failures in a row open a breaker; subsequent requests skip it.
func TestProxy_OpenBreakerExcludesBackend(t *testing.T) {
	var bk1Hits, bk2Hits atomic.Int64
	bk1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bk1Hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bk1.Close()
	bk2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bk2Hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	defer bk2.Close()

	// Tight breaker: window=4, threshold=0.5 → trips after 3 errors observed.
	tight := pool.BackendFactory(func(u *url.URL) *backend.Backend {
		return backend.New(u, ewma.New(0.1, time.Second), breaker.New(breaker.Config{
			Window:         4,
			ErrorThreshold: 0.5,
			ResetTimeout:   10 * time.Second, // long: don't reopen during test
			ResetCap:       30 * time.Second,
		}))
	})
	p, _ := pool.New([]string{bk1.URL, bk2.URL}, tight, 42)
	pr := New(p, Config{UpstreamTimeout: time.Second, MaxRetries: 2, RetryBase: time.Microsecond, RetryCap: time.Millisecond}, retrybudget.New(0.99), nil, nil)
	srv := httptest.NewServer(pr)
	defer srv.Close()

	// Drive enough traffic to trip bk1's breaker (it always 500s).
	for i := 0; i < 25; i++ {
		resp, _ := http.Get(srv.URL + "/")
		resp.Body.Close()
	}
	bk1AtTrip := bk1Hits.Load()

	// Now hit it 10 more times; bk1 hits should not increase materially
	// because its breaker is Open.
	for i := 0; i < 10; i++ {
		resp, _ := http.Get(srv.URL + "/")
		resp.Body.Close()
	}
	bk1AfterTrip := bk1Hits.Load()
	delta := bk1AfterTrip - bk1AtTrip
	if delta > 2 {
		t.Errorf("bk1 still receiving traffic after breaker trip: delta=%d (before=%d after=%d). Breaker not excluding Open.",
			delta, bk1AtTrip, bk1AfterTrip)
	}
	t.Logf("bk1 hits before/after trip: %d → %d (delta %d); bk2 total %d", bk1AtTrip, bk1AfterTrip, delta, bk2Hits.Load())
}
