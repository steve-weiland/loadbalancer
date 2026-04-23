package proxy

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steve-weiland/loadbalancer/internal/pool"
)

func newProxy(t *testing.T, timeout time.Duration, urls ...string) *Proxy {
	t.Helper()
	p, err := pool.New(urls)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	return New(p, timeout, nil, nil)
}

func TestProxy_ForwardsMethodPathBodyHeaders(t *testing.T) {
	type seen struct {
		Method string
		Path   string
		Query  string
		Body   string
		Custom string
	}
	var got seen
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = seen{
			Method: r.Method,
			Path:   r.URL.Path,
			Query:  r.URL.RawQuery,
			Body:   string(body),
			Custom: r.Header.Get("X-Custom"),
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer backend.Close()

	p := newProxy(t, 5*time.Second, backend.URL)
	srv := httptest.NewServer(p)
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/foo/bar?x=1", strings.NewReader("hello"))
	req.Header.Set("X-Custom", "yes")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	want := seen{Method: "POST", Path: "/foo/bar", Query: "x=1", Body: "hello", Custom: "yes"}
	if got != want {
		t.Errorf("forwarded: got %+v, want %+v", got, want)
	}
}

func TestProxy_StripsHopByHopHeaders(t *testing.T) {
	var saw http.Header
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		saw = r.Header.Clone()
		w.WriteHeader(200)
	}))
	defer backend.Close()

	p := newProxy(t, 5*time.Second, backend.URL)
	srv := httptest.NewServer(p)
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/", nil)
	// Standard hop-by-hop set, plus a custom header named in Connection.
	req.Header.Set("Connection", "X-Per-Hop, close")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("Proxy-Authorization", "Basic xyz")
	req.Header.Set("X-Per-Hop", "should-be-stripped")
	req.Header.Set("X-Keep-Me", "yes")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	stripped := []string{"Connection", "Keep-Alive", "Proxy-Authorization", "X-Per-Hop"}
	for _, h := range stripped {
		if v := saw.Get(h); v != "" {
			t.Errorf("expected %q stripped, got %q", h, v)
		}
	}
	if v := saw.Get("X-Keep-Me"); v != "yes" {
		t.Errorf("end-to-end header not forwarded: got %q", v)
	}
}

func TestProxy_AppendsXForwardedFor(t *testing.T) {
	var seen http.Header
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
	}))
	defer backend.Close()

	p := newProxy(t, 5*time.Second, backend.URL)
	srv := httptest.NewServer(p)
	defer srv.Close()

	// Case 1: no prior X-Forwarded-For — proxy creates it.
	req, _ := http.NewRequest("GET", srv.URL+"/", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	if xff := seen.Get("X-Forwarded-For"); xff == "" {
		t.Error("X-Forwarded-For not set")
	}
	if proto := seen.Get("X-Forwarded-Proto"); proto != "http" {
		t.Errorf("X-Forwarded-Proto: got %q, want http", proto)
	}

	// Case 2: prior X-Forwarded-For — proxy appends.
	req2, _ := http.NewRequest("GET", srv.URL+"/", nil)
	req2.Header.Set("X-Forwarded-For", "10.0.0.1")
	resp2, _ := http.DefaultClient.Do(req2)
	resp2.Body.Close()

	xff := seen.Get("X-Forwarded-For")
	if !strings.HasPrefix(xff, "10.0.0.1, ") {
		t.Errorf("X-Forwarded-For not appended: got %q", xff)
	}
}

func TestProxy_ForwardsResponseUnchanged(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Backend-Header", "v1")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("body-bytes"))
	}))
	defer backend.Close()

	p := newProxy(t, 5*time.Second, backend.URL)
	srv := httptest.NewServer(p)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("status: got %d, want 418", resp.StatusCode)
	}
	if resp.Header.Get("X-Backend-Header") != "v1" {
		t.Errorf("response header lost: %q", resp.Header.Get("X-Backend-Header"))
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "body-bytes" {
		t.Errorf("body: got %q, want %q", body, "body-bytes")
	}
}

// LB-12: bad backend → 502 with JSON body naming the backend.
func TestProxy_ConnRefusedReturns502(t *testing.T) {
	// Bind to a port, then close it, so the URL points to nothing listening.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadURL := "http://" + l.Addr().String()
	l.Close()

	p := newProxy(t, 1*time.Second, deadURL)
	srv := httptest.NewServer(p)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status: got %d, want 502", resp.StatusCode)
	}
	var body errorBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Error != "upstream connection failed" {
		t.Errorf("error: got %q", body.Error)
	}
	if body.Backend != deadURL {
		t.Errorf("backend label: got %q, want %q", body.Backend, deadURL)
	}
}

// LB-13: slow backend → 504 once the response-header timeout elapses.
func TestProxy_SlowBackendReturns504(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer backend.Close()

	p := newProxy(t, 50*time.Millisecond, backend.URL)
	srv := httptest.NewServer(p)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("status: got %d, want 504", resp.StatusCode)
	}
	var body errorBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Error != "upstream timeout" {
		t.Errorf("error: got %q", body.Error)
	}
}

// LB-14: a 5xx upstream response is forwarded as-is, not retried.
// Each request must hit exactly one backend.
func TestProxy_DoesNotRetryOn5xx(t *testing.T) {
	var hits atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer backend.Close()

	p := newProxy(t, 5*time.Second, backend.URL)
	srv := httptest.NewServer(p)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status: got %d, want 500 (forwarded as-is)", resp.StatusCode)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("backend hit %d times; LB-14 forbids retry, expected 1", got)
	}
}

// LB-06 + LB-14: a connection failure must NOT cause the next request to skip
// the dead backend. The counter advances normally and the next slot is hit.
func TestProxy_DeadBackendDoesNotCauseSkip(t *testing.T) {
	var hits atomic.Int64
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	defer live.Close()

	// dead URL: bind+close to grab an unused port.
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	deadURL := "http://" + l.Addr().String()
	l.Close()

	// Pool: [live, dead]. Round-robin should alternate; dead must not be skipped.
	p := newProxy(t, 1*time.Second, live.URL, deadURL)
	srv := httptest.NewServer(p)
	defer srv.Close()

	// 4 requests → 2 to live (200), 2 to dead (502).
	statuses := []int{}
	for i := 0; i < 4; i++ {
		resp, err := http.Get(srv.URL + "/")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		statuses = append(statuses, resp.StatusCode)
	}

	want := []int{200, 502, 200, 502}
	for i := range want {
		if statuses[i] != want[i] {
			t.Errorf("request %d: got %d, want %d (full sequence: %v)", i, statuses[i], want[i], statuses)
		}
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("live backend hit %d times; want 2 (no skip implies dead got equal share)", got)
	}
}
