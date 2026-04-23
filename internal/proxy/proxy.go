// Package proxy is the V2 data-plane HTTP handler. It wraps a hand-rolled
// upstream call (no httputil.ReverseProxy in V2) inside a retry loop that
// honours the per-backend circuit breaker, the per-request idempotency rules,
// and a global retry budget.
//
// V1 used httputil.ReverseProxy for fire-and-forget. V2 needs to inspect the
// upstream status before deciding whether to forward the response or retry,
// which fights ReverseProxy's design — so we issue the upstream call ourselves.
//
// CALLOUT — response buffering trade-off (spec §5):
//
//	The V2 spec lists "request/response body buffering beyond defaults" as
//	out of scope, but cross-backend retry (LB-40) intrinsically requires the
//	proxy to *capture* the upstream response so it can decide to discard a
//	5xx and try elsewhere. We resolve the tension by capping captured response
//	bodies at maxRetryBodyBytes (1 MiB). Above that, we cannot retry safely
//	(the body has already streamed to the client), so the response goes
//	through verbatim. Revisit if a chaos run shows large-response retry need
//	or memory pressure.
//
// CALLOUT — request body re-readability:
//
//	A retried request must send the same body to a different backend. We
//	snapshot req.Body into memory once at the top of ServeHTTP, capped at
//	maxRetryBodyBytes. Bodies above the cap are streamed directly on the
//	first attempt with no retry. Idempotent methods almost never carry large
//	bodies (GET/HEAD/DELETE/OPTIONS/TRACE → no body; PUT can but rarely does).
package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/steve-weiland/loadbalancer/internal/backend"
	"github.com/steve-weiland/loadbalancer/internal/metrics"
	"github.com/steve-weiland/loadbalancer/internal/pool"
	"github.com/steve-weiland/loadbalancer/internal/retrybudget"
)

// maxRetryBodyBytes caps both the request and response bytes we'll buffer
// for retry purposes. Larger payloads pass through but are not retry-eligible.
// See package doc CALLOUT for the trade-off.
const maxRetryBodyBytes = 1 << 20 // 1 MiB

// Config holds the V2 retry knobs. Pulled from CLI flags at the cmd layer.
type Config struct {
	UpstreamTimeout time.Duration // TTFB timeout per attempt
	MaxRetries      int           // additional attempts after the first failure
	RetryBase       time.Duration // base backoff before retry attempt 1
	RetryCap        time.Duration // cap on per-attempt backoff
}

type Proxy struct {
	pool      *pool.Pool
	transport *http.Transport
	cfg       Config
	budget    *retrybudget.Budget
	obs       metrics.Observer
	log       *slog.Logger

	rngMu sync.Mutex
	rng   *rand.Rand
}

func New(p *pool.Pool, cfg Config, budget *retrybudget.Budget, obs metrics.Observer, log *slog.Logger) *Proxy {
	if obs == nil {
		obs = metrics.Noop{}
	}
	if log == nil {
		log = slog.Default()
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: cfg.UpstreamTimeout, // LB-13 TTFB
	}
	return &Proxy{
		pool:      p,
		transport: transport,
		cfg:       cfg,
		budget:    budget,
		obs:       obs,
		log:       log,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// idempotent reports whether a method is safe to retry per RFC 7231 §4.2.2
// (LB-41). POST and PATCH are deliberately excluded.
func idempotent(method string) bool {
	switch strings.ToUpper(method) {
	case "GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE":
		return true
	}
	return false
}

// attemptOutcome classifies the result of a single upstream attempt for
// retry / breaker / metrics decisions.
type attemptOutcome struct {
	backend  *backend.Backend
	resp     *http.Response // non-nil on success or 5xx; body already drained into bodyBuf
	bodyBuf  []byte         // captured body, up to maxRetryBodyBytes
	err      error          // non-nil on transport / timeout
	latency  time.Duration
	reason   metrics.RetryReason // populated when retryable
	retryable bool
}

// ServeHTTP is the request entry point. It snapshots the request body, runs
// the retry loop, observes the terminal outcome, and copies it to the client.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.budget.Observe()

	// Snapshot body up to maxRetryBodyBytes. If the body is larger we bail to
	// streaming-no-retry mode (canRetry=false).
	bodyBytes, bodyTooLarge, err := snapshotBody(r.Body)
	_ = r.Body.Close()
	if err != nil {
		http.Error(w, "read request body", http.StatusBadRequest)
		return
	}
	canRetry := idempotent(r.Method) && !bodyTooLarge && p.cfg.MaxRetries > 0

	chain := []*backend.Backend{}
	var lastFailed *backend.Backend
	var terminal *attemptOutcome // most recent attempt — promoted to terminal if loop ends here

	totalStart := time.Now()
	maxAttempts := 1 + p.cfg.MaxRetries
	if !canRetry {
		maxAttempts = 1
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Budget gate (LB-44) — only on retry attempts; the first attempt is
		// not a retry.
		if attempt > 1 {
			if !p.budget.TryConsume() {
				p.obs.RecordBudgetExhaustion()
				// Out of budget: the last completed attempt becomes the
				// terminal response (terminal already points at it).
				break
			}
			// Backoff with full jitter (LB-42).
			p.sleepBackoff(attempt - 1)
		}

		// Pick a backend, avoiding the last failed.
		b, perr := p.pool.PickAvoiding(lastFailed)
		if perr != nil {
			// LB-22: nothing eligible (or only the avoided one was).
			// First attempt: 503. Later attempts: keep the previous failure
			// as the terminal response (terminal already set).
			if attempt == 1 {
				p.write503(w, len(chain))
				p.logProxied(r, "", http.StatusServiceUnavailable, time.Since(totalStart), 0, chain)
				p.obs.Observe("", http.StatusServiceUnavailable, time.Since(totalStart))
				return
			}
			break
		}

		chain = append(chain, b)
		// Reserve the breaker slot for this backend before issuing. The pool
		// already filtered out Open backends, so Allow returns true for
		// Closed; for Half-open it claims the single in-flight probe slot
		// (LB-36). If two requests race for the same Half-open backend, only
		// the first gets the slot — the loser falls back to the next pool
		// pick on its own retry path.
		_ = b.Breaker.Allow()

		out := p.attempt(r, b, bodyBytes)
		// LB-37: error iff transport, timeout, or 5xx. 4xx counts as non-error.
		b.Breaker.Record(!out.retryable)
		// LB-30: EWMA updates on every completed attempt, pass or fail.
		b.EWMA.Update(out.latency)
		// Always promote to terminal so a subsequent budget-exhaustion or
		// pick-failure leaves us with *something* to copy back.
		terminal = &out

		if !out.retryable || attempt == maxAttempts {
			break
		}

		// Retryable failure — record retry intent. The destination is whoever
		// PickAvoiding will return next; record now since we may not actually
		// reach the next iteration if budget is exhausted.
		lastFailed = b
		if next, perr := p.pool.PickAvoiding(b); perr == nil {
			p.obs.RecordRetry(b.String(), next.String(), out.reason)
		}
	}

	if terminal == nil {
		// Defensive: should never happen — the only way to skip the assignment
		// is to break before any attempt() call, which only the first-iteration
		// no-eligible path does, and that returns above. If it ever fires,
		// emit a 503.
		p.write503(w, 0)
		p.logProxied(r, "", http.StatusServiceUnavailable, time.Since(totalStart), 0, chain)
		p.obs.Observe("", http.StatusServiceUnavailable, time.Since(totalStart))
		return
	}

	latency := time.Since(totalStart)
	if terminal.err != nil {
		p.writeUpstreamError(w, terminal, len(chain))
		status := mapErrToStatus(terminal.err)
		p.obs.Observe(terminal.backend.String(), status, latency)
		p.logProxied(r, terminal.backend.String(), status, latency, len(chain), chain)
		return
	}
	p.copyResponse(w, terminal)
	p.obs.Observe(terminal.backend.String(), terminal.resp.StatusCode, latency)
	p.logProxied(r, terminal.backend.String(), terminal.resp.StatusCode, latency, len(chain), chain)
}

// attempt issues one upstream request to b, captures status+headers+body up
// to maxRetryBodyBytes, classifies the result for retry / breaker.
func (p *Proxy) attempt(r *http.Request, b *backend.Backend, bodyBytes []byte) attemptOutcome {
	out := attemptOutcome{backend: b}
	start := time.Now()

	// Build the outbound request. Clone preserves context (so per-request
	// timeouts/cancellation flow through).
	target := *b.URL
	target.Path = singleJoiningSlash(b.URL.Path, r.URL.Path)
	target.RawQuery = r.URL.RawQuery

	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		out.err = err
		out.latency = time.Since(start)
		out.reason = metrics.RetryReasonTransport
		out.retryable = true
		return out
	}
	copyEndToEndHeaders(outReq.Header, r.Header)
	stripHopByHopFromConnection(r.Header, outReq.Header)
	setForwardedHeaders(outReq, r)
	outReq.Host = b.URL.Host
	outReq.ContentLength = int64(len(bodyBytes))
	if len(bodyBytes) == 0 {
		outReq.Body = http.NoBody
	}

	resp, rterr := p.transport.RoundTrip(outReq)
	out.latency = time.Since(start)
	if rterr != nil {
		out.err = rterr
		out.retryable = true
		var nerr net.Error
		if errors.As(rterr, &nerr) && nerr.Timeout() {
			out.reason = metrics.RetryReasonTimeout
		} else if errors.Is(rterr, context.DeadlineExceeded) {
			out.reason = metrics.RetryReasonTimeout
		} else {
			out.reason = metrics.RetryReasonTransport
		}
		return out
	}
	// Drain body up to cap. If body exceeds cap, mark as not-retryable so we
	// stream it through directly (impossible at this level since we already
	// captured it — see CALLOUT in package doc; revisit if this becomes a
	// memory issue).
	body, _, berr := snapshotBody(resp.Body)
	_ = resp.Body.Close()
	if berr != nil {
		out.err = berr
		out.retryable = true
		out.reason = metrics.RetryReasonTransport
		return out
	}
	out.resp = resp
	out.bodyBuf = body
	out.retryable = resp.StatusCode >= 500
	if out.retryable {
		out.reason = metrics.RetryReason5xx
	}
	return out
}

func (p *Proxy) sleepBackoff(retryAttempt int) {
	// Full jitter: sleep uniformly random in [0, computed_delay].
	base := p.cfg.RetryBase
	computed := base * (1 << (retryAttempt - 1))
	if computed > p.cfg.RetryCap {
		computed = p.cfg.RetryCap
	}
	if computed <= 0 {
		return
	}
	p.rngMu.Lock()
	d := time.Duration(p.rng.Int63n(int64(computed)))
	p.rngMu.Unlock()
	time.Sleep(d)
}

func (p *Proxy) copyResponse(w http.ResponseWriter, out *attemptOutcome) {
	for k, vv := range out.resp.Header {
		if isHopByHop(k) {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(out.resp.StatusCode)
	_, _ = w.Write(out.bodyBuf)
}

func (p *Proxy) writeUpstreamError(w http.ResponseWriter, out *attemptOutcome, attempts int) {
	status := mapErrToStatus(out.err)
	msg := "upstream connection failed"
	if status == http.StatusGatewayTimeout {
		msg = "upstream timeout"
	}
	p.writeError(w, status, msg, out.backend.String(), attempts)
}

func (p *Proxy) write503(w http.ResponseWriter, attempts int) {
	p.writeError(w, http.StatusServiceUnavailable, "no eligible backends", "", attempts)
}

func (p *Proxy) logProxied(r *http.Request, backendStr string, status int, latency time.Duration, attempt int, chain []*backend.Backend) {
	if attempt < 1 {
		attempt = 1
	}
	chainStr := ""
	for i, b := range chain {
		if i > 0 {
			chainStr += "→"
		}
		chainStr += b.String()
	}
	p.log.Info("proxied",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("backend", backendStr),
		slog.Int("status", status),
		slog.Float64("latency_ms", float64(latency.Microseconds())/1000.0),
		slog.Int("attempt", attempt),
		slog.String("backend_chain", chainStr),
	)
}

func mapErrToStatus(err error) int {
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return http.StatusGatewayTimeout
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	return http.StatusBadGateway
}

// snapshotBody reads up to maxRetryBodyBytes from r and reports whether the
// stream exceeded the cap. Returns (data, tooLarge, err).
func snapshotBody(r io.Reader) ([]byte, bool, error) {
	if r == nil {
		return nil, false, nil
	}
	limited := io.LimitReader(r, maxRetryBodyBytes+1)
	buf, err := io.ReadAll(limited)
	if err != nil {
		return nil, false, err
	}
	if len(buf) > maxRetryBodyBytes {
		// Drain the rest so the connection can be reused.
		_, _ = io.Copy(io.Discard, r)
		return buf[:maxRetryBodyBytes], true, nil
	}
	return buf, false, nil
}

// singleJoiningSlash joins two URL paths with exactly one slash. Lifted from
// stdlib httputil so we don't need the import.
func singleJoiningSlash(a, b string) string {
	aSlash := strings.HasSuffix(a, "/")
	bSlash := strings.HasPrefix(b, "/")
	switch {
	case aSlash && bSlash:
		return a + b[1:]
	case !aSlash && !bSlash:
		return a + "/" + b
	}
	return a + b
}

