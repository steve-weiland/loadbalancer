// Package proxy is the data-plane HTTP handler. It wraps httputil.ReverseProxy
// with a Director that selects a backend from the pool, scrubs hop-by-hop
// headers, sets the X-Forwarded-* headers, and translates upstream transport
// errors into 502 or 504 responses.
//
// V1 deliberately does not retry on any status or transport error (LB-14).
package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/steve-weiland/loadbalancer/internal/metrics"
	"github.com/steve-weiland/loadbalancer/internal/pool"
)

type Proxy struct {
	pool    *pool.Pool
	rp      *httputil.ReverseProxy
	timeout time.Duration
	obs     metrics.Observer
	log     *slog.Logger
}

func New(p *pool.Pool, timeout time.Duration, obs metrics.Observer, log *slog.Logger) *Proxy {
	if obs == nil {
		obs = metrics.Noop{}
	}
	if log == nil {
		log = slog.Default()
	}

	pr := &Proxy{pool: p, timeout: timeout, obs: obs, log: log}

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
		// LB-13: time-to-first-byte timeout. After this elapses with no
		// response headers, RoundTrip returns a net.Error with Timeout()=true
		// which the ErrorHandler maps to 504.
		ResponseHeaderTimeout: timeout,
	}

	pr.rp = &httputil.ReverseProxy{
		Rewrite:      pr.rewrite,
		Transport:    transport,
		ErrorHandler: pr.errorHandler,
	}
	return pr
}

// rewrite picks the next backend, points the outbound request at it, and
// records the choice on the inbound recorder so the access log, metrics, and
// ErrorHandler can all label observations consistently.
//
// httputil.ReverseProxy strips hop-by-hop headers from pr.Out before calling
// Rewrite (RFC 7230 §6.1, including those named in Connection), so we don't
// repeat that work here.
func (p *Proxy) rewrite(pr *httputil.ProxyRequest) {
	target := p.pool.Next()
	pr.SetURL(target) // sets Out.URL.Scheme/Host and Out.Host

	// LB-10: append client IP to X-Forwarded-For. ReverseProxy strips
	// client-supplied X-Forwarded-* from Out before Rewrite runs (so a hostile
	// client cannot impersonate a downstream proxy). Copy the original XFF
	// back across so SetXForwarded appends to it instead of replacing it.
	if prior := pr.In.Header.Get("X-Forwarded-For"); prior != "" {
		pr.Out.Header.Set("X-Forwarded-For", prior)
	}
	pr.SetXForwarded() // sets X-Forwarded-{For,Host,Proto} using In.RemoteAddr / In.Host

	// Surface the chosen backend back to ServeHTTP / ErrorHandler via the
	// recorder. ProxyRequest.In must not be mutated, so we don't touch it.
	if rec, ok := pr.Out.Context().Value(recorderCtxKey{}).(*statusRecorder); ok {
		rec.backend = target.String()
	}
}

// errorHandler maps transport errors to 502 or 504 with the JSON bodies
// described in spec §4. Critically it does NOT retry (LB-14).
//
// w here is the same ResponseWriter we handed to ReverseProxy.ServeHTTP — our
// statusRecorder — so we read the chosen backend back from it.
func (p *Proxy) errorHandler(w http.ResponseWriter, _ *http.Request, err error) {
	backend := ""
	if rec, ok := w.(*statusRecorder); ok {
		backend = rec.backend
	}

	status := http.StatusBadGateway
	msg := "upstream connection failed"

	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		status = http.StatusGatewayTimeout
		msg = "upstream timeout"
	} else if errors.Is(err, context.DeadlineExceeded) {
		status = http.StatusGatewayTimeout
		msg = "upstream timeout"
	}

	writeError(w, status, msg, backend)
}

// ServeHTTP wraps ReverseProxy with a recorder that captures both the chosen
// backend (set by rewrite) and the upstream status code (set by WriteHeader).
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

	// Stash the recorder on the context so the Rewrite hook can find it via
	// pr.Out.Context() (Out is cloned from In, inheriting the context).
	r = r.WithContext(context.WithValue(r.Context(), recorderCtxKey{}, rec))

	start := time.Now()
	p.rp.ServeHTTP(rec, r)
	latency := time.Since(start)

	p.obs.Observe(rec.backend, rec.status, latency)
	p.log.Info("proxied",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("backend", rec.backend),
		slog.Int("status", rec.status),
		slog.Float64("latency_ms", float64(latency.Microseconds())/1000.0),
	)
}

// recorderCtxKey carries the per-request statusRecorder from ServeHTTP into
// the Rewrite hook. The recorder is the only durable per-request side channel
// shared between ServeHTTP, Rewrite, and ErrorHandler — context values flow
// through the cloned outbound request, while the recorder pointer is reachable
// from ErrorHandler via the ResponseWriter argument.
type recorderCtxKey struct{}

// statusRecorder captures the upstream status code (set on WriteHeader) and
// the chosen backend URL (set in Rewrite). One instance per request.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
	backend     string
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if !s.wroteHeader {
		s.wroteHeader = true
	}
	return s.ResponseWriter.Write(b)
}
