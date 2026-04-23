// Command lbserver is the V2 health-aware, retry-capable HTTP load balancer.
//
//	lbserver --backends=http://b1:9001,http://b2:9001 \
//	         --listen=:7080 --admin-listen=:7090 \
//	         --max-retries=2 --retry-budget=0.10
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/steve-weiland/loadbalancer/internal/admin"
	"github.com/steve-weiland/loadbalancer/internal/backend"
	"github.com/steve-weiland/loadbalancer/internal/breaker"
	"github.com/steve-weiland/loadbalancer/internal/ewma"
	"github.com/steve-weiland/loadbalancer/internal/metrics"
	"github.com/steve-weiland/loadbalancer/internal/pool"
	"github.com/steve-weiland/loadbalancer/internal/proxy"
	"github.com/steve-weiland/loadbalancer/internal/retrybudget"
)

func main() {
	cfg := parseFlags()

	log := newLogger(cfg.logFormat)
	slog.SetDefault(log)

	if err := cfg.validate(); err != nil {
		log.Error("invalid configuration", slog.Any("err", err))
		os.Exit(2)
	}

	urls := splitCSV(cfg.backendsFlag)
	reg := prometheus.NewRegistry()

	// Chicken-and-egg: the breaker needs a transition callback that names the
	// backend, but the *Backend struct is what owns the breaker. We resolve it
	// by capturing a pointer-to-pointer (`&b`) inside the closure — populated
	// after backend.New returns. Same trick for the Observer (defined below
	// the pool, since metrics.New takes the live []*Backend).
	var obsHolder metrics.Observer = metrics.Noop{}
	factory := func(u *url.URL) *backend.Backend {
		var b *backend.Backend
		br := breaker.New(
			breaker.Config{
				Window:         cfg.breakerWindow,
				ErrorThreshold: cfg.breakerErrorThresh,
				ResetTimeout:   cfg.breakerResetTimeout,
				ResetCap:       cfg.breakerResetCap,
			},
			breaker.WithOnTransition(func(from, to breaker.State) {
				if b != nil {
					obsHolder.RecordBreakerTransition(b.String(), from, to)
				}
			}),
		)
		b = backend.New(u, ewma.New(cfg.ewmaAlpha, cfg.upstreamTimeout), br)
		return b
	}

	p, err := pool.New(urls, factory, 0)
	if err != nil {
		log.Error("invalid backend pool", slog.Any("err", err))
		os.Exit(2) // LB-07
	}

	promObs := metrics.New(reg, p.Backends())
	obsHolder = promObs // resolve the forward declaration

	budget := retrybudget.New(cfg.retryBudget)

	pr := proxy.New(p, proxy.Config{
		UpstreamTimeout: cfg.upstreamTimeout,
		MaxRetries:      cfg.maxRetries,
		RetryBase:       cfg.retryBase,
		RetryCap:        cfg.retryCap,
	}, budget, promObs, log)

	// Drain state — flipped true on SIGTERM. Read by the admin /healthz handler
	// (LB-27) and used to trigger the dataSrv.Shutdown wait window (LB-25).
	var draining atomic.Bool

	// Track active data-plane connections so we can report how many got
	// forcibly closed if the drain timeout expires (LB-26). The ConnState
	// callback fires for every connection state transition.
	var activeConns atomic.Int64
	connState := func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			activeConns.Add(1)
		case http.StateClosed, http.StateHijacked:
			activeConns.Add(-1)
		}
	}

	dataSrv := &http.Server{Addr: cfg.listenAddr, Handler: pr, ConnState: connState}
	adminSrv := &http.Server{Addr: cfg.adminListenAddr, Handler: admin.NewMux(reg, draining.Load)}

	errCh := make(chan error, 2)
	go func() {
		log.Info("admin listening",
			slog.String("addr", cfg.adminListenAddr),
			slog.String("endpoints", "/healthz, /metrics"),
		)
		if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("admin server: %w", err)
		}
	}()
	go func() {
		log.Info("proxy listening",
			slog.String("addr", cfg.listenAddr),
			slog.Int("backends", len(p.Backends())),
			slog.Duration("upstream_timeout", cfg.upstreamTimeout),
			slog.Int("max_retries", cfg.maxRetries),
			slog.Float64("retry_budget", cfg.retryBudget),
			slog.Duration("drain_timeout", cfg.drainTimeout),
		)
		if err := dataSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("proxy server: %w", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	exitCode := 0
	select {
	case sig := <-sigCh:
		log.Info("draining",
			slog.String("signal", sig.String()),
			slog.Duration("drain_timeout", cfg.drainTimeout),
			slog.Int64("active_connections", activeConns.Load()),
		)
		// LB-25: stop accepting new data-plane connections; wait for in-flight.
		// LB-27: flip the drain flag so /healthz starts returning 503 *before*
		// we begin draining, so external LBs notice and stop sending traffic.
		draining.Store(true)
		// Tiny grace period so an in-flight scrape sees the new health status
		// before we close the listener. Optional but kind to operators.
		time.Sleep(100 * time.Millisecond)

		drainCtx, cancel := context.WithTimeout(context.Background(), cfg.drainTimeout)
		defer cancel()
		drainErr := dataSrv.Shutdown(drainCtx)
		remaining := activeConns.Load()
		if errors.Is(drainErr, context.DeadlineExceeded) {
			// LB-26: timeout elapsed with requests still in flight. Force-close
			// (Shutdown already returned; the underlying connections will be
			// reaped on Close). Log the count and exit non-zero.
			log.Error("drain timeout — forcing close",
				slog.Int64("forcibly_closed", remaining),
				slog.Duration("elapsed", cfg.drainTimeout),
			)
			_ = dataSrv.Close()
			exitCode = 1
		} else if drainErr != nil {
			log.Error("drain error", slog.Any("err", drainErr))
		} else {
			log.Info("drain complete", slog.Int64("forcibly_closed", 0))
		}
	case err := <-errCh:
		log.Error("server failure", slog.Any("err", err))
		exitCode = 1
	}

	// Admin server stays up through the drain (LB-25). Close it now with a
	// short deadline; any in-flight /metrics scrape is fast, /healthz never
	// blocks.
	adminCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = adminSrv.Shutdown(adminCtx)
	os.Exit(exitCode)
}

type config struct {
	listenAddr      string
	adminListenAddr string
	backendsFlag    string
	upstreamTimeout time.Duration
	logFormat       string

	ewmaAlpha            float64
	breakerWindow        int
	breakerErrorThresh   float64
	breakerResetTimeout  time.Duration
	breakerResetCap      time.Duration
	maxRetries           int
	retryBase            time.Duration
	retryCap             time.Duration
	retryBudget          float64

	drainTimeout         time.Duration
}

func parseFlags() config {
	var c config
	// V1 flags (preserved)
	flag.StringVar(&c.listenAddr, "listen", ":7080", "HTTP listen address for the proxy")
	flag.StringVar(&c.adminListenAddr, "admin-listen", ":7090", "HTTP listen address for /healthz and /metrics")
	flag.StringVar(&c.backendsFlag, "backends", "", "comma-separated backend URLs (required)")
	flag.DurationVar(&c.upstreamTimeout, "upstream-timeout", 5*time.Second, "TTFB timeout per upstream attempt")
	flag.StringVar(&c.logFormat, "log-format", "json", `"json" or "text"`)

	// V2 health & breaker flags
	flag.Float64Var(&c.ewmaAlpha, "ewma-alpha", 0.1, "EWMA smoothing factor (0 < α ≤ 1)")
	flag.IntVar(&c.breakerWindow, "breaker-window", 10, "sliding-window size for breaker error rate")
	flag.Float64Var(&c.breakerErrorThresh, "breaker-error-threshold", 0.5, "error ratio that trips Closed → Open")
	flag.DurationVar(&c.breakerResetTimeout, "breaker-reset-timeout", 10*time.Second, "initial Open → Half-open delay")
	flag.DurationVar(&c.breakerResetCap, "breaker-reset-cap", 60*time.Second, "maximum Open → Half-open delay")

	// V2 retry flags
	flag.IntVar(&c.maxRetries, "max-retries", 2, "additional attempts after the first failure")
	flag.DurationVar(&c.retryBase, "retry-base", 10*time.Millisecond, "base backoff before retry attempt 1")
	flag.DurationVar(&c.retryCap, "retry-cap", 200*time.Millisecond, "cap on per-attempt backoff")
	flag.Float64Var(&c.retryBudget, "retry-budget", 0.10, "fraction of total requests that may retry")

	// v2.1 graceful drain
	flag.DurationVar(&c.drainTimeout, "drain-timeout", 30*time.Second, "max wait for in-flight requests on SIGTERM (LB-25)")
	flag.Parse()
	return c
}

func (c config) validate() error {
	if c.ewmaAlpha <= 0 || c.ewmaAlpha > 1 {
		return fmt.Errorf("--ewma-alpha must be in (0, 1], got %v", c.ewmaAlpha)
	}
	if c.breakerWindow < 1 {
		return fmt.Errorf("--breaker-window must be >= 1, got %d", c.breakerWindow)
	}
	if c.breakerErrorThresh < 0 || c.breakerErrorThresh > 1 {
		return fmt.Errorf("--breaker-error-threshold must be in [0, 1], got %v", c.breakerErrorThresh)
	}
	if c.breakerResetTimeout <= 0 {
		return fmt.Errorf("--breaker-reset-timeout must be > 0, got %v", c.breakerResetTimeout)
	}
	if c.breakerResetCap < c.breakerResetTimeout {
		return fmt.Errorf("--breaker-reset-cap (%v) must be >= --breaker-reset-timeout (%v)", c.breakerResetCap, c.breakerResetTimeout)
	}
	if c.maxRetries < 0 {
		return fmt.Errorf("--max-retries must be >= 0, got %d", c.maxRetries)
	}
	if c.retryBase < 0 {
		return fmt.Errorf("--retry-base must be >= 0, got %v", c.retryBase)
	}
	if c.retryCap < c.retryBase {
		return fmt.Errorf("--retry-cap (%v) must be >= --retry-base (%v)", c.retryCap, c.retryBase)
	}
	if c.retryBudget < 0 || c.retryBudget > 1 {
		return fmt.Errorf("--retry-budget must be in [0, 1], got %v", c.retryBudget)
	}
	if c.upstreamTimeout <= 0 {
		return fmt.Errorf("--upstream-timeout must be > 0, got %v", c.upstreamTimeout)
	}
	if c.drainTimeout < 0 {
		return fmt.Errorf("--drain-timeout must be >= 0, got %v", c.drainTimeout)
	}
	return nil
}

func newLogger(format string) *slog.Logger {
	var h slog.Handler
	switch strings.ToLower(format) {
	case "text":
		h = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	default:
		h = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	}
	return slog.New(h)
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
