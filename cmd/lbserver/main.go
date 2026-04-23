// Command lbserver is the V1 blind round-robin HTTP load balancer.
//
//	lbserver --backends=http://b1:9001,http://b2:9001 --listen=:7080 --admin-listen=:7090
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/steve-weiland/loadbalancer/internal/admin"
	"github.com/steve-weiland/loadbalancer/internal/metrics"
	"github.com/steve-weiland/loadbalancer/internal/pool"
	"github.com/steve-weiland/loadbalancer/internal/proxy"
)

func main() {
	var (
		listenAddr      = flag.String("listen", ":7080", "HTTP listen address for the proxy")
		adminListenAddr = flag.String("admin-listen", ":7090", "HTTP listen address for /healthz and /metrics")
		backendsFlag    = flag.String("backends", "", "comma-separated backend URLs (required), e.g. http://b1:9001,http://b2:9001")
		upstreamTimeout = flag.Duration("upstream-timeout", 5*time.Second, "time-to-first-byte timeout per upstream request")
		logFormat       = flag.String("log-format", "json", `"json" or "text"`)
	)
	flag.Parse()

	log := newLogger(*logFormat)
	slog.SetDefault(log)

	urls := splitCSV(*backendsFlag)
	p, err := pool.New(urls)
	if err != nil {
		log.Error("invalid backend pool", slog.Any("err", err))
		os.Exit(2) // LB-07
	}

	reg := prometheus.NewRegistry()
	backendStrs := make([]string, 0, len(p.Backends()))
	for _, b := range p.Backends() {
		backendStrs = append(backendStrs, b.String())
	}
	obs := metrics.New(reg, backendStrs)

	pr := proxy.New(p, *upstreamTimeout, obs, log)

	dataSrv := &http.Server{Addr: *listenAddr, Handler: pr}
	adminSrv := &http.Server{Addr: *adminListenAddr, Handler: admin.NewMux(reg)}

	errCh := make(chan error, 2)
	go func() {
		log.Info("admin listening",
			slog.String("addr", *adminListenAddr),
			slog.String("endpoints", "/healthz, /metrics"),
		)
		if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("admin server: %w", err)
		}
	}()
	go func() {
		log.Info("proxy listening",
			slog.String("addr", *listenAddr),
			slog.Int("backends", len(backendStrs)),
			slog.Duration("upstream_timeout", *upstreamTimeout),
		)
		if err := dataSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("proxy server: %w", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Info("shutting down", slog.String("signal", sig.String()))
	case err := <-errCh:
		log.Error("server failure", slog.Any("err", err))
	}

	// Spec out-of-scope: no graceful drain. Close immediately.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_ = dataSrv.Shutdown(ctx)
	_ = adminSrv.Shutdown(ctx)
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
