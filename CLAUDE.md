# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
make build                                          # bin/lbserver, bin/echobackend
make test                                           # go test -race -count=1 ./...
go test -race -count=1 -run TestName ./internal/proxy/...   # single test
make run                                            # foreground LB + one local echo backend
make run-cluster                                    # 3 backends + 1 LB, all backgrounded
make stop-cluster                                   # kills lbserver and echobackend
make run-docker                                     # docker compose up --build -d
make stop-docker                                    # docker compose down
make chaos                                          # 60s vegeta + 10s kill/revive cadence; writes reports/v1-<ts>/
make chaos-report                                   # cat latest summary.txt + chaos.log
```

Go 1.23+ required.

## Architecture

```
cmd/lbserver/main.go         — flag parsing, wiring, signal handling (SIGTERM = immediate close, no drain)
cmd/echobackend/main.go      — toy backend used by run-cluster + docker-compose; not part of V1 surface
cmd/chaos/main.go            — vegeta load + kill/revive runner; spawns its own cluster, writes reports/<tag>-<ts>/{vegeta.bin,timeseries.csv,chaos.log,summary.txt}

internal/pool/
  pool.go                    — Pool: backends []*url.URL, atomic.Uint64 counter; New/Next/Backends

internal/proxy/
  proxy.go                   — Proxy: wraps httputil.ReverseProxy in Rewrite mode; rewrite() picks backend (pool.Next), calls SetURL + SetXForwarded; ErrorHandler maps transport errors to 502/504; statusRecorder carries the chosen backend (set in rewrite, read by ErrorHandler via the ResponseWriter) and the upstream status (WriteHeader)
  errorpage.go               — writeError: JSON body for 502/504

internal/admin/
  admin.go                   — NewMux: /healthz (200 ok) and /metrics (promhttp) on a separate listener

internal/metrics/
  metrics.go                 — Observer interface (Noop, PromObserver); pre-registers backend label sets at startup
```

**Request path:**
```
client → lbserver :7080 → proxy.ServeHTTP → ReverseProxy → director → pool.Next → backend
                                              ↓
                                         (response)
                                              ↓
                                statusRecorder → obs.Observe + slog access log
```

**Admin path** (separate listener `:7090`): `client → admin.NewMux → /healthz | /metrics(promhttp.HandlerFor(reg))`.

## Key Design Points

**Atomic counter** — single shared `atomic.Uint64`. Post-incremented in `Pool.Next` so the first call selects `backends[0]`. Per-worker counters were rejected (drift under uneven scheduling complicates the LB-03 fairness guarantee).

**Hop-by-hop strip** — handled by `httputil.ReverseProxy` itself. In Rewrite mode the proxy strips the canonical RFC 7230 §6.1 set (including headers named in the inbound `Connection` field) from `pr.Out` *before* `rewrite()` is called, so we don't repeat that work. `TestProxy_StripsHopByHopHeaders` confirms the behaviour.

**TTFB timeout, not total** — `Transport.ResponseHeaderTimeout = upstream-timeout`. After this elapses with no headers, RoundTrip returns a `net.Error` with `Timeout()=true` and the ErrorHandler writes `504`. Total-request timeouts would drop legitimate streaming responses.

**No retry, ever** — V1's `ErrorHandler` writes the error response and returns. There is no path that issues a second upstream attempt for any reason. This is locked in by LB-14 and verified by `TestProxy_DoesNotRetryOn5xx` and `TestProxy_DeadBackendDoesNotCauseSkip`.

**Backend label via the recorder** — `ServeHTTP` allocates one `statusRecorder` per request, stashes a pointer to it on the context (so `rewrite()` can find it via `pr.Out.Context()` — Out is cloned from In and inherits the context), and passes the recorder as the ResponseWriter. `rewrite()` writes the chosen backend URL onto `rec.backend`. Both the ErrorHandler and the post-call access log read it back. The context-on-In approach doesn't work because `httputil.ReverseProxy.ServeHTTP` runs the Director/Rewrite on a clone of the inbound request — context mutations there don't propagate to the outer `r`.

**X-Forwarded-For preservation** — In Rewrite mode, `httputil.ReverseProxy` strips client-supplied `X-Forwarded-*` from `pr.Out` *before* `rewrite()` runs (defence against a hostile client impersonating a downstream proxy). To honour LB-10's "append client IP" semantics, `rewrite()` copies the inbound XFF from `pr.In` back onto `pr.Out` *before* calling `SetXForwarded()`, which then appends the client IP correctly.

**Separate admin listener** — `/healthz` and `/metrics` are bound to `--admin-listen` (different port from `--listen`). If the data plane is overloaded with stuck upstream connections, observability still works.

**Pre-register metric labels** — `metrics.New` calls `latency.WithLabelValues(b)` for every configured backend at startup. This avoids label registration cost on the first request and means `/metrics` shows zero-valued histograms even before traffic arrives.

## Test Conventions

- Backends mocked with `httptest.NewServer` — no Docker, no external services.
- "Dead backend" tests bind a port with `net.Listen("tcp", "127.0.0.1:0")`, capture the address, then `Close()` so the URL points to nothing listening.
- LB-13 (504) test uses a backend that `time.Sleep`s past the configured `--upstream-timeout`; the test sets `upstream-timeout=50ms` so the test runs in well under a second.
- Run-cluster ports: backends on `:9001/:9002/:9003`, LB on `:7080`/`:7090`. Docker uses container-internal `:9000` for backends and exposes `:7080`/`:7090` for the LB.
