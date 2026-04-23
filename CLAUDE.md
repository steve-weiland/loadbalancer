# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
make build                                           # bin/lbserver, bin/echobackend, bin/chaos
make test                                            # go test -race -count=1 ./...
go test -race -count=1 -run TestName ./internal/proxy/...   # single test
make run-cluster                                     # 3 backends + 1 LB, all backgrounded
make stop-cluster                                    # kills lbserver and echobackend
make run-docker                                      # docker compose up --build -d
make stop-docker                                     # docker compose down
make chaos                                           # V1 baseline: ./bin/chaos --tag=v1 --seed=42
make chaos-v2                                        # V2 acceptance: ./bin/chaos --tag=v2 --seed=42
make chaos-report                                    # cat latest summary.txt + chaos.log
```

Go 1.23+ required.

## Architecture

```
cmd/lbserver/main.go         — flag parsing, validation, wiring; SIGTERM = immediate close, no drain
cmd/echobackend/main.go      — toy backend used by run-cluster + docker-compose; not part of V2 surface
cmd/chaos/main.go            — vegeta load + kill/revive runner; spawns its own cluster, writes reports/<tag>-<ts>/{vegeta.bin,timeseries.csv,chaos.log,summary.txt,seed.txt}

internal/ewma/
  ewma.go                    — Score: thread-safe EWMA with lazy idle decay
internal/breaker/
  breaker.go                 — Breaker: 3-state machine (Closed/Open/Half-open); sliding window error counter; doubling reset cap; OnTransition callback
internal/backend/
  backend.go                 — Backend struct: composes URL + EWMA + Breaker; Eligibility() classifies for selection
internal/retrybudget/
  budget.go                  — Budget: 1-second sliding-window token bucket (100 × 10ms slots); TryConsume / Observe / ExhaustedCount

internal/pool/
  pool.go                    — Pool: []*Backend + per-Pool *rand.Rand; Pick / PickAvoiding (P2C over EligiblePrimary, fallback to EligibleProbe, error if all Open)

internal/proxy/
  proxy.go                   — Proxy: hand-rolled forward+retry loop (no httputil.ReverseProxy in V2); per-attempt response buffering up to maxRetryBodyBytes (1 MiB); idempotent-only retry; full-jitter backoff
  headers.go                 — copyEndToEndHeaders, stripHopByHopFromConnection, setForwardedHeaders
  errorpage.go               — writeError: JSON body for 502/503/504

internal/admin/
  admin.go                   — NewMux: /healthz (200 ok) and /metrics (promhttp) on a separate listener
internal/metrics/
  metrics.go                 — Observer interface (Noop, PromObserver); 2 counters + 1 histogram + 1 budget counter (registered) + 3 gauges (GaugeFunc, read live state on each scrape)
```

**Request path (V2):**
```
client → lbserver :7080 → proxy.ServeHTTP
                             ↓ snapshot req body (≤1 MiB)
                             ↓ for attempt = 1..1+MaxRetries:
                             ↓     if attempt > 1: budget.TryConsume + sleepBackoff(jitter)
                             ↓     b = pool.PickAvoiding(lastFailed)  ← P2C over EWMA
                             ↓     b.Breaker.Allow()                  ← claim half-open slot if needed
                             ↓     out = transport.RoundTrip → capture status+headers+body
                             ↓     b.Breaker.Record(success)          ← LB-37: 5xx counts as error
                             ↓     b.EWMA.Update(out.latency)
                             ↓     terminal = &out
                             ↓     if !retryable or attempt == max: break
                             ↓     lastFailed = b
                             ↓ copy terminal to client + obs.Observe + access log
```

**Admin path** (separate listener `:7090`): `client → admin.NewMux → /healthz | /metrics(promhttp.HandlerFor(reg))`. Gauges (`lb_breaker_state`, `lb_ewma_score_seconds`, `lb_eligible_backends`) are `GaugeFunc`s reading live backend state on each scrape.

## Key Design Points

**P2C over EWMA** — `Pool.PickAvoiding` partitions backends into Closed (eligible primary) and Half-open (eligible probe). With ≥2 primaries, picks two random distinct indices and returns the one with lower EWMA. With 1 primary, returns it directly (LB-21). With 0 primaries but ≥1 probe, returns the probe. With 0 of either, returns `ErrNoEligible` (proxy → 503, LB-22).

**Breaker is passive** — updated only from in-band request results (LB-38). No background polling. The breaker's `OnTransition` callback fires `metrics.Observer.RecordBreakerTransition` so `lb_breaker_transitions_total` reflects state changes.

**Half-open probe slot** — `Breaker.Allow()` claims the single in-flight probe slot in Half-open state; concurrent `Allow()` returns false. The proxy calls `Allow()` *after* the pool has handed it the backend (the pool already filtered by eligibility), so the slot reservation happens at the right moment.

**No `httputil.ReverseProxy` in V2** — V1 used it; V2 cannot because it needs to inspect the upstream response status before deciding whether to forward (success, 4xx) or retry (5xx, transport error, TTFB timeout). The proxy issues the upstream call directly via `http.Transport.RoundTrip`.

**Response buffering CALLOUT** — V2 spec §5 lists "body buffering beyond defaults" as out of scope, but cross-backend retry (LB-40) requires it. Resolved by capping at `maxRetryBodyBytes = 1 MiB`. Above the cap, a request is treated as not-retry-eligible and streams through on the first attempt only. Documented in the package doc of `internal/proxy/proxy.go`. Revisit if a chaos run shows large-response retry need or memory pressure.

**Request body re-readability** — A retried PUT must send the same body to a different backend. `snapshotBody` reads the inbound body into memory (capped at 1 MiB) at the top of `ServeHTTP`. Bodies above the cap stream directly with no retry; this is logged. Idempotent methods almost never carry large bodies (GET/HEAD/DELETE/OPTIONS/TRACE → no body; PUT can but rarely does).

**Idempotency check** — `idempotent(method)` enforces LB-41. Only GET, HEAD, PUT, DELETE, OPTIONS, TRACE retry. POST/PATCH always pass through with no retry.

**Backoff with full jitter** — `sleepBackoff(retryAttempt)` computes `min(retryBase × 2^(N-1), retryCap)` and sleeps a uniformly random duration in `[0, that]`. The proxy holds its own `*rand.Rand` (mutex-guarded) so jitter is deterministic in tests if seeded explicitly.

**Retry budget** — Per-`Proxy` `*retrybudget.Budget`. `Observe()` on every client request; `TryConsume()` on every retry attempt. Returns false when retries-in-window exceeds `budget × total-in-window`. Backed by 100×10ms slots in a ring buffer.

**Backward-compatible behaviour preserved from V1:**
- Hop-by-hop strip (now in `headers.go` since we're not using `httputil.ReverseProxy`)
- X-Forwarded-For appending + X-Forwarded-Proto
- 502 on transport error, 504 on TTFB timeout (now: terminal *after* retries exhausted)
- Separate admin listener
- Empty-pool startup fail-fast

## Test Conventions

- Backends mocked with `httptest.NewServer` — no Docker, no external services.
- "Dead backend" tests bind a port with `net.Listen("tcp", "127.0.0.1:0")`, capture the address, then `Close()` so the URL points to nothing listening.
- LB-13 (504) test uses a backend that `time.Sleep`s past the configured `--upstream-timeout`; the test sets `upstream-timeout=50ms` so the test runs in well under a second.
- Breaker tests use a `fakeClock` (injected via `WithClock`) to advance time deterministically rather than `time.Sleep`.
- Retry-related proxy tests use `retrybudget.New(1.0)` (100% budget) by default so retry isn't budget-limited unless the test specifically exercises the budget.
- Run-cluster ports: backends on `:9001/:9002/:9003`, LB on `:7080`/`:7090`. Docker uses container-internal `:9000` for backends and exposes `:7080`/`:7090` for the LB.
- Chaos runner output lives at `$TMPDIR/chaos-{lbserver,b1,b2,b3}.log` on macOS (NOT `/tmp/chaos-*`). Useful when investigating a chaos run's lbserver behaviour after the fact — grep for `"attempt":2` for retry events, `→` in `backend_chain` for cross-backend hops.
