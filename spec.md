# Load Balancer — V2 (Health-aware + Resilient)

| Field   | Value              |
|---------|--------------------|
| Version | 0.2 (draft)        |
| Author  | Steve Weiland      |
| Date    | 2026-04-23         |
| Status  | In review          |

---

## 1. Overview

A single-listener HTTP reverse proxy that distributes incoming requests across a static
pool of backends using **power-of-two random choices** (P2C) over an exponentially weighted
moving average (EWMA) of per-backend latency. Each backend is fronted by a three-state
circuit breaker (Closed / Open / Half-open) updated passively from in-band request results.
Failed requests on idempotent methods are retried against a *different* backend with
exponential backoff and full jitter, capped by a global retry budget to prevent retry
storms.

V2 fixes the four deliberate weaknesses documented and measured in V1's chaos baseline
(`reports/v1-20260423T222339Z/`, seed=42, 83.30% success ratio, per-second success rate
sagging to ~67% during single-backend kill windows):

| V1 failure | V2 fix |
|------------|--------|
| No health awareness — dead backends keep getting traffic (LB-04) | Per-backend circuit breaker; Open backends excluded from selection |
| No per-backend state — no latency tracking (LB-05) | Per-backend EWMA latency score, sliding-window error counts |
| Connection failures don't skip the failed backend (LB-06) | P2C selects from the closed-circuit set only |
| No retry — neither same-backend nor cross-backend (LB-14) | Cross-backend retry on idempotent methods, exp backoff + jitter, retry budget |

V2 keeps V1's V1's hop-by-hop semantics, X-Forwarded handling, separate admin listener,
and 502/504 terminal-error mappings unchanged. Active out-of-band health checks remain
out of scope; the breaker provides health awareness from real traffic.

---

## 2. Definitions

| Term | Definition |
|------|------------|
| Backend | A single upstream HTTP server identified by a `scheme://host:port` URL |
| Pool | The ordered list of backends configured at startup; immutable for the proxy's lifetime |
| Hop-by-hop header | A header defined in [RFC 7230 §6.1](https://www.rfc-editor.org/rfc/rfc7230#section-6.1) that **MUST NOT** be forwarded by an intermediary |
| Idempotent method | `GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`, `TRACE` per RFC 7231 §4.2.2 — eligible for retry under V2 |
| EWMA | Exponentially weighted moving average: `score_new = α · sample + (1 − α) · score_prev` |
| P2C | Power-of-two random choices: pick two candidates uniformly at random, route to the one with the better score. Approximates "always pick the best" with O(1) work and no global contention |
| Circuit breaker | Per-backend state machine (Closed / Open / Half-open) gating whether a backend is eligible for selection |
| Half-open probe | The single in-flight request a breaker permits while in the Half-open state |
| Retry budget | Cap on retries per unit time as a fraction of total requests; prevents retry storms during widespread upstream failure |
| Hedged request | A speculative parallel dispatch of the same request to a second backend before the first responds — explicitly out of scope (§5) |

---

## 3. Requirements

Requirements use [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) keywords: **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, **MAY**.

### 3.1 Dispatch

| ID | Requirement |
|----|-------------|
| LB‑01 | The proxy **MUST** expose a single HTTP listener and forward each accepted request to one *initial* backend selection (subject to retry per §3.5). |
| LB‑02 | The proxy **MUST** select each request's initial backend using **power-of-two random choices** over the EWMA latency scores of the *eligible* set (Closed-circuit backends, plus any Half-open backend if no Closed exists): pick two candidates uniformly at random, route to the one with the lower EWMA score. |
| LB‑03 | Under ideal conditions (all backends Closed and equally fast), each backend **MUST** receive `total/N ± 5%` of requests over any window of ≥ 10 000 requests. (P2C is approximate; the tolerance is wider than V1's `±1`.) |
| LB‑04 | The proxy **MUST** maintain a per-backend EWMA of upstream response latency, updated on every completed request (success or failure). |
| LB‑05 | The proxy **MUST** maintain per-backend state: EWMA latency score, sliding-window error count, and circuit-breaker state. |
| LB‑06 | A backend whose breaker is **Open MUST** be excluded from the eligible set; a backend whose breaker is **Half-open MUST** be eligible only when no Closed backend exists. |
| LB‑07 | The proxy **MUST** refuse to start if the configured pool is empty, exiting with a non-zero status. |
| LB‑21 | If exactly one backend is eligible, the proxy **MUST** route to it without invoking P2C. |
| LB‑22 | If *zero* backends are eligible (all Open), the proxy **MUST** respond `503 Service Unavailable` to the client immediately. The breaker reset timers continue running so the cluster recovers without external intervention. |

### 3.2 Request forwarding

| ID | Requirement |
|----|-------------|
| LB‑08 | The proxy **MUST** forward the request method, path, query string, body, and all end-to-end headers to the chosen backend unchanged. |
| LB‑09 | The proxy **MUST** strip hop-by-hop headers per RFC 7230 §6.1 from both the upstream request and the downstream response (`Connection`, `Keep-Alive`, `TE`, `Transfer-Encoding`, `Upgrade`, `Proxy-Authenticate`, `Proxy-Authorization`, plus any header listed in the inbound `Connection` field). |
| LB‑10 | The proxy **MUST** append the immediate client IP to `X-Forwarded-For` and set `X-Forwarded-Proto` to the scheme of the inbound request. |
| LB‑11 | The proxy **MUST** propagate the *terminal* backend's response status code, end-to-end headers, and body to the client unchanged. (Earlier failed attempts in a retry sequence are not propagated.) |
| LB‑12 | When all retries are exhausted on a transport-level failure (connection refusal, reset before headers), the proxy **MUST** respond `502 Bad Gateway` carrying the URL of the *last* attempted backend in the JSON body. |
| LB‑13 | When all retries are exhausted on TTFB timeouts (default 5 s, configurable via `--upstream-timeout`), the proxy **MUST** respond `504 Gateway Timeout` carrying the URL of the *last* attempted backend in the JSON body. |

### 3.3 Configuration & operations

| ID | Requirement |
|----|-------------|
| LB‑15 | The backend pool **MUST** be configured at startup via the `--backends` CLI flag as a comma-separated list of `scheme://host:port` URLs. |
| LB‑16 | The pool **MUST** be static for the lifetime of the process; runtime add/remove is out of scope. |
| LB‑17 | The proxy **MUST** expose `GET /healthz` on a separate admin listener returning `200 OK` with body `ok` while the proxy process is running. The endpoint **MUST NOT** check backend health. |
| LB‑18 | The proxy **MUST** emit one structured log line per *client* request containing: ISO-8601 timestamp, method, path, terminal backend URL, terminal upstream status, total latency in milliseconds, `attempt` count (1, 2, or 3), and `backend_chain` (e.g. `b1→b2` if the first attempt failed and the second succeeded). |
| LB‑19 | The proxy **MUST** expose `GET /metrics` on the admin listener in Prometheus text format with at minimum the metrics enumerated in LB‑52. |
| LB‑20 | The proxy **MAY** support `--log-format=json|text` (default `json`). |
| LB‑50 | The proxy **MUST** accept tuning flags with the defaults given in §3.4–3.5: `--ewma-alpha`, `--breaker-window`, `--breaker-error-threshold`, `--breaker-reset-timeout`, `--breaker-reset-cap`, `--max-retries`, `--retry-base`, `--retry-cap`, `--retry-budget`. |

### 3.4 Health & circuit breaking (passive)

| ID | Requirement |
|----|-------------|
| LB‑30 | The proxy **MUST** maintain a per-backend EWMA of completed-request latency: `score_new = α · latency_observed + (1 − α) · score_prev`, with α default 0.1 (`--ewma-alpha`). The first observation seeds the score with no smoothing. |
| LB‑31 | A backend's EWMA score **MUST** decay toward zero when idle, so a recovered backend gradually re-enters the P2C selection set without an explicit reset. Decay applied lazily on the next selection (`score *= (1 − α)^idle_intervals` clamped to ≥ a small floor). |
| LB‑32 | The proxy **MUST** maintain a per-backend circuit breaker as a finite state machine with three states: **Closed** (initial), **Open**, **Half-open**. State transitions are atomic. |
| LB‑33 | While Closed, the breaker **MUST** track the error count over a sliding window of the last `--breaker-window` requests (default 10). If `errors / total > --breaker-error-threshold` (default 0.5) **and** `total ≥ --breaker-window`, the breaker **MUST** transition to Open. |
| LB‑34 | While Open, the proxy **MUST NOT** route any request to the backend. The breaker **MUST** record the time it entered Open and the current `reset_timeout` (initial value `--breaker-reset-timeout`, default 10 s). |
| LB‑35 | After `reset_timeout` elapses in Open state, the breaker **MUST** transition to Half-open. |
| LB‑36 | While Half-open, the proxy **MUST** allow exactly one in-flight probe request through; concurrent selections **MUST** treat the backend as ineligible. On probe success the breaker **MUST** transition to Closed and reset `reset_timeout` to its initial value; on probe failure (transport error, 5xx, or timeout) it **MUST** transition back to Open and double `reset_timeout`, capped at `--breaker-reset-cap` (default 60 s). |
| LB‑37 | An "error" for breaker accounting **MUST** include: connection failures, TTFB timeouts (LB‑13), and HTTP 5xx responses. 4xx responses **MUST NOT** count as errors (they signal client problems, not backend health). |
| LB‑38 | The proxy **MUST NOT** perform background active health checks in V2; breaker state is updated *passively* from in-band request outcomes only. (Active health checks remain out of scope per §5.) |

### 3.5 Retry

| ID | Requirement |
|----|-------------|
| LB‑40 | On a transport error (LB‑12) or 5xx response, the proxy **MUST** retry the request against a *different* backend selected by the same P2C algorithm (LB‑02), up to `--max-retries` additional attempts (default 2 → 3 total). |
| LB‑41 | Retries **MUST** be confined to idempotent methods (GET, HEAD, PUT, DELETE, OPTIONS, TRACE per RFC 7231 §4.2.2). For non-idempotent methods (POST, PATCH, custom) the proxy **MUST** return the first response or error verbatim, with no retry attempted. |
| LB‑42 | Retry attempt N **MUST** wait `min(--retry-base × 2^(N−1), --retry-cap)` ± full jitter before issuing, where defaults are base = 10 ms and cap = 200 ms. ("Full jitter" per AWS Architecture Blog: sleep a uniformly random duration in `[0, computed_delay]`.) |
| LB‑43 | A retry **MUST NOT** select a backend whose breaker is Open, and **MUST NOT** select the backend that just failed. If P2C cannot find any other eligible backend, the retry **MUST** be skipped and the loop **MUST** terminate; the proxy returns the last failure (LB‑22's 503 if zero eligible remain). |
| LB‑44 | The proxy **MUST** maintain a global retry budget of at most `--retry-budget` (default 0.10 = 10%) of total requests in any 1-second window. When the budget is exhausted, further retries **MUST NOT** be issued until the budget refills; the originating failure passes through directly. (Prevents retry storms when the entire upstream fleet is degraded.) |
| LB‑45 | The terminal response after retries **MUST** carry the status of the *last* attempt. The 502/504 mapping (LB‑12, LB‑13) applies to the terminal attempt only. |

### 3.6 Metrics (additions for V2)

| ID | Requirement |
|----|-------------|
| LB‑52 | `/metrics` (LB‑19) **MUST** include at minimum: |
|       | • `lb_requests_total{backend, status}` — counter (unchanged from V1, but `backend` labels the *terminal* backend) |
|       | • `lb_upstream_latency_seconds{backend}` — histogram (unchanged from V1) |
|       | • `lb_retries_total{from_backend, to_backend, reason}` — counter; `reason` ∈ `{transport, 5xx, timeout}` |
|       | • `lb_breaker_state{backend}` — gauge: `0` Closed, `1` Open, `2` Half-open |
|       | • `lb_breaker_transitions_total{backend, from, to}` — counter |
|       | • `lb_ewma_score_seconds{backend}` — gauge |
|       | • `lb_eligible_backends` — gauge: count of currently eligible (non-Open) backends |
|       | • `lb_retry_budget_exhausted_total` — counter |

---

## 4. Inputs / Outputs

### Proxy HTTP surface

```
ANY  /*

  Upstream-success path (after 0 or more retries):
    Status, end-to-end headers, and body forwarded verbatim from the terminal backend.

  Header rewrites applied to every upstream request (initial and retries):
    X-Forwarded-For:    <existing>, <client-ip>      (appended; created if absent)
    X-Forwarded-Proto:  http | https                 (set to inbound scheme)
    Connection, Keep-Alive, TE, Transfer-Encoding,
    Upgrade, Proxy-Authenticate, Proxy-Authorization: stripped
    Any header named in the inbound Connection field: stripped

  502 Bad Gateway        { "error": "upstream connection failed", "backend": "<last-attempted-url>", "attempts": <N> }
  503 Service Unavailable{ "error": "no eligible backends",       "attempts": 0 }
  504 Gateway Timeout    { "error": "upstream timeout",           "backend": "<last-attempted-url>", "attempts": <N> }
```

### Admin HTTP surface

```
GET /healthz
  200 OK
  Content-Type: text/plain
  Body: ok

GET /metrics
  200 OK
  Content-Type: text/plain; version=0.0.4
  Body: Prometheus exposition format

  # HELP lb_requests_total Total proxied requests by terminal backend and upstream status.
  # TYPE lb_requests_total counter
  lb_requests_total{backend="http://localhost:7081",status="200"} 1234

  # HELP lb_retries_total Cross-backend retries by source, destination, and reason.
  # TYPE lb_retries_total counter
  lb_retries_total{from_backend="http://localhost:7081",to_backend="http://localhost:7082",reason="5xx"} 12

  # HELP lb_breaker_state Per-backend circuit-breaker state (0=Closed, 1=Open, 2=Half-open).
  # TYPE lb_breaker_state gauge
  lb_breaker_state{backend="http://localhost:7081"} 0

  # HELP lb_breaker_transitions_total Breaker state-machine transitions.
  # TYPE lb_breaker_transitions_total counter
  lb_breaker_transitions_total{backend="http://localhost:7081",from="closed",to="open"} 3

  # HELP lb_ewma_score_seconds Per-backend EWMA latency score in seconds.
  # TYPE lb_ewma_score_seconds gauge
  lb_ewma_score_seconds{backend="http://localhost:7081"} 0.0007

  # HELP lb_eligible_backends Count of currently eligible (non-Open) backends.
  # TYPE lb_eligible_backends gauge
  lb_eligible_backends 3

  # HELP lb_retry_budget_exhausted_total Requests where retry was skipped because the budget was exhausted.
  # TYPE lb_retry_budget_exhausted_total counter
  lb_retry_budget_exhausted_total 0

  # HELP lb_upstream_latency_seconds Upstream response latency.
  # TYPE lb_upstream_latency_seconds histogram
  lb_upstream_latency_seconds_bucket{backend="http://localhost:7081",le="0.005"} 800
  ...
```

### Structured access log (one line per *client* request)

```json
{
  "time":          "2026-04-23T19:43:55.004Z",
  "level":         "INFO",
  "msg":           "proxied",
  "method":        "GET",
  "path":          "/anything",
  "backend":       "http://localhost:7082",
  "status":        200,
  "latency_ms":    1.487,
  "attempt":       2,
  "backend_chain": "http://localhost:7081→http://localhost:7082"
}
```

### CLI flags (`lbserver`)

```
# Unchanged from V1
--listen                    string   HTTP listen address for the proxy            (default ":7080")
--admin-listen              string   HTTP listen address for /healthz, /metrics   (default ":7090")
--backends                  string   comma-separated backend URLs (required)
--upstream-timeout          duration TTFB timeout per upstream request            (default 5s)
--log-format                string   "json" or "text"                             (default "json")

# New in V2 — health & breaker
--ewma-alpha                float    EWMA smoothing factor (0 < α ≤ 1)            (default 0.1)
--breaker-window            int      sliding-window size for breaker error rate   (default 10)
--breaker-error-threshold   float    error ratio that trips Closed → Open         (default 0.5)
--breaker-reset-timeout     duration initial Open → Half-open delay               (default 10s)
--breaker-reset-cap         duration maximum Open → Half-open delay (doubling)    (default 60s)

# New in V2 — retry
--max-retries               int      additional attempts after the first failure  (default 2)
--retry-base                duration base backoff before retry attempt 1          (default 10ms)
--retry-cap                 duration cap on per-attempt backoff                   (default 200ms)
--retry-budget              float    fraction of total requests that may retry    (default 0.10)
```

---

## 5. Out of Scope

Each of the following is an explicit non-goal of V2:

- **Active (out-of-band) health checks** — V2 uses passive (in-band) breaker accounting only
- **Sticky sessions / consistent hashing** for session affinity
- **Dynamic backend registration, deregistration, or weighting** at runtime
- **Graceful drain** of in-flight requests on shutdown (process exits when SIGTERM received)
- **TLS termination at the proxy or mTLS to backends**
- **HTTP/2, gRPC, or WebSocket upgrades**
- **Authentication, authorization, or rate limiting** at the proxy layer
- **Request hedging** (parallel speculative dispatch to multiple backends)
- **Multi-region or zone-aware routing**
- **Idempotency-Key header support** to extend retry to POST/PATCH (LB‑41 forbids it for V2)
- **Request or response body buffering** beyond Go `httputil.ReverseProxy` defaults

---

## 6. Resolved Decisions

| # | Question | Decision |
|---|----------|----------|
| Q1 | (V1) Counter primitive — shared atomic vs per-worker? | (V2 supersedes this — selection is now P2C, no shared counter.) |
| Q2 | (V1) Empty pool at startup — fail-fast vs serve 502? | Fail-fast with non-zero exit (LB‑07). Returning 502 on every request masks a config error. *(Carried over from V1.)* |
| Q3 | (V1) Timeout semantics — total request vs TTFB? | Time-to-first-byte (LB‑13). A total-request timeout drops legitimate streaming responses. *(Carried over from V1.)* |
| Q4 | **EWMA α**: 0.05 (slow), 0.1 (medium), 0.3 (jumpy)? | **0.1** default. Slow enough that one unlucky outlier doesn't dominate; fast enough that a recovered backend re-enters the pool within ~10 requests. Tunable via `--ewma-alpha`. |
| Q5 | **Breaker error threshold**: fixed count or ratio? | **Ratio over a sliding window** (LB‑33). Fixed count would trip immediately on a tiny pool under tiny RPS. Ratio scales naturally with throughput. |
| Q6 | **Retry on POST/PATCH**: never, opt-in via header, or `Idempotency-Key`? | **Never** in V2 (LB‑41). Default-safe. `Idempotency-Key` support is a future extension, listed in §5. |
| Q7 | **Selection algorithm**: P2C vs least-connections vs weighted random? | **P2C** (LB‑02). Least-connections needs accurate in-flight counts (more state, more contention); weighted random requires explicit weights (deferred to a stretch). P2C is O(1), contention-free, and provably outperforms uniform random for skewed-latency pools (Mitzenmacher 2001). |
| Q8 | **Retry budget**: per-instance vs per-backend? | **Per-instance, 1-second window** (LB‑44). Per-backend budgets fragment under skew (a slow-but-up backend exhausts its budget while peers idle). Per-instance is the metric Envoy/Istio settled on after seeing retry storms in the wild. |
| Q9 | **Half-open probe concurrency**: one probe or N probes per reset? | **Exactly one** (LB‑36). Multiple parallel probes under load can re-trip the breaker on a backend that's recovering, defeating the point of the Open delay. |

---

## 7. Revision History

| Version | Date | Author | Notes |
|---------|------|--------|-------|
| 0.1 | 2026-04-23 | Steve Weiland | Initial V1 draft (blind round-robin) |
| 0.2 | 2026-04-23 | Steve Weiland | V2 draft: P2C + EWMA, per-backend circuit breakers, cross-backend retry with budget |

---

## 8. Acceptance

V2 is *measurably* an improvement over V1, against the artifact V1 produced:

**Baseline** — `reports/v1-20260423T222339Z/` (seed=42, deterministic):

| Metric | V1 baseline |
|--------|-------------|
| Requests | 12 000 |
| Success ratio | 83.30% |
| 502 count | 2 004 |
| p50 latency | 1.33 ms |
| p99 latency | 5.70 ms |
| Per-second success rate during kill window | ~0.665 (= 2/3) |
| Chaos events in run | 4 KILL + 2 REVIVE on 10 s cadence (seed-driven; deeper outage windows than the original random run) |

**V2 acceptance** — `make chaos-v2` (which runs `./bin/chaos --tag=v2 --seed=42`)
against the V2 implementation **MUST** produce a `reports/v2-<ts>/` whose
`summary.txt` and `timeseries.csv` satisfy:

| Criterion | Threshold | Source |
|-----------|-----------|--------|
| Overall success ratio | **≥ 99.0%** | `summary.txt` `Success [ratio]` |
| 502 count | **≤ 0.5%** of total requests | `summary.txt` `Status Codes` |
| 503 count | 0 *during steady-state*; brief spikes during the moment of breaker tripping are acceptable | `summary.txt` `Status Codes` |
| p99 latency | **No regression vs V1 baseline** (V1 was 5.70 ms; V2 must be within +1 ms) | `summary.txt` `Latencies [p99]` |
| Per-second success rate, every full 1-second bin | **≥ 0.99** | `timeseries.csv` `success_rate` column (exclude the partial last bin if the attack ended mid-second) |
| Retries observed | `lb_retries_total` > 0 (proves cross-backend retry is active) | `/metrics` post-run |
| Breaker state machine | Verified by unit tests in `internal/breaker/` rather than chaos. The specific seed=42 run may not produce sustained-enough errors to trip the breaker because retry absorbs them first; a dedicated chaos scenario (longer kill windows, no retry) would be needed to exercise it end-to-end. The state machine itself is covered by `TestClosed_TripsOnErrorRateAboveThreshold`, `TestOpen_RejectsUntilTimeout`, `TestHalfOpen_OneProbeAtATime`, `TestHalfOpen_SuccessReclosesAndResetsTimeout`, `TestHalfOpen_FailureReopensWithDoubledTimeoutCapped`. | `internal/breaker/breaker_test.go` |

The chaos seed used for the V1 baseline is the `<seed>` printed at the start of the V1
report (or any fixed value supplied via `--seed`); using the same seed gives both runs
identical kill/revive timelines for a fair comparison.

If V2 fails any criterion above, V2 is not done. Tune the relevant parameter
(typically `--ewma-alpha`, `--breaker-window`, or `--max-retries`) and rerun.
