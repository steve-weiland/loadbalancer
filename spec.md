# Load Balancer — V1 (Blind Round-Robin)

| Field   | Value              |
|---------|--------------------|
| Version | 0.1 (draft)        |
| Author  | Steve Weiland      |
| Date    | 2026-04-23         |
| Status  | In review          |

---

## 1. Overview

A single-listener HTTP reverse proxy that distributes incoming requests across a static
pool of backend servers using a shared atomic counter (`counter % len(backends)`). Each
backend receives `1/N` of traffic under ideal conditions.

V1 is intentionally minimal: it has no health awareness, no per-backend state, and no
cross-backend retry. These omissions are not oversights — they are the failure surface
that the Week 3 chaos test will exercise and that V2 will fix with EWMA scoring,
exponential backoff retry across backends, and per-backend circuit breakers.

---

## 2. Definitions

| Term | Definition |
|------|------------|
| Backend | A single upstream HTTP server identified by a `scheme://host:port` URL |
| Pool | The ordered list of backends configured at startup; immutable for the proxy's lifetime |
| Dispatcher | The component that selects which backend receives the next request |
| Round-robin counter | A shared `atomic.Uint64` incremented once per dispatched request |
| Upstream request | The HTTP request the proxy issues to a backend on behalf of the client |
| Hop-by-hop header | A header defined in [RFC 7230 §6.1](https://www.rfc-editor.org/rfc/rfc7230#section-6.1) that **MUST NOT** be forwarded by an intermediary |
| Idempotent method | `GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`, `TRACE` per RFC 7231 — relevant in V2 retry policy, not V1 |

---

## 3. Requirements

Requirements use [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) keywords: **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, **MAY**.

### 3.1 Dispatch

| ID | Requirement |
|----|-------------|
| LB‑01 | The proxy **MUST** expose a single HTTP listener and forward each accepted request to exactly one backend selected from the static pool. |
| LB‑02 | The proxy **MUST** select the next backend by `atomic.AddUint64(&counter, 1) % uint64(len(backends))`; no other selection state is permitted. |
| LB‑03 | Under ideal conditions (all backends responsive, uniform latency), each backend **MUST** receive `⌊total/N⌋ ± 1` requests over any window of ≥ 1000 requests. |
| LB‑04 | The proxy **MUST NOT** perform active health checks against backends. |
| LB‑05 | The proxy **MUST NOT** track per-backend latency, error rate, success rate, or any liveness state. |
| LB‑06 | A connection failure to a backend **MUST NOT** cause that backend to be skipped, demoted, or removed from rotation; the next request advances the counter as normal. |
| LB‑07 | The proxy **MUST** refuse to start if the configured pool is empty, exiting with a non-zero status. |

### 3.2 Request forwarding

| ID | Requirement |
|----|-------------|
| LB‑08 | The proxy **MUST** forward the request method, path, query string, body, and all end-to-end headers to the chosen backend unchanged. |
| LB‑09 | The proxy **MUST** strip hop-by-hop headers per RFC 7230 §6.1 from both the upstream request and the downstream response (`Connection`, `Keep-Alive`, `TE`, `Transfer-Encoding`, `Upgrade`, `Proxy-Authenticate`, `Proxy-Authorization`, plus any header listed in the inbound `Connection` field). |
| LB‑10 | The proxy **MUST** append the immediate client IP to `X-Forwarded-For` and set `X-Forwarded-Proto` to the scheme of the inbound request. |
| LB‑11 | The proxy **MUST** propagate the backend's response status code, end-to-end headers, and body to the client unchanged. |
| LB‑12 | On failure to establish a TCP connection to the chosen backend or on a connection reset before any response bytes are received, the proxy **MUST** respond `502 Bad Gateway` to the client. |
| LB‑13 | If the backend does not return response headers within the configured upstream timeout (default 5 s), the proxy **MUST** respond `504 Gateway Timeout` and close the upstream connection. |
| LB‑14 | The proxy **MUST NOT** retry a request against any backend — same or different — for any status code or transport error in V1. |

### 3.3 Configuration & operations

| ID | Requirement |
|----|-------------|
| LB‑15 | The backend pool **MUST** be configured at startup via the `--backends` CLI flag as a comma-separated list of `scheme://host:port` URLs. |
| LB‑16 | The pool **MUST** be static for the lifetime of the process; runtime add/remove is out of scope. |
| LB‑17 | The proxy **MUST** expose `GET /healthz` on a separate admin listener returning `200 OK` with body `ok` while the proxy process is running. The endpoint **MUST NOT** check backend health. |
| LB‑18 | The proxy **SHOULD** emit one structured log line per dispatched request containing: ISO-8601 timestamp, method, path, chosen backend URL, upstream status code, upstream latency in milliseconds. |
| LB‑19 | The proxy **SHOULD** expose `GET /metrics` on the admin listener in Prometheus text format with at minimum: `lb_requests_total{backend, status}` (counter) and `lb_upstream_latency_seconds{backend}` (histogram). |
| LB‑20 | The proxy **MAY** support `--log-format=json|text` (default `json`). |

---

## 4. Inputs / Outputs

### Proxy HTTP surface

```
ANY  /*

  Upstream-success path:
    Status, end-to-end headers, and body forwarded verbatim from the chosen backend.

  Header rewrites applied to the upstream request:
    X-Forwarded-For:    <existing>, <client-ip>      (appended; created if absent)
    X-Forwarded-Proto:  http | https                 (set to inbound scheme)
    Connection, Keep-Alive, TE, Transfer-Encoding,
    Upgrade, Proxy-Authenticate, Proxy-Authorization: stripped
    Any header named in the inbound Connection field: stripped

  502 Bad Gateway        { "error": "upstream connection failed", "backend": "<url>" }
  504 Gateway Timeout    { "error": "upstream timeout",          "backend": "<url>" }
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

  # HELP lb_requests_total Total proxied requests by backend and upstream status.
  # TYPE lb_requests_total counter
  lb_requests_total{backend="http://localhost:7081",status="200"} 1234
  lb_requests_total{backend="http://localhost:7081",status="502"} 7
  ...

  # HELP lb_upstream_latency_seconds Upstream response latency.
  # TYPE lb_upstream_latency_seconds histogram
  lb_upstream_latency_seconds_bucket{backend="http://localhost:7081",le="0.005"} 800
  ...
```

### CLI flags (`lbserver`)

```
--listen            string   HTTP listen address for the proxy (default ":7080")
--admin-listen      string   HTTP listen address for /healthz and /metrics (default ":7090")
--backends          string   comma-separated backend URLs, e.g.
                             "http://localhost:7081,http://localhost:7082,http://localhost:7083"
                             (required; empty pool causes startup to fail)
--upstream-timeout  duration time-to-first-byte timeout per upstream request (default 5s)
--log-format        string   "json" or "text" (default "json")
```

---

## 5. Out of Scope

Everything in this list is deferred to V2 or beyond. Each is an explicit non-goal of V1:

- Active or passive health checks
- Circuit breaking (per-backend state machine)
- Cross-backend retry, request hedging, or fallback dispatch
- Latency-aware routing (EWMA, power-of-two-choices, p99 estimation)
- Sticky sessions / consistent hashing for session affinity
- Dynamic backend registration, deregistration, or weighting
- TLS termination at the proxy or mTLS to backends
- HTTP/2, gRPC, or WebSocket upgrades
- Request or response body buffering beyond Go `httputil.ReverseProxy` defaults
- Authentication, authorization, or rate limiting at the proxy layer
- Access logs in any format other than the single structured log line per request
- Graceful drain of in-flight requests on shutdown (process exits when SIGTERM received)

---

## 6. Resolved Decisions

| # | Question | Decision |
|---|----------|----------|
| Q1 | **Counter primitive**: shared `atomic.Uint64` vs per-worker counters reconciled periodically? | Shared `atomic.Uint64`. Per-worker counters drift under uneven goroutine scheduling and complicate the LB‑03 fairness guarantee. Contention on a single atomic is irrelevant at V1's target scale (single host, < 10k RPS). |
| Q2 | **Empty pool at startup**: fail-fast vs serve `502` on every request? | Fail-fast with non-zero exit (LB‑07). Returning `502` on every request would mask a configuration error and produce noisy alerts that look like a backend outage. |
| Q3 | **Timeout semantics**: total request timeout vs time-to-first-byte? | Time-to-first-byte (LB‑13). A total-request timeout would drop legitimate streaming responses; TTFB cleanly distinguishes "backend is stuck" from "response is large". |

---

## 7. Revision History

| Version | Date | Author | Notes |
|---------|------|--------|-------|
| 0.1 | 2026-04-23 | Steve Weiland | Initial V1 draft |
