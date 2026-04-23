# Chaos reports

Each subdirectory here is one run of `make chaos`. Generated reports are
gitignored (binary results files); only this README is checked in.

## Format

```
reports/<tag>-<UTC-timestamp>/
├── vegeta.bin       Raw vegeta results stream. Replay with:
│                      vegeta report < vegeta.bin
│                      vegeta plot   < vegeta.bin > plot.html
├── timeseries.csv   Per-second bins:
│                      ts_unix,ts_iso,total,success,success_rate,p50_ms,p99_ms
│                    Plot in any tool (Excel, gnuplot, datasette).
├── chaos.log        One line per kill/revive event:
│                      <ISO timestamp>\tKILL|REVIVE\t<backend id>\terr=...
└── summary.txt      Vegeta's text summary (status codes, latency percentiles).
```

## Generate

```bash
make chaos                                # default: 60s @ 200 rps, kill/revive every 10s, tag=v1
./bin/chaos --tag=v2 --duration=120s      # custom: 2 min run, tagged v2
./bin/chaos --rps=500 --chaos-interval=5s # higher load, faster chaos
./bin/chaos --seed=42                     # deterministic kill/revive sequence
```

Flags:

| Flag | Default | Notes |
|------|---------|-------|
| `--duration` | `60s` | Total attack length |
| `--rps` | `200` | Constant request rate |
| `--chaos-interval` | `10s` | Time between kill/revive decisions |
| `--backends` | `3` | Number of echo backends to spawn |
| `--tag` | `v1` | Goes in the report dir name (`v1-…`, `v2-…` for comparison) |
| `--seed` | now-ns | RNG seed for kill/revive choice — pin for reproducible reports |
| `--lb-listen` / `--lb-admin-listen` / `--backend-base-port` | `:7080` / `:7090` / `9001` | Override if those are taken |

## V1 vs V2 comparison

The chaos runner is identical for V1 and V2 — same flags, same artifacts. To
compare:

```bash
git checkout v1.0.0
make chaos                                  # → reports/v1-<ts>/
git checkout v2.0.0
./bin/chaos --tag=v2                        # → reports/v2-<ts>/
```

Diff the two `summary.txt` files and overlay the two `timeseries.csv` files
to produce the before/after graph for the README.

Expected V1 signature (this is the *broken* baseline):
- Success rate sags during kill windows in proportion to dead/total.
- p99 stays low during kills (502s are fast) but can spike on revive due to
  TCP connection re-establishment to the freshly started backend.
- `summary.txt` shows non-trivial 502 counts under `Status Codes`.

Expected V2 signature:
- Success rate stays > 99% throughout — circuit breaker absorbs the failures
  and cross-backend retry routes around them.
- p99 stays flat — EWMA scoring + P2C avoids the slow/dead backend for new
  requests once the breaker opens.
