# Milestone 6: Metrics, docker-compose, and README

Status: done. The project is now runnable with one command, instrumented with
Prometheus, and documented with an architecture diagram and measured numbers.

## What was built

- **`internal/metrics`** — Prometheus collectors (messages, parse errors, trades,
  book gaps, resyncs, sink batch rows, flush latency, end-to-end latency; per
  venue where applicable) plus a `/metrics` server. Connectors, book feeds, and
  the sink are instrumented.
- **Metrics config** — `metrics.addr` in the YAML; the server starts in the
  config-driven run.
- **`Dockerfile`** — multi-stage: static binary built with `golang:1.26`, shipped
  on `distroless/static`.
- **`docker-compose.yml`** — now runs the app *and* ClickHouse together, with the
  app waiting on ClickHouse health and reading `config.docker.yaml`.
- **`README.md`** — architecture diagram (mermaid), the three-venue integrity
  table, quick start, config reference, metrics list, layout, and measured
  numbers.

## Design decisions (docs/DECISIONS.md)

- **D20** — package-global Prometheus collectors (no plumbing); one compose builds
  and runs app + ClickHouse; distroless image.

## Verification

- `docker compose up -d --build` brings up ClickHouse and the app; the
  containerized app connected to ClickHouse over the compose network and ingested
  all three venues (~2k trades/minute of majors), with `/metrics` serving live
  counters and histograms.
- Measured for the README: sustained trade rate, end-to-end latency p50/p99 per
  venue, ClickHouse compression, and example query latency — with an honest
  clock-skew caveat on the latency numbers.

## What's deliberately left (honest scope)

- **book_updates persistence.** Books are reconstructed and validated in memory
  across all venues, but not written to a `book_updates` table yet. The engine and
  the schema shape are ready; wiring the sink for book updates (with a
  checksum/validity column and the 30-day TTL) is the natural next step.
- **72-hour soak + gap/resync counts over time.** The metrics exist; the numbers
  in the README are from short local runs, not a multi-day run.
- **Grafana dashboard / Prometheus scrape config.** The endpoint is there; a
  packaged dashboard would complete the observability story.
- **Cross-venue symbol normalization** (BTC-USD / BTC/USD / BTC-USDT) and
  **per-venue sink isolation** remain the two open architectural threads from
  milestones 4-5.

## Milestones recap

1. Coinbase connector -> normalized trades to stdout. ✓
2. ClickHouse sink with batching. ✓
3. Order book engine with gap detection + resync. ✓
4. Kraken (second venue) with live CRC32 checksum validation. ✓
5. OKX (third venue) + config-driven multi-venue selection. ✓
6. Metrics + docker-compose + README with measured numbers. ✓

All six milestones complete, each verified against live exchange feeds.
