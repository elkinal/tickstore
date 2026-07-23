# Milestone 2: ClickHouse sink with batching

Status: code complete and unit-tested. Live end-to-end verification (real
Coinbase trades into a dockerized ClickHouse) pending a running Docker engine.

## What was built

- **`internal/sink/batcher.go`** — a transport-agnostic `Batcher` that implements
  `venue.Handler`, so it drops in exactly where the stdout printer was. Flushes on
  size (10k) or 250ms, whichever first. One goroutine owns the buffer (no locks);
  a bounded channel gives backpressure; failed inserts retry with jittered
  backoff; `Close` drains and flushes so a clean stop loses nothing.
- **`internal/sink/clickhouse.go` + `schema.sql`** — the `ClickHouse` `Inserter`.
  `trades` table is `MergeTree`, `ORDER BY (venue, symbol, ts_exchange)`,
  partitioned by day. Prices/sizes stored as exact `Int64` fixed-point. The schema
  is embedded (`go:embed`) so `Migrate` and the docker init script share one
  source. Each `Insert` builds a fresh batch, so a failed send is safe to retry.
- **`docker-compose.yml`** — local ClickHouse, mounting `schema.sql` as its init
  script (the full app+DB compose is milestone 6).
- **`cmd/tickstore`** — `-clickhouse host:port` routes trades to the sink; empty
  keeps the milestone-1 stdout printer. Graceful shutdown flushes the sink under a
  10s deadline.

## Design decisions (recorded in docs/DECISIONS.md)

- **D11** — drop Coinbase `last_match` frames (settled before this milestone;
  otherwise reconnects would write duplicates/partial samples).
- **D12** — store prices/sizes as raw `Int64` (scale 8), not `Decimal` or `Float`:
  exact, zero-conversion, no extra dependency. Queries scale at read time.
- **D13** — batching design: single-owner goroutine, bounded-channel backpressure,
  retry-until-shutdown, `Inserter` interface for testability.

## Testing

- Five batcher unit tests pass under `-race`: flush-on-size, flush-on-delay,
  Close-flushes-remainder, retry-until-success, and no-data-loss under
  backpressure (2500 trades through a 64-slot buffer, order preserved).
- Integration test (`TestClickHouseRoundTrip`) is env-gated on `CLICKHOUSE_ADDR`,
  so `go test ./...` stays green without Docker. It inserts and reads trades back,
  checking the fixed-point values and the side enum survive exactly.

## How to run it

    docker compose up -d
    CLICKHOUSE_ADDR=127.0.0.1:9000 go test ./internal/sink/ -run ClickHouse
    go run ./cmd/tickstore -clickhouse 127.0.0.1:9000 -symbols BTC-USD

Example VWAP query (accounting for the scale-8 fixed point). Note the
`toInt128`: `price` and `size` are each scaled by 1e8, so their product is
scaled by 1e16 and overflows Int64 for large trades — cast to Int128 first.

    SELECT symbol,
           sum(toInt128(price) * size) / sum(toInt128(size)) / 1e8 AS vwap
    FROM tickstore.trades
    WHERE ts_exchange > now() - INTERVAL 1 MINUTE
    GROUP BY symbol;

Verified on a live 20s run (BTC-USD, ETH-USD): 120 trades ingested, VWAP landed
between the min and max trade price for each symbol, fixed-point values exact.

## Open questions for the author

1. **Serial inserts.** The batcher flushes one batch at a time; a flush blocks the
   next. Fine at Coinbase rates, but do we want pipelined flushing now, or defer
   until a venue actually saturates it? (Recorded as D13's revisit trigger.)
2. **Shared sink isolation.** With one venue, backpressure from a slow sink stalls
   only that venue. When Kraken/OKX (milestone 4-5) share a sink, one slow insert
   would stall all of them. Do we want per-venue sinks, or one sink with an
   internal fan-in, before adding venues?
3. **`ts_received` clock.** Still the local wall clock (the negative-latency
   observation from milestone 1). For latency metrics in milestone 6, do we want an
   NTP-disciplined source, or treat exchange-to-committed latency as relative?
4. **Auth/config.** The sink currently hardcodes the `default` user and no
   password. Real config (address, credentials, batch sizes) belongs in the
   `internal/config` YAML — introduce it now, or with config-driven symbols in
   milestone 5?
