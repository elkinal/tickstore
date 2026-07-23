# tickstore - Spec

Multi-venue market data engine in Go: normalized exchange feeds, real-time
order book reconstruction, gap detection, and a ClickHouse tick store.

## Goals
- Ingest real-time trades and L2 order book updates from multiple crypto
  venues (Coinbase, Kraken, OKX) over public WebSockets.
- Normalize venue-specific messages into one canonical schema.
- Maintain correct in-memory order books per symbol per venue, with
  sequence-gap detection and automatic resync (snapshot + replay).
- Persist normalized ticks to ClickHouse for analytical queries.
- Expose operational metrics (Prometheus) and be benchmarkable.

## Non-goals (v1)
- No trading, no order execution, no private/authenticated endpoints.
- No historical backfill beyond what resync requires.
- No UI. CLI + metrics endpoint only.
- No Binance (US geo-restrictions); revisit later.

## Architecture
One binary, several packages:

    cmd/tickstore/        main: config load, lifecycle, graceful shutdown
    internal/venue/       Venue interface + one package per exchange
        coinbase/
        kraken/
        okx/
    internal/norm/        canonical types: Trade, BookUpdate, BookSnapshot
    internal/book/        order book engine: apply updates, detect gaps,
                          trigger resync, checksum validation where the
                          venue supports it (Kraken does)
    internal/sink/        ClickHouse writer: batching, retries, backpressure
    internal/metrics/     Prometheus counters/gauges/histograms
    internal/config/      YAML config: venues, symbols, batch sizes

Data flow:
    venue websocket -> parse -> normalize -> [book engine] -> sink -> ClickHouse
                                        \-> metrics everywhere

## Canonical types (internal/norm)
- Trade: venue, symbol, ts_exchange, ts_received, price, size, side, trade_id
- BookUpdate: venue, symbol, ts_exchange, ts_received, side, price, size,
  seq, is_snapshot
- Prices/sizes as fixed-point int64 with per-symbol scale, not float64.
  (Interview-defensible decision: exact equality, no float drift.)

## Order book engine (internal/book)
- Sorted bid/ask sides; apply deltas by seq.
- Gap detection: if incoming seq != expected, mark book stale, request
  snapshot, buffer deltas, replay after snapshot. Count gaps in metrics.
- Expose top-of-book and depth-N views for validation.

## ClickHouse schema (start here, iterate)
- trades table: MergeTree, ORDER BY (venue, symbol, ts_exchange),
  partition by toYYYYMMDD(ts_exchange).
- book_updates table: same shape plus seq, is_snapshot.
- TTL: raw book_updates 30 days, trades kept indefinitely.
- Async batch inserts from sink (target: 1k-10k rows per insert, flush
  on size or 250ms, whichever first).

## Reliability requirements
- Reconnect with exponential backoff + jitter per venue.
- Graceful shutdown: flush sink before exit.
- A venue dying must not affect other venues (isolated goroutine trees).

## Observability
- Prometheus: messages/sec per venue, parse errors, gaps detected,
  resyncs, sink batch size, sink flush latency, end-to-end latency
  (ts_received minus ts_exchange, histogram).

## Milestones
1. Coinbase connector printing normalized trades to stdout.
2. ClickHouse sink with batching; trades flowing into the trades table.
3. Order book engine for Coinbase with gap detection + resync.
4. Second venue (Kraken, has book checksums) proving the abstraction.
5. Third venue (OKX). Config-driven symbol/venue selection.
6. Metrics endpoint + docker-compose (app + ClickHouse) + README with
   architecture diagram and measured numbers.

## Benchmarks to publish in README
- Sustained messages/sec ingested per venue and total.
- End-to-end p50/p99 latency (exchange ts to ClickHouse-committed).
- Gap/resync counts over a 72h run.
- ClickHouse: compression ratio achieved, example analytical queries
  (VWAP, spread over time) with timings.

## Testing
- Golden-file tests for each venue parser (recorded raw messages).
- Book engine: property-style tests (apply shuffled deltas, verify
  resync converges to snapshot state).
- One integration test with dockerized ClickHouse.
