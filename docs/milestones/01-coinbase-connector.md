# Milestone 1: Coinbase connector printing normalized trades

Status: done. Verified against the live feed (BTC-USD, ETH-USD): trades
stream, one line each, clean shutdown on SIGINT.

## Design decisions

- **Fixed-point**: prices and sizes parse straight from the wire decimal
  strings into int64 via `norm.ParseFixed`, never touching float64. One
  global scale pair for now (8 decimal places for both price and size);
  the per-symbol scale registry waits for config-driven symbols
  (milestone 5). Nonzero digits beyond the scale are a hard parse error,
  not silent truncation. Representable range is ±(2^63−1); MinInt64 is
  rejected.
- **Side convention**: canonical `Trade.Side` is the taker (aggressor)
  side. Coinbase reports the maker side, so the parser flips it (maker
  sell → taker buy). Pinned by a dedicated test.
- **Venue interface**: `Run(ctx, Handler) error` with a push-style
  `Handler` (`OnTrade`). Connectors own their reconnect loop; `Run`
  returns only on ctx cancellation or unrecoverable failure. Channels vs
  callback was a coin toss; callback keeps backpressure decisions with
  the caller and avoids picking a buffer size this early.
- **Trade ID as string**: Coinbase uses ints, Kraken uses strings —
  string is the common denominator.
- **Liveness**: subscribed to `heartbeat` (1/s per product) alongside
  `matches`, with a 30s per-read timeout, so a silently dead peer is
  detected even in quiet markets.
- **Reconnect**: exponential backoff 1s→60s with full jitter
  (`rand.N(backoff)`); backoff resets after any successfully processed
  frame.
- **Error handling split**: venue-reported `error` frames (e.g. bad
  product id) end the session; a single malformed frame is logged and
  skipped. Parse errors will become a Prometheus counter in milestone 6.
- **Golden tests**: `testdata/*.input.json` → `*.golden.json`, regenerate
  with `go test ./internal/venue/coinbase -update`. Goldens render
  fixed-point values back to decimal strings plus the raw int64, so a
  reviewer can eyeball both.
- **Websocket lib**: nhooyr.io/websocket v1.8.17 (minimal API, context
  everywhere). Read limit raised to 8 MiB now so L2 snapshots don't hit
  it later.

## Observed in the live run

- First frame per product is `last_match` — a replay of the most recent
  trade, so its exchange timestamp is stale (showed as 3.4s "latency").
  It is normalized like any match; downstream consumers that care can
  dedupe by trade_id.
- Steady-state `ts_received − ts_exchange` printed ≈ −90ms: the local
  clock is behind Coinbase's. Latency measurement needs NTP-honest
  clocks or should be treated as relative, not absolute.

## Open questions for the author

1. **last_match**: keep normalizing it as a regular trade (current
   behavior, duplicates possible across reconnects), or drop it/flag it?
   The sink milestone makes this concrete: duplicates would land in
   ClickHouse.
2. **Golden inputs are hand-written**, faithful to the documented frame
   shapes but not literally recorded off the wire. Worth adding a small
   `record` mode to the connector later to capture real frames into
   testdata?
3. **Negative latency**: acceptable to publish as-is in metrics
   (documenting clock skew), or clamp at 0?
4. **Scale choice**: 8/8 decimals caps prices at ~92.2e9 — fine for
   crypto. OK to keep global until milestone 5's config, or want
   per-symbol scales earlier?
5. **Sequence numbers**: match frames carry `sequence`; it's currently
   ignored (trade streams have no gap contract). Book gap detection in
   milestone 3 will use the L2 channel's sequencing instead. Confirm.
