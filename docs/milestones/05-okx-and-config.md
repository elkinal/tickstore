# Milestone 5: Third venue (OKX) + config-driven selection

Status: done. OKX trades and books run live; a YAML config runs all three venues
concurrently into one sink.

## What was built

- **`internal/venue/okx`** — OKX v5 connector:
  - Trades: string prices, taker side (no flip), millisecond timestamps.
    Golden-tested.
  - Books: `seqId`/`prevSeqId` linkage for gap detection (contiguous only when
    `prevSeqId` == last applied `seqId`), resync on a break.
  - Client-side `ping` keeps the idle connection alive (OKX has no server
    heartbeat).
- **`internal/config`** — YAML config (ClickHouse, sink tuning, venues+symbols)
  with a `Duration` type and validation. Table-driven tests.
- **`cmd/tickstore`** — `-venue okx`; `-config file.yaml` runs every listed venue
  concurrently into one shared sink, each in its own goroutine.
- **`config.example.yaml`** — documents the format.

## Design decisions (docs/DECISIONS.md)

- **D18** — OKX book integrity is the `seqId`/`prevSeqId` linkage (the public
  checksum is always 0). This is **live** sequence-based gap detection.
- **D19** — config-driven multi-venue: one shared sink, each venue in an isolated
  goroutine so one failing can't take down the others (SPEC reliability).

## The integrity story across three venues

The three venues now cover both integrity mechanisms **live**, which is the point
of building more than one:

| Venue    | Book integrity (live)              | Proven by |
|----------|------------------------------------|-----------|
| Coinbase | none on public feed; resync on reconnect | D15 |
| Kraken   | CRC32 **checksum** every frame     | D17 |
| OKX      | **seqId/prevSeqId** gap detection  | D18 |

The engine's own seq-based detection is covered by property tests; Kraken and OKX
exercise the two live paths. No single venue was enough to prove both.

## Testing

- OKX trade parser: golden + error tests; a pong-ignored test.
- OKX book: unit test of the `prevSeqId` contiguity rule.
- Config: table-driven valid/invalid load tests.
- Live: OKX trades and book (0 gaps over sustained runs); all three venues via
  `-config` into ClickHouse at once — Coinbase, Kraken, and OKX rows in one table,
  clean shutdown.

## Open questions for the author

1. **Per-venue sink isolation.** All venues share one sink, so a slow ClickHouse
   backpressures all of them (now real with three). Split into per-venue sinks, or
   accept the coupling?
2. **Cross-venue symbol normalization.** BTC reads as `BTC-USD` / `BTC/USD` /
   `BTC-USDT` across venues. Add a symbol alias map so cross-venue queries unify,
   or leave native formats?
3. **Book persistence.** Books are still validated in memory across all three
   venues; the `book_updates` table isn't written yet. Wire it in milestone 6 with
   the metrics work, or sooner?
4. **Books in the config path.** `-config` runs trades; book mode is still the
   separate `-book` flag. Fold book streaming into the config-driven run?
