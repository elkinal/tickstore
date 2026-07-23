# Milestone 3: Order book engine for Coinbase with gap detection + resync

Status: done. Engine built and property-tested; Coinbase L2 books reconstructed
live from the public feed.

## What was built

- **`internal/norm/book.go`** — `Level`, `BookSnapshot`, `BookUpdate` canonical
  types (fixed-point, like `Trade`).
- **`internal/book`** — the venue-agnostic engine. A `Book` holds each side as a
  `map[price]size`, applies sequenced deltas (size 0 removes a level), detects
  gaps by sequence number, and recovers via snapshot + replay of buffered
  updates. `TopOfBook` and `Depth(n)` are the validation views; gap/resync/dropped
  counters are exposed for later metrics.
- **`internal/venue/coinbase/bookparse.go`** — parses `level2_batch` snapshot and
  `l2update` frames into norm types (golden-tested).
- **`internal/venue/coinbase/book.go`** — `BookConnector` maintains a `Book` per
  symbol over the public feed, with the same reconnect/backoff shape as the trade
  connector, notifying a `BookObserver` after each frame.
- **`cmd/tickstore`** — `-book` streams books and prints throttled top-of-book.

## Design decisions (docs/DECISIONS.md)

- **D14** — engine is venue-agnostic (`norm` types only), `map` per side, snapshot
  + replay for resync. Written and tested once, reused by every venue.
- **D15** — Coinbase books use the public `level2_batch` feed. `level2/level3/full`
  now require auth (forbidden by non-goals), and `level2_batch` has no sequence
  numbers, so **live gap detection isn't possible for Coinbase** — recovery is
  snapshot-on-reconnect. Seq-based gap detection is proven by tests and will run
  live on Kraken (milestone 4).

## Testing

- 7 engine tests pass under `-race`, including two property tests over 50
  randomized trials each:
  - **shuffled replay converges** — a whole stream delivered out of order,
    buffered, then replayed after a snapshot, matches the in-order reference.
  - **post-gap resync converges** — apply a prefix, drop a run to force a gap,
    buffer the tail, resync from a snapshot, and converge to the full state;
    gap and resync counters fire.
- Coinbase level2 parser is golden-tested (snapshot + update) with table-driven
  error cases.

## Live verification

    go run ./cmd/tickstore -book -symbols BTC-USD,ETH-USD

Reconstructed both books from the live feed: BTC-USD bid/ask around 65,150 with a
1-cent spread, ETH-USD around 1,880. `bid < ask` held on every printed line (book
never crossed), `seq` climbed contiguously, `gaps=0 resyncs=0` (expected: no seq
in the feed, no reconnects in the window).

## Open questions for the author

1. **Persisting book_updates.** The SPEC's `book_updates` table (with 30-day TTL)
   isn't wired yet — the book is reconstructed in memory and printed. Persist L2
   updates to ClickHouse now, or after Kraken proves the abstraction (so the
   schema covers seq + checksum from the start)?
2. **Book + trades on one process.** `-book` and the trade path are currently
   separate modes on separate connections. Do we want one process streaming both
   (trades → sink, books → engine) before adding venues, or keep them split until
   the config/multiplex work in milestone 5?
3. **Forced periodic resync for Coinbase.** Since we can't detect gaps live on
   Coinbase, a timer-based drop-and-resnapshot would bound staleness. Worth adding,
   or rely on natural reconnects?
4. **Depth persistence vs. top-of-book.** For analytics, do we want full-depth
   snapshots at intervals, or is top-of-book + the update stream enough?
