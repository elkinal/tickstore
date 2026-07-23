# Milestone 4: Second venue (Kraken) with book checksums

Status: done. Kraken trades and order books both run live; the CRC32 checksum is
validated on every book frame with zero mismatches on sustained runs.

## What was built

- **`internal/venue/kraken/parse.go`** — Kraken v2 trade parser. Prices/qtys
  arrive as JSON numbers, decoded via `json.Number` and `ParseFixed`d so no float
  is involved. Taker side maps straight through (no maker flip). Golden-tested.
- **`internal/venue/kraken/kraken.go`** — trade `Connector`, using the shared
  `venue.RunWithReconnect` helper (extracted so this is not a third copy of the
  backoff loop).
- **`internal/venue/kraken/checksum.go`** — Kraken's CRC32 book checksum, computed
  from our int64 values with no floats. Verified offline against real BTC/USD
  (price precision 1) and ETH/USD (precision 2) snapshots.
- **`internal/venue/kraken/book.go`** — `BookConnector`: subscribes to `book` and
  `instrument` (for per-pair precision), maintains a `Book` per symbol, and
  validates every snapshot/update against Kraken's checksum. On mismatch it
  resyncs by reconnecting.
- **`internal/book/book.go`** — added `Book.Trim(n)` so depth-limited feeds mirror
  the venue's window exactly.
- **`cmd/tickstore`** — `-venue coinbase|kraken` selects the trade connector and
  the book feed.

## Design decisions (docs/DECISIONS.md)

- **D16** — `json.Number` keeps Kraken's numeric prices exact; taker side needs no
  flip (contrast Coinbase, D2).
- **D17** — Kraken v2 books carry no sequence number; the CRC32 checksum is the
  integrity/gap mechanism. Compute it from int64 (no floats), validate every
  frame, resync on mismatch, and `Trim` to the subscribed depth so window-exit
  levels can't corrupt the checksum.

## What Kraken proves

This is the milestone the SPEC designed around ("second venue proving the
abstraction"):

- The venue abstraction generalizes: Kraken trades flow into the same `trades`
  table as Coinbase, unchanged downstream. Verified live (both venues' VWAPs
  sane, side by side).
- The book engine is genuinely venue-agnostic: the same `internal/book` drives
  Coinbase's seqless feed and Kraken's checksum feed.
- **Live integrity validation** that Coinbase's public feed couldn't provide: the
  CRC32 checksum confirms every reconstructed Kraken book byte-for-byte against
  the exchange, and a mismatch triggers a resync.

## Testing

- Trade parser: golden + error tests; a test pinning the no-flip side convention.
- Checksum: validated offline against real captured snapshots for both price
  precisions (the algorithm is exact, not "close enough").
- `Book.Trim`: unit test that it keeps the best N per side.
- Live: `-book -venue kraken` on BTC/USD + ETH/USD — **0 checksum mismatches**
  over sustained runs (~1000 updates per symbol), `bid < ask` throughout.

## The bug worth remembering

Checksums passed offline for both pairs but ETH occasionally mismatched live while
BTC never did. The offline tests isolated it: not a precision/formatting bug (both
precisions verified), but the depth-limited book accumulating stale levels that
left Kraken's window. `Trim(depth)` after each frame fixed it — 0 mismatches
after. Lesson: a passing offline vector plus a live mismatch localizes the fault
to state maintenance, not the algorithm.

## Open questions for the author

1. **Persisting book_updates.** Books are still validated in memory, not written
   to the `book_updates` table. Now that Kraken gives us seq-equivalent integrity,
   wire book persistence (with checksum/validity columns), or wait for OKX?
2. **Resync granularity.** A checksum mismatch reconnects the whole session (all
   symbols). A per-symbol resubscribe would be tidier. Worth it now, or fine given
   how rare mismatches are?
3. **Symbol naming.** Coinbase uses `BTC-USD`, Kraken uses `BTC/USD`. Cross-venue
   symbol normalization is a config concern — introduce it with milestone 5's
   config, or sooner?
4. **Migrate Coinbase to `RunWithReconnect`.** The helper is used by Kraken; the
   Coinbase connectors still have their own copies. Unify now, or leave the
   working code until a broader refactor?
