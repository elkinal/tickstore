# Decision log

A running record of significant design decisions: what we chose, why, what it
costs, and what would make us revisit. The milestone summaries in
`docs/milestones/` tell the story; this file is the authoritative list of the
calls behind it.

Newest decisions are appended at the bottom. Each entry is dated and states its
trade-offs and its revisit trigger honestly — a decision with no downside listed
is usually a decision not thought through.

---

## D1 — Prices and sizes are fixed-point int64, never float64
*2026-07-22*

**Decision.** Every price and size is an `int64` scaled by a fixed number of
decimal places (see `internal/norm`), parsed straight from the venue's decimal
string without ever touching `float64`.

**Why.** Floats can't represent most decimal fractions exactly, so two values
that should be equal can compare unequal, and repeated arithmetic drifts.
Money math wants exactness. Integers give exact equality and stable sums.

**Cost.** We carry an explicit scale everywhere and must reject inputs with more
precision than the scale holds.

**Revisit.** If a venue ever needs more than 18 significant digits, the scale
scheme (below) needs rework.

---

## D2 — Canonical trades carry the taker (aggressor) side
*2026-07-22*

**Decision.** `norm.Trade.Side` is always the taker's side. Coinbase reports the
maker's side, so its connector flips it during normalization.

**Why.** Venues disagree on which side they report. Picking one convention at the
normalization boundary means everything downstream is consistent and no consumer
has to know venue quirks. Taker side is the more common analytical convention
(it tells you who crossed the spread).

**Cost.** Each connector must know and document its venue's convention.

---

## D3 — Trade IDs are strings
*2026-07-22*

**Decision.** `norm.Trade.TradeID` is a `string`.

**Why.** Coinbase uses integers, Kraken uses non-numeric strings. String is the
only type that fits every venue without loss.

**Cost.** We can't assume numeric ordering of trade IDs across venues (relevant
to D11's dedup discussion).

---

## D4 — Connectors push to a Handler interface
*2026-07-22*

**Decision.** A venue connector streams trades by calling `venue.Handler.OnTrade`.
The connector has no idea what the handler does.

**Why.** Decoupling. Today the handler prints; next it's the ClickHouse sink;
later a fan-out to the book engine and the sink. The connector never changes.
Every venue speaks the same contract, so `main` can point them all at one
destination.

**Cost.** `OnTrade` runs on the connector's read-loop goroutine, so a slow
handler applies backpressure to reading. The interface doc makes this explicit;
handlers must be quick or hand off to a channel.

---

## D5 — ParseFixed rejects excess precision; range is ±(2^63 − 1)
*2026-07-23*

**Decision.** Parsing a decimal with more nonzero fractional digits than the
scale allows is an error, not a silent round-off. Junk characters are reported
as such (not misattributed to precision). The representable range is symmetric,
so plain `MinInt64` is treated as overflow.

**Why.** Silent precision loss in a market-data store is a data-integrity bug.
Better to reject and alert than to persist a subtly wrong number. Symmetric range
keeps negate/format total and the round-trip exact.

**Cost.** A venue that sends more precision than our scale would be rejected
loudly — which is the point, but it means scale must be chosen correctly.

---

## D6 — Reject non-positive price and size at normalization
*2026-07-23*

**Decision.** `ParseFixed` allows zero and negatives (the book engine will need
them), but the Coinbase trade normalizer rejects any trade with price ≤ 0 or
size ≤ 0.

**Why.** A trade with a non-positive price or size is malformed. Catching it at
ingest keeps bad data out of ClickHouse entirely, consistent with treating `norm`
as the place trade validity is enforced.

**Cost.** A hypothetical legitimate zero (none exist for trades) would be
rejected. Acceptable.

---

## D7 — FormatFixed panics on an out-of-range scale
*2026-07-23*

**Decision.** `FormatFixed` panics if given a decimal-place count outside [0, 18],
rather than returning a sentinel string.

**Why.** The scale is always a compile-time constant at call sites, so a bad value
is a programmer bug, not a runtime input. Panicking surfaces it immediately in
development instead of letting a poison string slip into output unnoticed.

**Cost.** None in practice; the input is never user-controlled.

---

## D8 — Per-venue reconnect: exponential backoff + full jitter, heartbeat-backed read timeout
*2026-07-23*

**Decision.** Each connector runs its own reconnect loop: on disconnect, wait a
random duration in [0, backoff) (full jitter), doubling backoff up to a cap, and
reset backoff once a session receives data. Reads use a timeout backed by
subscribing to the venue's heartbeat channel.

**Why.** Jittered exponential backoff is the standard way to recover without
hammering a struggling venue or reconnecting in synchronized waves. TCP can go
half-open silently, so we don't trust the socket to report death — a heartbeat
roughly every second makes "N seconds of silence = dead line" a reliable signal.

**Cost.** Backoff currently resets on *any* frame including heartbeats, so a venue
that flaps (accept → ack → drop) would reconnect fast rather than backing off.
Flagged as an open question; acceptable for now since a heartbeat does prove
liveness.

**Revisit.** If flapping is observed in the 72h run, reset backoff only after a
minimum healthy duration or after real trade data, not any frame.

---

## D9 — WebSocket library: github.com/coder/websocket
*2026-07-23*

**Decision.** Use `github.com/coder/websocket`.

**Why.** It's the maintained successor to `nhooyr.io/websocket` (same author, same
API, now under Coder); the old path is formally deprecated. Context-first API fits
our cancellation model. Migration was an import-path swap only.

**Cost.** None; identical API.

---

## D10 — One global fixed-point scale (8/8) in v1
*2026-07-23*

**Decision.** Price and size both use 8 decimal places globally for now, rather
than a per-symbol scale registry.

**Why.** 8 places covers every tick size on the supported venues, and a global
constant is far simpler than a registry. The registry earns its complexity only
when config-driven symbols arrive (milestone 5).

**Cost.** Any symbol needing >8 places, or wanting a tighter scale for storage,
isn't served until the registry exists.

**Revisit.** Milestone 5 (config-driven symbols) introduces per-symbol scales.

---

## D11 — Drop Coinbase `last_match` frames; trades table is plain MergeTree
*2026-07-23*

**Context.** Coinbase's `matches` channel emits one `last_match` per product on
subscribe: the most recent trade from before we connected. While we only printed
to stdout this was harmless. With a ClickHouse sink, these frames would persist,
and across reconnects they duplicate or misrepresent data.

**Decision.** The parser drops `last_match` frames entirely (treated as a valid
non-trade, like a heartbeat). The `trades` table is a plain `MergeTree`,
`ORDER BY (venue, symbol, ts_exchange)`, partitioned by day — exactly the SPEC
shape. No app-level dedup and no deduping table engine.

**Why.** `last_match` is not a completeness mechanism, it's a "show the last
price" convenience. On a reconnect it is one of:
- *No gap:* a trade we already stored → an exact duplicate.
- *Gap of N trades:* only the single newest of the N → a non-representative
  sample that makes a window look covered when it isn't. This silent corruption
  of analytics is worse than the data loss it pretends to prevent.

Dropping it also:
- Matches SPEC scope — non-goal "no historical backfill"; the trade stream's job
  is real-time forward. Trade streams have no sequence/gap contract, which is why
  gap detection + resync lives in the order book (milestone 3, separate L2
  channel), not here.
- Is consistent with treating `norm` as the validity boundary (D2, D6): replayed
  trades are invalid-for-our-purposes, so we reject them at ingest.
- Preserves the analytics-friendly schema. The main alternative (below) would
  force a trade_id sort key that fights the time-range queries we actually want.

**Cost.** We lose the single stale most-recent trade per product at each connect.
Negligible for a real-time engine.

**Alternatives considered.**
- *ReplacingMergeTree keyed by (venue, symbol, trade_id).* Keeps all data, dedups
  by ID — but forces `ORDER BY (venue, symbol, trade_id)`, which hurts the SPEC's
  time-range analytics (VWAP/spread over time), and its dedup is only *eventual*
  (merge-time), so queries still need `FINAL`/`argMax`. All cost, no clean
  guarantee. Also still stores the misleading gap sample.
- *Keep everything, plain MergeTree, dedup at query time.* Most faithful to the
  raw feed, but dirtiest table; every consumer must dedup forever, and the
  misleading-gap-sample problem remains. Conflicts with our ingest-time-validity
  philosophy.
- *App-level dedup by remembering the last trade_id per symbol.* Would keep the
  first `last_match` and drop replays — but robust dedup needs numeric monotonic
  IDs, which D3 says we can't assume across venues, and it adds stateful logic to
  the hot path to preserve data we've argued we don't want anyway.

**Revisit.** If trade-level gap-filling is ever required, do it with a bounded
REST backfill over the gap window (mirroring the book's snapshot resync), never
with `last_match`.

---

## D12 — Store prices/sizes as raw Int64 in ClickHouse (scale 8), not Decimal
*2026-07-23*

**Decision.** The `trades` table stores `price` and `size` as `Int64` — the same
fixed-point integers we carry in memory (real value = stored / 1e8). Not
`Decimal`, not `Float`. The scale (8) is documented in the schema; queries scale
at read time (e.g. `price / 1e8`, or `sum(price*size)/sum(size)/1e8` for VWAP).

**Why.** It's the exact same representation as `norm.Trade` (D1), so insert is a
straight copy with no conversion and no precision question. `Float` is out — it
reintroduces the drift D1 exists to avoid. `Decimal` (ClickHouse's exact
fixed-point) would be the analyst-friendly middle ground, but the `clickhouse-go`
driver wants a `shopspring/decimal` value for Decimal columns, which is a new
dependency outside the allowed list — not worth it to save a `/1e8` in queries.

**Cost.** Every query must know the scale and divide. Slightly less ergonomic for
ad-hoc analysis. Sharper: a product of two columns is scaled by 1e16 and
overflows Int64 for large trades, so aggregates like VWAP must cast to Int128
first (`sum(toInt128(price) * size) / sum(toInt128(size)) / 1e8`). A naive
`sum(price*size)` silently returns garbage (observed: a negative VWAP on the
first live run). The eventual Decimal view (below) would also hide this.

**Revisit.** When per-symbol scales arrive (D10, milestone 5), or if query
ergonomics matter enough, expose a `Decimal` view (`CAST(price AS Decimal64(8)) /
1e8`) over the raw table — keeping exact storage while giving analysts natural
columns. That needs no driver dependency since it's read-side SQL.

---

## D13 — Sink batching: single-owner goroutine, bounded-channel backpressure, retry-until-shutdown
*2026-07-23*

**Decision.** The `Batcher` (`internal/sink`) buffers trades in one goroutine and
flushes on **size (10k) or 250ms**, whichever first. `OnTrade` feeds it over a
bounded channel; when full it blocks. Failed inserts retry with jittered
exponential backoff and never drop data except when a shutdown deadline is hit.
The DB write sits behind an `Inserter` interface.

**Why.**
- *One goroutine owns the buffer* → no mutexes; trades, the flush timer, and
  shutdown are all just cases in one `select`. Simplest thing that's correct.
- *Bounded channel = backpressure.* If ClickHouse can't keep up, the channel
  fills and `OnTrade` blocks, which (per D4) propagates back through the connector
  read loop to the venue — bounded memory instead of an unbounded queue that OOMs.
- *Retry-until-shutdown.* A storage hiccup shouldn't lose trades, so inserts retry
  indefinitely while running; only a bounded shutdown flush may give up.
- *Inserter interface* keeps all of the above testable without a real ClickHouse
  (fake inserter unit tests, incl. no-loss-under-backpressure).

**Cost.**
- Inserts are serial with buffering: while one flush is in flight the run loop
  isn't draining, so throughput is one batch at a time. Fine at target rates;
  a pipelined design (accumulate the next batch during a flush) is the upgrade if
  a single venue ever saturates it.
- Backpressure on one slow sink stalls the connector feeding it. Acceptable with
  one venue; when venues share a sink, isolation may need revisiting.
- The flush timer is a free-running ticker, so it can fire on an empty buffer just
  after a size flush (a cheap no-op). Simpler than resetting a timer per flush.

**Revisit.** If one venue's volume outgrows serial inserts, pipeline flushing.
Prometheus counters (batch size, flush latency, retries) land with the metrics
package in milestone 6.

---

## D14 — Order book engine is venue-agnostic; snapshot + replay for resync
*2026-07-23*

**Decision.** `internal/book` consumes only `norm.BookSnapshot` and
`norm.BookUpdate`, never venue wire formats. A `Book` keeps each side as a
`map[price]size` (O(1) delta apply; sorted views computed on demand), applies
updates in sequence order, detects gaps by sequence number, and recovers by
reseeding from a snapshot and replaying the updates buffered since.

**Why.**
- *Venue-agnostic* means the hard, correctness-critical logic is written and
  tested once, and every venue (Coinbase now, Kraken/OKX later) reuses it. It
  also makes the engine fully testable with synthetic streams — no live feed
  needed — which is how the property tests reach real confidence.
- *map per side* keeps the hot path (applying deltas) O(1). Views (top-of-book,
  depth-N) are for validation/metrics, not the hot path, so sorting them on
  demand is fine.
- *Snapshot + replay* is the standard, correct way to recover a delta-based book:
  a snapshot gives a known-good base at a known sequence, and buffered updates
  newer than it replay to catch up. Property tests confirm both shuffled-buffer
  replay and post-gap resync converge to the in-order reference.

**Cost.** Views cost O(n log n) per call (sorting the side). Fine for validation
cadence; a real hot top-of-book would maintain a sorted structure. Books are not
concurrency-safe — one goroutine per book (matches the connector's read loop).

**Revisit.** Checksum validation (Kraken/OKX provide one) plugs in as a per-venue
hook in milestone 4; the seam is the snapshot/update boundary.

---

## D15 — Coinbase books use the public level2_batch feed; no live gap detection
*2026-07-23*

**Context.** The engine (D14) detects gaps by sequence number. Coinbase's
sequenced/order-level channels (`level2`, `level3`, `full`) now *require
authentication*, which the SPEC forbids (non-goal: no authenticated endpoints).
Probed live and confirmed. The only public book channel is `level2_batch`: a
full snapshot followed by batched `l2update` deltas — with **no sequence
numbers**.

**Decision.** Use `level2_batch`. The `BookConnector` assigns a monotonic seq
per book (snapshot, then one per change), so the engine sees a contiguous stream
and its gap detection never false-fires. Live recovery is snapshot-driven: a
reconnect yields a fresh snapshot that reseeds the book.

**Why.** It's the only public option, and it still exercises the full engine
(snapshot seeding, delta application, top-of-book, resync-on-reconnect). Seq-based
gap detection is proven by the property tests and will run *live* on Kraken
(milestone 4), which sequences its feed and adds a checksum — exactly why the SPEC
picks Kraken to prove the abstraction.

**Cost.** No live gap detection for Coinbase specifically: if the feed silently
drops an update between snapshots, we can't tell until the next reconnect. Bounded
by how often we reconnect. Also note the book's `side` reuses `norm.Side` with
bid/ask meaning (buy = bid), *not* the taker convention trades use (D2) — no flip,
because level2 "buy"/"sell" name the resting side directly.

**Workarounds evaluated (and why they don't give public gap detection).**
- *REST reconciliation.* The public REST book (`/products/{id}/book?level=2`) is
  unauthenticated and carries a global `sequence`. Tempting: fetch it periodically
  and diff against the in-memory book. It fails as gap *detection* because there's
  no shared key to align on — `level2_batch` exposes no sequence, so REST's
  sequence can't be mapped to the ws stream. Content diffing is noise: the book
  changes constantly and the REST fetch is always newer than the last ws update,
  so the two legitimately differ every time from timing skew, not from missed
  updates. It can only be used as a snapshot *source* (a resync), never detection.
- *matches sequence.* The public `matches` (trade) channel does carry sequence
  numbers, but that's the trade stream in a different sequence space — no help for
  book-update gaps.
- *Forced periodic resync.* Re-snapshot on a timer. Bounds staleness but detects
  nothing, and natural reconnects already reseed, so marginal.
- *Authenticate (read-only key).* The only real fix — unlocks the sequenced
  `level2`/`full`. Rejected: breaks the public-only / no-auth principle for a
  capability Kraken already proves live next.

Conclusion: real live gap detection is not achievable on Coinbase's public feed.
It's not an architectural gap — the engine's detection is proven by tests and runs
live on Kraken (D14 seam). Left as-is deliberately.

**Cost.** No live gap detection for Coinbase specifically: if the feed silently
drops an update between snapshots, we can't tell until the next reconnect. Bounded
by how often we reconnect. Also note the book's `side` reuses `norm.Side` with
bid/ask meaning (buy = bid), *not* the taker convention trades use (D2) — no flip,
because level2 "buy"/"sell" name the resting side directly.

**Revisit.** Only if Coinbase book fidelity becomes important on its own — then a
read-only authenticated key is the honest path, accepted as a scoped exception.
