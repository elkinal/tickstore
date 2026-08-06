CREATE DATABASE IF NOT EXISTS tickstore;

-- Prices and sizes are fixed-point int64: the real value is the stored integer
-- divided by 1e8 (8 decimal places). Kept as Int64, not Float, so values are
-- exact; queries scale at read time (e.g. price / 1e8). See DECISIONS.md D1/D12.
CREATE TABLE IF NOT EXISTS tickstore.trades
(
    venue        LowCardinality(String),
    symbol       LowCardinality(String),
    ts_exchange  DateTime64(9, 'UTC'),
    ts_received  DateTime64(9, 'UTC'),
    price        Int64,
    size         Int64,
    side         Enum8('unknown' = 0, 'buy' = 1, 'sell' = 2),
    trade_id     String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts_exchange)
ORDER BY (venue, symbol, ts_exchange)
TTL toDateTime(ts_exchange) + INTERVAL 90 DAY;

-- L2 order book updates: one row per changed price level. size 0 means the
-- level was removed. is_snapshot = 1 marks levels that came from a full
-- snapshot (book reseed), 0 marks incremental deltas. seq is the per-book
-- monotonic sequence the connector assigned.
--
-- Books are the firehose (far higher volume than trades), so this table has a
-- 30-day TTL and is partitioned/expired by ts_received (always populated;
-- ts_exchange can be absent on some venues' snapshots). See DECISIONS.md.
CREATE TABLE IF NOT EXISTS tickstore.book_updates
(
    venue        LowCardinality(String),
    symbol       LowCardinality(String),
    ts_exchange  DateTime64(9, 'UTC'),
    ts_received  DateTime64(9, 'UTC'),
    side         Enum8('unknown' = 0, 'buy' = 1, 'sell' = 2),
    price        Int64,
    size         Int64,
    seq          UInt64,
    is_snapshot  UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts_received)
ORDER BY (venue, symbol, ts_received, seq)
TTL toDateTime(ts_received) + INTERVAL 30 DAY;

-- Per-minute rollups feeding the dashboard's longer chart ranges (1h/6h/24h).
-- Querying raw trades/books for a day is wasteful (the book feed alone is tens
-- of millions of rows), so these materialized views pre-aggregate on insert and
-- the chart queries merge the minute states into whatever coarser bucket a range
-- needs. The live 3-minute view still reads the raw tables. See DECISIONS.md.
--
-- trades_1m: last price (for the price + spread charts), latency percentiles,
-- and trade count, per venue+symbol+minute. Latency outside [0, 60s] is nulled
-- so it's ignored by the quantiles without dropping the trade from the count.
CREATE MATERIALIZED VIEW IF NOT EXISTS tickstore.trades_1m
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(minute)
ORDER BY (venue, symbol, minute)
TTL toDateTime(minute) + INTERVAL 90 DAY
AS
SELECT
    venue,
    symbol,
    toStartOfMinute(ts_received) AS minute,
    argMaxState(price, ts_received) AS close_state,
    quantilesState(0.5, 0.99)(
        if(lat_ms >= 0 AND lat_ms < 60000, lat_ms, NULL)
    ) AS lat_state,
    countState() AS cnt_state
FROM
(
    SELECT venue, symbol, ts_received, price,
           (toUnixTimestamp64Nano(ts_received) - toUnixTimestamp64Nano(ts_exchange)) / 1e6 AS lat_ms
    FROM tickstore.trades
)
GROUP BY venue, symbol, minute;

-- book_1m: book-update count per venue+minute, for the throughput chart's book
-- source. SummingMergeTree collapses the partial per-insert counts.
CREATE MATERIALIZED VIEW IF NOT EXISTS tickstore.book_1m
ENGINE = SummingMergeTree
PARTITION BY toYYYYMMDD(minute)
ORDER BY (venue, minute)
TTL toDateTime(minute) + INTERVAL 30 DAY
AS
SELECT venue, toStartOfMinute(ts_received) AS minute, count() AS cnt
FROM tickstore.book_updates
GROUP BY venue, minute;
