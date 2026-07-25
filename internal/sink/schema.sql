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
ORDER BY (venue, symbol, ts_exchange);

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
