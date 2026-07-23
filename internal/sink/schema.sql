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
