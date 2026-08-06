package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/elkinal/tickstore/internal/norm"
)

// tables are the tickstore tables the dashboard reports on. They're listed
// explicitly so a table with no data yet still shows up (with zeros) rather than
// vanishing from the stats.
var tables = []string{"trades", "book_updates"}

// TableStat is one table's on-disk footprint: how many rows and how many bytes.
type TableStat struct {
	Name  string `json:"name"`
	Rows  uint64 `json:"rows"`
	Bytes uint64 `json:"bytes"`
}

// TableStats returns the row count and disk usage of each tickstore table,
// straight from ClickHouse's own bookkeeping (system.parts). Every known table
// is included even if empty.
func (c *ClickHouse) TableStats(ctx context.Context) ([]TableStat, error) {
	const q = `
		SELECT table, sum(rows), sum(bytes_on_disk)
		FROM system.parts
		WHERE database = 'tickstore' AND active
		GROUP BY table`
	rows, err := c.conn.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: table stats: %w", err)
	}
	defer rows.Close()

	byName := make(map[string]TableStat, len(tables))
	for rows.Next() {
		var s TableStat
		if err := rows.Scan(&s.Name, &s.Rows, &s.Bytes); err != nil {
			return nil, fmt.Errorf("clickhouse: scan table stat: %w", err)
		}
		byName[s.Name] = s
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Emit in a stable order, filling in zeros for tables with no parts yet.
	out := make([]TableStat, 0, len(tables))
	for _, name := range tables {
		s := byName[name]
		s.Name = name
		out = append(out, s)
	}
	return out, nil
}

// FeedRow is one row for the live feed, pre-formatted for display. Price and
// Size are decimal strings (the fixed-point ints rendered), Time is HH:MM:SS,
// and TsNanos is the received time as a nanosecond cursor so the client can ask
// for "only rows newer than this" on the next poll.
type FeedRow struct {
	TsNanos    int64  `json:"ts_nanos"`
	Time       string `json:"time"`
	Venue      string `json:"venue"`
	Symbol     string `json:"symbol"`
	Side       string `json:"side"`
	Size       string `json:"size"`
	Price      string `json:"price"`
	IsSnapshot bool   `json:"is_snapshot,omitempty"`
}

// RecentTrades returns up to limit trades received after sinceNanos, oldest
// first (so the client can append them to the bottom of a scrolling feed).
// sinceNanos == 0 returns the most recent trades.
func (c *ClickHouse) RecentTrades(ctx context.Context, sinceNanos int64, limit int) ([]FeedRow, error) {
	const q = `
		SELECT ts_received, venue, symbol, side, price, size
		FROM tickstore.trades
		WHERE ts_received > fromUnixTimestamp64Nano(?)
		ORDER BY ts_received DESC
		LIMIT ?`
	rows, err := c.conn.Query(ctx, q, sinceNanos, limit)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: recent trades: %w", err)
	}
	defer rows.Close()

	var out []FeedRow
	for rows.Next() {
		var (
			ts          time.Time
			venue, sym  string
			side        string
			price, size int64
		)
		if err := rows.Scan(&ts, &venue, &sym, &side, &price, &size); err != nil {
			return nil, fmt.Errorf("clickhouse: scan trade: %w", err)
		}
		out = append(out, FeedRow{
			TsNanos: ts.UnixNano(),
			Time:    ts.Format("15:04:05"),
			Venue:   venue, Symbol: sym, Side: side,
			Size:  norm.FormatFixed(size, norm.SizeDecimals),
			Price: norm.FormatFixed(price, norm.PriceDecimals),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	reverse(out) // DESC query -> flip to oldest-first for appending
	return out, nil
}

// RecentBookUpdates returns up to limit book updates received after sinceNanos,
// oldest first. sinceNanos == 0 returns the most recent updates.
func (c *ClickHouse) RecentBookUpdates(ctx context.Context, sinceNanos int64, limit int) ([]FeedRow, error) {
	const q = `
		SELECT ts_received, venue, symbol, side, price, size, is_snapshot
		FROM tickstore.book_updates
		WHERE ts_received > fromUnixTimestamp64Nano(?)
		ORDER BY ts_received DESC
		LIMIT ?`
	rows, err := c.conn.Query(ctx, q, sinceNanos, limit)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: recent book updates: %w", err)
	}
	defer rows.Close()

	var out []FeedRow
	for rows.Next() {
		var (
			ts          time.Time
			venue, sym  string
			side        string
			price, size int64
			snap        uint8
		)
		if err := rows.Scan(&ts, &venue, &sym, &side, &price, &size, &snap); err != nil {
			return nil, fmt.Errorf("clickhouse: scan book update: %w", err)
		}
		out = append(out, FeedRow{
			TsNanos: ts.UnixNano(),
			Time:    ts.Format("15:04:05"),
			Venue:   venue, Symbol: sym, Side: side,
			Size:       norm.FormatFixed(size, norm.SizeDecimals),
			Price:      norm.FormatFixed(price, norm.PriceDecimals),
			IsSnapshot: snap == 1,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	reverse(out)
	return out, nil
}

func reverse(rows []FeedRow) {
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
}

// PricePoint is one (venue, base, quote, time, price) sample for the
// price-history chart. Base is the asset ("BTC" or "ETH"); Quote is the quote
// currency ("USD" or "USDT"), which the spread chart needs so it only compares
// same-currency prices; TMs is a millisecond epoch; Price is the decimal price
// (fixed-point / 1e8).
type PricePoint struct {
	Venue string  `json:"venue"`
	Base  string  `json:"base"`
	Quote string  `json:"quote"`
	TMs   int64   `json:"t_ms"`
	Price float64 `json:"price"`
}

// rollup reports whether a chart range should read the per-minute materialized
// views instead of the raw tables. Buckets of a minute or more (the 1h/6h/24h
// ranges) use the rollups; the sub-minute live view reads raw. See DECISIONS.md.
func rollup(bucketSec int) bool { return bucketSec >= 60 }

// PriceHistory returns the last-trade price per venue and asset (BTC and ETH)
// over the last windowSec seconds, bucketed to bucketSec. It backs both the
// live price chart and its longer ranges: sub-minute buckets read raw trades,
// while minute-plus buckets merge the trades_1m rollup's argMax states (the last
// price in each coarse bucket). Symbols are listed explicitly because each venue
// names them differently (BTC-USD, BTC/USD, BTC-USDT, and the ETH equivalents).
func (c *ClickHouse) PriceHistory(ctx context.Context, windowSec, bucketSec int) ([]PricePoint, error) {
	const raw = `
		SELECT venue,
		       multiIf(startsWith(symbol, 'BTC'), 'BTC', startsWith(symbol, 'ETH'), 'ETH', 'OTHER') AS base,
		       if(endsWith(symbol, 'USDT'), 'USDT', 'USD') AS quote,
		       toUInt64(toUnixTimestamp(toStartOfInterval(ts_received, toIntervalSecond(?)))) * 1000 AS t_ms,
		       argMax(price, ts_received) / 1e8 AS price
		FROM tickstore.trades
		WHERE symbol IN ('BTC-USD', 'BTC/USD', 'BTC-USDT', 'ETH-USD', 'ETH/USD', 'ETH-USDT')
		  AND ts_received > now() - toIntervalSecond(?)
		GROUP BY venue, base, quote, t_ms
		ORDER BY t_ms`
	const mv = `
		SELECT venue,
		       multiIf(startsWith(symbol, 'BTC'), 'BTC', startsWith(symbol, 'ETH'), 'ETH', 'OTHER') AS base,
		       if(endsWith(symbol, 'USDT'), 'USDT', 'USD') AS quote,
		       toUInt64(toUnixTimestamp(toStartOfInterval(minute, toIntervalSecond(?)))) * 1000 AS t_ms,
		       argMaxMerge(close_state) / 1e8 AS price
		FROM tickstore.trades_1m
		WHERE symbol IN ('BTC-USD', 'BTC/USD', 'BTC-USDT', 'ETH-USD', 'ETH/USD', 'ETH-USDT')
		  AND minute > now() - toIntervalSecond(?)
		GROUP BY venue, base, quote, t_ms
		ORDER BY t_ms`
	q := raw
	if rollup(bucketSec) {
		q = mv
	}
	rows, err := c.conn.Query(ctx, q, bucketSec, windowSec)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: price history: %w", err)
	}
	defer rows.Close()

	var out []PricePoint
	for rows.Next() {
		var venue, base, quote string
		var tms uint64
		var price float64
		if err := rows.Scan(&venue, &base, &quote, &tms, &price); err != nil {
			return nil, fmt.Errorf("clickhouse: scan price point: %w", err)
		}
		out = append(out, PricePoint{Venue: venue, Base: base, Quote: quote, TMs: int64(tms), Price: price})
	}
	return out, rows.Err()
}

// LatencyPoint is one venue's ingest latency (exchange → stored) at one time
// bucket: the median (P50) and tail (P99) in milliseconds. TMs is a ms epoch.
type LatencyPoint struct {
	Venue string  `json:"venue"`
	TMs   int64   `json:"t_ms"`
	P50   float64 `json:"p50"`
	P99   float64 `json:"p99"`
}

// LatencyHistory returns per-venue ingest latency percentiles (the gap between
// the exchange timestamp and when tickstore received the trade) over the last
// windowSec seconds, bucketed to bucketSec. It feeds the dashboard's latency
// chart, which shows how quickly each venue's feed reaches the pipeline. The
// diff is clamped to a sane range so a venue with a missing/garbage exchange
// timestamp can't distort the scale.
func (c *ClickHouse) LatencyHistory(ctx context.Context, windowSec, bucketSec int) ([]LatencyPoint, error) {
	const raw = `
		SELECT venue,
		       toUInt64(toUnixTimestamp(toStartOfInterval(ts_received, toIntervalSecond(?)))) * 1000 AS t_ms,
		       quantiles(0.5, 0.99)(lat_ms) AS q
		FROM (
			SELECT venue, ts_received,
			       (toUnixTimestamp64Nano(ts_received) - toUnixTimestamp64Nano(ts_exchange)) / 1e6 AS lat_ms
			FROM tickstore.trades
			WHERE ts_received > now() - toIntervalSecond(?)
		)
		WHERE lat_ms >= 0 AND lat_ms < 60000
		GROUP BY venue, t_ms
		ORDER BY t_ms`
	const mv = `
		SELECT venue,
		       toUInt64(toUnixTimestamp(toStartOfInterval(minute, toIntervalSecond(?)))) * 1000 AS t_ms,
		       quantilesMerge(0.5, 0.99)(lat_state) AS q
		FROM tickstore.trades_1m
		WHERE minute > now() - toIntervalSecond(?)
		GROUP BY venue, t_ms
		ORDER BY t_ms`
	q := raw
	if rollup(bucketSec) {
		q = mv
	}
	rows, err := c.conn.Query(ctx, q, bucketSec, windowSec)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: latency history: %w", err)
	}
	defer rows.Close()

	var out []LatencyPoint
	for rows.Next() {
		var venue string
		var tms uint64
		var qs []float64
		if err := rows.Scan(&venue, &tms, &qs); err != nil {
			return nil, fmt.Errorf("clickhouse: scan latency point: %w", err)
		}
		var p50, p99 float64
		if len(qs) == 2 {
			p50, p99 = qs[0], qs[1]
		}
		out = append(out, LatencyPoint{Venue: venue, TMs: int64(tms), P50: p50, P99: p99})
	}
	return out, rows.Err()
}

// RatePoint is one venue's ingest rate for one source ("trades" or "book") at
// one time bucket, in messages per second. TMs is a millisecond epoch.
type RatePoint struct {
	Venue  string  `json:"venue"`
	Source string  `json:"source"`
	TMs    int64   `json:"t_ms"`
	Rate   float64 `json:"rate"`
}

// ThroughputHistory returns per-venue ingest throughput (rows written per
// second) for both the trade and book-update feeds over the last windowSec
// seconds, bucketed to bucketSec. It feeds the dashboard's throughput chart —
// the "how much" counterpart to the latency chart's "how fast" — and makes the
// order-book firehose (many times the trade volume) visible.
func (c *ClickHouse) ThroughputHistory(ctx context.Context, windowSec, bucketSec int) ([]RatePoint, error) {
	const raw = `
		SELECT venue, 'trades' AS source,
		       toUInt64(toUnixTimestamp(toStartOfInterval(ts_received, toIntervalSecond(?)))) * 1000 AS t_ms,
		       count() / ? AS rate
		FROM tickstore.trades
		WHERE ts_received > now() - toIntervalSecond(?)
		GROUP BY venue, t_ms
		UNION ALL
		SELECT venue, 'book' AS source,
		       toUInt64(toUnixTimestamp(toStartOfInterval(ts_received, toIntervalSecond(?)))) * 1000 AS t_ms,
		       count() / ? AS rate
		FROM tickstore.book_updates
		WHERE ts_received > now() - toIntervalSecond(?)
		GROUP BY venue, t_ms
		ORDER BY t_ms`
	const mv = `
		SELECT venue, 'trades' AS source,
		       toUInt64(toUnixTimestamp(toStartOfInterval(minute, toIntervalSecond(?)))) * 1000 AS t_ms,
		       sum(countMerge(cnt_state)) / ? AS rate
		FROM tickstore.trades_1m
		WHERE minute > now() - toIntervalSecond(?)
		GROUP BY venue, t_ms
		UNION ALL
		SELECT venue, 'book' AS source,
		       toUInt64(toUnixTimestamp(toStartOfInterval(minute, toIntervalSecond(?)))) * 1000 AS t_ms,
		       sum(cnt) / ? AS rate
		FROM tickstore.book_1m
		WHERE minute > now() - toIntervalSecond(?)
		GROUP BY venue, t_ms
		ORDER BY t_ms`
	q := raw
	if rollup(bucketSec) {
		q = mv
	}
	rows, err := c.conn.Query(ctx, q, bucketSec, bucketSec, windowSec, bucketSec, bucketSec, windowSec)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: throughput history: %w", err)
	}
	defer rows.Close()

	var out []RatePoint
	for rows.Next() {
		var venue, source string
		var tms uint64
		var rate float64
		if err := rows.Scan(&venue, &source, &tms, &rate); err != nil {
			return nil, fmt.Errorf("clickhouse: scan rate point: %w", err)
		}
		out = append(out, RatePoint{Venue: venue, Source: source, TMs: int64(tms), Rate: rate})
	}
	return out, rows.Err()
}
