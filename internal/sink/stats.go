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

// PricePoint is one (venue, base, time, price) sample for the price-history
// chart. Base is the asset ("BTC" or "ETH"); TMs is a millisecond epoch; Price
// is the decimal price (fixed-point / 1e8).
type PricePoint struct {
	Venue string  `json:"venue"`
	Base  string  `json:"base"`
	TMs   int64   `json:"t_ms"`
	Price float64 `json:"price"`
}

// PriceHistory returns the last-trade price per venue and asset (BTC and ETH)
// over the last windowSec seconds, bucketed to bucketSec. It seeds the
// dashboard's price chart so the page loads with a full line instead of drawing
// from empty. Symbols are listed explicitly because each venue names them
// differently (BTC-USD, BTC/USD, BTC-USDT, and the ETH equivalents).
func (c *ClickHouse) PriceHistory(ctx context.Context, windowSec, bucketSec int) ([]PricePoint, error) {
	const q = `
		SELECT venue,
		       multiIf(startsWith(symbol, 'BTC'), 'BTC', startsWith(symbol, 'ETH'), 'ETH', 'OTHER') AS base,
		       toUInt64(toUnixTimestamp(toStartOfInterval(ts_received, toIntervalSecond(?)))) * 1000 AS t_ms,
		       argMax(price, ts_received) / 1e8 AS price
		FROM tickstore.trades
		WHERE symbol IN ('BTC-USD', 'BTC/USD', 'BTC-USDT', 'ETH-USD', 'ETH/USD', 'ETH-USDT')
		  AND ts_received > now() - toIntervalSecond(?)
		GROUP BY venue, base, t_ms
		ORDER BY t_ms`
	rows, err := c.conn.Query(ctx, q, bucketSec, windowSec)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: price history: %w", err)
	}
	defer rows.Close()

	var out []PricePoint
	for rows.Next() {
		var venue, base string
		var tms uint64
		var price float64
		if err := rows.Scan(&venue, &base, &tms, &price); err != nil {
			return nil, fmt.Errorf("clickhouse: scan price point: %w", err)
		}
		out = append(out, PricePoint{Venue: venue, Base: base, TMs: int64(tms), Price: price})
	}
	return out, rows.Err()
}
