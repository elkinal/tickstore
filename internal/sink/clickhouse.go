package sink

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/elkinal/tickstore/internal/norm"
)

// schemaSQL is the trades DDL, run by Migrate. The same file is mounted into
// the ClickHouse container's init directory by docker-compose.
//
//go:embed schema.sql
var schemaSQL string

// Insert statements name the table for PrepareBatch; the driver fills in columns.
const (
	tradeInsertStmt = "INSERT INTO tickstore.trades"
	bookInsertStmt  = "INSERT INTO tickstore.book_updates"
)

// ClickHouseConfig points the sink at a ClickHouse server (native protocol,
// default port 9000).
type ClickHouseConfig struct {
	Addr     string // host:port
	Database string
	Username string
	Password string
}

// ClickHouse owns a connection and hands out typed inserters (Trades, Books).
type ClickHouse struct {
	conn driver.Conn
}

// OpenClickHouse connects and verifies the server is reachable.
func OpenClickHouse(ctx context.Context, cfg ClickHouseConfig) (*ClickHouse, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("clickhouse: ping: %w", err)
	}
	return &ClickHouse{conn: conn}, nil
}

// Migrate creates the database and trades table if they don't exist. It's
// idempotent, so it's safe to run on every startup.
func (c *ClickHouse) Migrate(ctx context.Context) error {
	// Strip line comments first: a comment may itself contain a ';', which
	// would otherwise split into a comment-only (empty) statement.
	for _, stmt := range strings.Split(stripLineComments(schemaSQL), ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if err := c.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("clickhouse: migrate: %w", err)
		}
	}
	return nil
}

// BackfillRollups seeds the rollup views with pre-existing history (see
// backfillRollups). It can scan hundreds of millions of book rows on first run,
// so the caller runs it in the background rather than blocking startup; it's
// idempotent and a no-op once the views cover the retained history.
func (c *ClickHouse) BackfillRollups(ctx context.Context) error { return c.backfillRollups(ctx) }

// backfillRollups fills the per-minute rollup views with existing history the
// views themselves never saw — a materialized view only aggregates rows inserted
// after it's created, so without this the longer/custom chart ranges would be
// empty for everything older than the view. It fills the gap between the raw
// retention window and whatever the view already covers, so it seeds the full
// history once and is a no-op on later startups. Aggregation memory is bounded by
// the (venue, symbol, minute) group count, not rows scanned, so it's cheap even
// over the book firehose.
func (c *ClickHouse) backfillRollups(ctx context.Context) error {
	if err := c.fillRollupGap(ctx, "tickstore.trades_1m", "tickstore.trades", "90 DAY", func(upper string) string {
		return `
			INSERT INTO tickstore.trades_1m
			SELECT venue, symbol, toStartOfMinute(ts_received) AS minute,
			       argMaxState(price, ts_received),
			       quantilesState(0.5, 0.99)(if(lat_ms >= 0 AND lat_ms < 60000, lat_ms, NULL)),
			       countState()
			FROM (
				SELECT venue, symbol, ts_received, price,
				       (toUnixTimestamp64Nano(ts_received) - toUnixTimestamp64Nano(ts_exchange)) / 1e6 AS lat_ms
				FROM tickstore.trades
				WHERE ts_received > now() - INTERVAL 90 DAY AND ts_received < ` + upper + `
			)
			GROUP BY venue, symbol, minute`
	}); err != nil {
		return err
	}
	return c.fillRollupGap(ctx, "tickstore.book_1m", "tickstore.book_updates", "30 DAY", func(upper string) string {
		return `
			INSERT INTO tickstore.book_1m
			SELECT venue, toStartOfMinute(ts_received) AS minute, count()
			FROM tickstore.book_updates
			WHERE ts_received > now() - INTERVAL 30 DAY AND ts_received < ` + upper + `
			GROUP BY venue, minute`
	})
}

// fillRollupGap aggregates raw rows older than what mvTable already holds (down
// to the ttl) into it. The upper bound is the view's current oldest minute (or
// two minutes ago if the view is empty, leaving the live view its territory), so
// re-runs find no gap and do nothing.
func (c *ClickHouse) fillRollupGap(ctx context.Context, mvTable, rawTable, ttl string, buildInsert func(upper string) string) error {
	var cnt uint64
	if err := c.conn.QueryRow(ctx, "SELECT count() FROM "+mvTable).Scan(&cnt); err != nil {
		return fmt.Errorf("clickhouse: rollup count %s: %w", mvTable, err)
	}
	upper := "now() - INTERVAL 2 MINUTE"
	if cnt > 0 {
		upper = "(SELECT min(minute) FROM " + mvTable + ")"
	}
	var gap uint64
	gapQ := "SELECT count() FROM " + rawTable + " WHERE ts_received > now() - INTERVAL " + ttl + " AND ts_received < " + upper
	if err := c.conn.QueryRow(ctx, gapQ).Scan(&gap); err != nil {
		return fmt.Errorf("clickhouse: rollup gap check %s: %w", mvTable, err)
	}
	if gap == 0 {
		return nil // the view already covers all retained history
	}
	if err := c.conn.Exec(ctx, buildInsert(upper)); err != nil {
		return fmt.Errorf("clickhouse: backfill %s: %w", mvTable, err)
	}
	return nil
}

// stripLineComments removes "-- ..." comments so the naive ';' split below is
// safe. Our schema has no "--" inside string literals, so this is sufficient.
func stripLineComments(sql string) string {
	var b strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// Close releases the connection. The per-table inserters share it, so close it
// once here after their Batchers have stopped.
func (c *ClickHouse) Close() error { return c.conn.Close() }

// Trades returns an inserter for the trades table.
func (c *ClickHouse) Trades() TradeInserter { return TradeInserter{c} }

// Books returns an inserter for the book_updates table.
func (c *ClickHouse) Books() BookInserter { return BookInserter{c} }

// TradeInserter writes batches of trades. It implements Inserter[norm.Trade].
type TradeInserter struct{ ch *ClickHouse }

// Insert writes one batch. It builds a fresh batch each call, so a failed send
// leaves nothing half-applied and the Batcher can safely retry.
func (t TradeInserter) Insert(ctx context.Context, trades []norm.Trade) error {
	batch, err := t.ch.conn.PrepareBatch(ctx, tradeInsertStmt)
	if err != nil {
		return fmt.Errorf("clickhouse: prepare trades: %w", err)
	}
	for i := range trades {
		r := &trades[i]
		if err := batch.Append(
			r.Venue, r.Symbol, r.TsExchange, r.TsReceived,
			r.Price, r.Size, r.Side.String(), r.TradeID,
		); err != nil {
			batch.Abort()
			return fmt.Errorf("clickhouse: append trade: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: send trades: %w", err)
	}
	return nil
}

// Close is a no-op: the shared connection is closed via ClickHouse.Close.
func (t TradeInserter) Close() error { return nil }

// BookInserter writes batches of book updates. It implements
// Inserter[norm.BookUpdate].
type BookInserter struct{ ch *ClickHouse }

// Insert writes one batch of book-level changes.
func (b BookInserter) Insert(ctx context.Context, ups []norm.BookUpdate) error {
	batch, err := b.ch.conn.PrepareBatch(ctx, bookInsertStmt)
	if err != nil {
		return fmt.Errorf("clickhouse: prepare book: %w", err)
	}
	for i := range ups {
		r := &ups[i]
		snap := uint8(0)
		if r.IsSnapshot {
			snap = 1
		}
		if err := batch.Append(
			r.Venue, r.Symbol, r.TsExchange, r.TsReceived,
			r.Side.String(), r.Price, r.Size, r.Seq, snap,
		); err != nil {
			batch.Abort()
			return fmt.Errorf("clickhouse: append book: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: send book: %w", err)
	}
	return nil
}

// Close is a no-op: the shared connection is closed via ClickHouse.Close.
func (b BookInserter) Close() error { return nil }
