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
	return c.backfillRollups(ctx)
}

// backfillRollups seeds the per-minute rollup views with the last ~25 hours of
// existing data the first time they're created — materialized views only see
// rows inserted after creation, so without this the 24h chart ranges would be
// empty until a full day had elapsed. It's guarded on the view being empty, so
// it runs once (on the deploy that adds the views) and is a no-op thereafter.
// Aggregation memory is bounded by the tiny number of (venue, symbol, minute)
// groups, not the rows scanned, so this is cheap despite the book volume.
func (c *ClickHouse) backfillRollups(ctx context.Context) error {
	// Backfill everything older than 2 minutes; the live view already covers the
	// last couple of minutes, so this avoids re-counting rows into those buckets.
	backfills := []struct{ table, insert string }{
		{"tickstore.trades_1m", `
			INSERT INTO tickstore.trades_1m
			SELECT venue, symbol, toStartOfMinute(ts_received) AS minute,
			       argMaxState(price, ts_received),
			       quantilesState(0.5, 0.99)(if(lat_ms >= 0 AND lat_ms < 60000, lat_ms, NULL)),
			       countState()
			FROM (
				SELECT venue, symbol, ts_received, price,
				       (toUnixTimestamp64Nano(ts_received) - toUnixTimestamp64Nano(ts_exchange)) / 1e6 AS lat_ms
				FROM tickstore.trades
				WHERE ts_received > now() - INTERVAL 25 HOUR AND ts_received < now() - INTERVAL 2 MINUTE
			)
			GROUP BY venue, symbol, minute`},
		{"tickstore.book_1m", `
			INSERT INTO tickstore.book_1m
			SELECT venue, toStartOfMinute(ts_received) AS minute, count()
			FROM tickstore.book_updates
			WHERE ts_received > now() - INTERVAL 25 HOUR AND ts_received < now() - INTERVAL 2 MINUTE
			GROUP BY venue, minute`},
	}
	for _, b := range backfills {
		// Guard on data OLDER than the live view could have produced since it was
		// created, so a freshly-created view (holding only the last few seconds of
		// live rows) still gets backfilled, but an existing one is left alone.
		var n uint64
		q := "SELECT count() FROM " + b.table + " WHERE minute < now() - INTERVAL 10 MINUTE"
		if err := c.conn.QueryRow(ctx, q).Scan(&n); err != nil {
			return fmt.Errorf("clickhouse: rollup check %s: %w", b.table, err)
		}
		if n > 0 {
			continue // already backfilled on an earlier startup
		}
		if err := c.conn.Exec(ctx, b.insert); err != nil {
			return fmt.Errorf("clickhouse: backfill %s: %w", b.table, err)
		}
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
