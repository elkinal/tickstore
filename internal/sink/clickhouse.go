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

// insertStmt names the table for PrepareBatch; the driver fills in the columns.
const insertStmt = "INSERT INTO tickstore.trades"

// ClickHouseConfig points the sink at a ClickHouse server (native protocol,
// default port 9000).
type ClickHouseConfig struct {
	Addr     string // host:port
	Database string
	Username string
	Password string
}

// ClickHouse is an Inserter that writes batches of trades to ClickHouse.
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

// Insert writes one batch. It builds a fresh batch each call, so a failed send
// leaves nothing half-applied and the Batcher can safely retry.
func (c *ClickHouse) Insert(ctx context.Context, trades []norm.Trade) error {
	batch, err := c.conn.PrepareBatch(ctx, insertStmt)
	if err != nil {
		return fmt.Errorf("clickhouse: prepare: %w", err)
	}
	for i := range trades {
		t := &trades[i]
		if err := batch.Append(
			t.Venue, t.Symbol, t.TsExchange, t.TsReceived,
			t.Price, t.Size, t.Side.String(), t.TradeID,
		); err != nil {
			batch.Abort()
			return fmt.Errorf("clickhouse: append: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: send: %w", err)
	}
	return nil
}

// Close releases the connection.
func (c *ClickHouse) Close() error { return c.conn.Close() }
