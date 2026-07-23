package sink

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/elkinal/tickstore/internal/norm"
)

// TestClickHouseRoundTrip inserts trades and reads them back. It needs a live
// ClickHouse, so it's skipped unless CLICKHOUSE_ADDR is set:
//
//	docker compose up -d
//	CLICKHOUSE_ADDR=127.0.0.1:9000 go test ./internal/sink/ -run ClickHouse
func TestClickHouseRoundTrip(t *testing.T) {
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("set CLICKHOUSE_ADDR to run the ClickHouse integration test")
	}
	ctx := context.Background()

	ch, err := OpenClickHouse(ctx, ClickHouseConfig{
		Addr: addr, Database: "tickstore", Username: "tickstore", Password: "tickstore",
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer ch.Close()
	if err := ch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Unique marker so this run's rows don't collide with any other's, and clean
	// them up afterward so the test doesn't pollute the shared table.
	marker := fmt.Sprintf("itest-%d", time.Now().UnixNano())
	defer ch.conn.Exec(context.Background(),
		"ALTER TABLE tickstore.trades DELETE WHERE venue = ? SETTINGS mutations_sync = 1", marker)
	now := time.Now().UTC().Truncate(time.Nanosecond)
	in := []norm.Trade{
		{Venue: marker, Symbol: "BTC-USD", TsExchange: now, TsReceived: now,
			Price: 5_012_345_000_000, Size: 542_000, Side: norm.Buy, TradeID: "a1"},
		{Venue: marker, Symbol: "ETH-USD", TsExchange: now, TsReceived: now,
			Price: 352_107_000_000, Size: 125_000_000, Side: norm.Sell, TradeID: "a2"},
	}
	if err := ch.Insert(ctx, in); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var count uint64
	if err := ch.conn.QueryRow(ctx,
		"SELECT count() FROM tickstore.trades WHERE venue = ?", marker).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != uint64(len(in)) {
		t.Fatalf("row count = %d, want %d", count, len(in))
	}

	// Verify the fixed-point values and enum survive the round trip exactly.
	var price, size int64
	var side string
	if err := ch.conn.QueryRow(ctx,
		"SELECT price, size, side FROM tickstore.trades WHERE venue = ? AND trade_id = 'a1'",
		marker).Scan(&price, &size, &side); err != nil {
		t.Fatalf("select: %v", err)
	}
	if price != 5_012_345_000_000 || size != 542_000 || side != "buy" {
		t.Fatalf("round trip mismatch: price=%d size=%d side=%q", price, size, side)
	}
}
