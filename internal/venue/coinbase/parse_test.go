package coinbase

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elkinal/tickstore/internal/norm"
)

var update = flag.Bool("update", false, "rewrite golden files")

// tsReceived is a fixed receive timestamp injected into every golden run so
// outputs are deterministic.
var tsReceived = time.Date(2026, 7, 22, 14, 3, 11, 500_000_000, time.UTC)

// goldenTrade is the human-readable rendering of a norm.Trade used in
// golden files: fixed-point values are formatted back to decimal strings
// so a reviewer can eyeball them against the raw input.
type goldenTrade struct {
	Venue      string `json:"venue"`
	Symbol     string `json:"symbol"`
	TsExchange string `json:"ts_exchange"`
	TsReceived string `json:"ts_received"`
	Price      string `json:"price"`
	PriceRaw   int64  `json:"price_raw"`
	Size       string `json:"size"`
	SizeRaw    int64  `json:"size_raw"`
	Side       string `json:"side"`
	TradeID    string `json:"trade_id"`
}

// TestParseMessageGolden runs every testdata/*.input.json frame through
// parseMessage and compares the result against the matching .golden.json.
// Run `go test ./internal/venue/coinbase -update` to regenerate goldens.
func TestParseMessageGolden(t *testing.T) {
	inputs, err := filepath.Glob(filepath.Join("testdata", "*.input.json"))
	if err != nil || len(inputs) == 0 {
		t.Fatalf("no golden inputs found: %v", err)
	}
	for _, inPath := range inputs {
		name := strings.TrimSuffix(filepath.Base(inPath), ".input.json")
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(inPath)
			if err != nil {
				t.Fatal(err)
			}
			trade, err := parseMessage(raw, tsReceived)
			if err != nil {
				t.Fatalf("parseMessage: %v", err)
			}
			var rendered any
			if trade == nil {
				rendered = map[string]any{"trade": nil}
			} else {
				rendered = goldenTrade{
					Venue:      trade.Venue,
					Symbol:     trade.Symbol,
					TsExchange: trade.TsExchange.UTC().Format(time.RFC3339Nano),
					TsReceived: trade.TsReceived.UTC().Format(time.RFC3339Nano),
					Price:      norm.FormatFixed(trade.Price, norm.PriceDecimals),
					PriceRaw:   trade.Price,
					Size:       norm.FormatFixed(trade.Size, norm.SizeDecimals),
					SizeRaw:    trade.Size,
					Side:       trade.Side.String(),
					TradeID:    trade.TradeID,
				}
			}
			got, err := json.MarshalIndent(rendered, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')

			goldenPath := filepath.Join("testdata", name+".golden.json")
			if *update {
				if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("missing golden file (run with -update): %v", err)
			}
			if string(got) != string(want) {
				t.Errorf("golden mismatch for %s\ngot:\n%s\nwant:\n%s", name, got, want)
			}
		})
	}
}

// TestParseMessageErrors covers frames that must be rejected.
func TestParseMessageErrors(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantSub string
	}{
		{
			name:    "malformed json",
			raw:     `{"type":"match",`,
			wantSub: "bad json",
		},
		{
			name:    "venue error frame",
			raw:     `{"type":"error","message":"Failed to subscribe","reason":"BTC-USDX is not a valid product"}`,
			wantSub: "venue error",
		},
		{
			name:    "unknown type",
			raw:     `{"type":"ticker","product_id":"BTC-USD"}`,
			wantSub: "unexpected message type",
		},
		{
			name:    "bad side",
			raw:     `{"type":"match","trade_id":1,"side":"hold","size":"1","price":"1","product_id":"BTC-USD","time":"2026-07-22T14:03:11Z"}`,
			wantSub: "unexpected side",
		},
		{
			name:    "bad price",
			raw:     `{"type":"match","trade_id":1,"side":"buy","size":"1","price":"1.2.3","product_id":"BTC-USD","time":"2026-07-22T14:03:11Z"}`,
			wantSub: "price",
		},
		{
			name:    "bad time",
			raw:     `{"type":"match","trade_id":1,"side":"buy","size":"1","price":"1","product_id":"BTC-USD","time":"yesterday"}`,
			wantSub: "time",
		},
		{
			name:    "missing product",
			raw:     `{"type":"match","trade_id":1,"side":"buy","size":"1","price":"1","time":"2026-07-22T14:03:11Z"}`,
			wantSub: "product_id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trade, err := parseMessage([]byte(tt.raw), tsReceived)
			if err == nil {
				t.Fatalf("parseMessage(%s) = %+v, want error", tt.raw, trade)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not contain %q", err, tt.wantSub)
			}
		})
	}
}

// TestNormalizeSideConvention pins the maker->taker side flip: Coinbase
// reports the maker side, canonical trades carry the aggressor side.
func TestNormalizeSideConvention(t *testing.T) {
	tests := []struct {
		makerSide string
		want      norm.Side
	}{
		{makerSide: "sell", want: norm.Buy}, // maker sold, taker bought: uptick
		{makerSide: "buy", want: norm.Sell}, // maker bought, taker sold: downtick
	}
	for _, tt := range tests {
		t.Run(tt.makerSide, func(t *testing.T) {
			m := &wireMessage{
				Type: "match", ProductID: "BTC-USD", TradeID: 1,
				Side: tt.makerSide, Price: "100", Size: "1",
				Time: "2026-07-22T14:03:11Z",
			}
			trade, err := normalize(m, tsReceived)
			if err != nil {
				t.Fatal(err)
			}
			if trade.Side != tt.want {
				t.Errorf("maker side %q -> %v, want %v", tt.makerSide, trade.Side, tt.want)
			}
		})
	}
}
