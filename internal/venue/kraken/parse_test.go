package kraken

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

var tsReceived = time.Date(2026, 7, 23, 22, 56, 48, 500_000_000, time.UTC)

type goldenTrade struct {
	Venue      string `json:"venue"`
	Symbol     string `json:"symbol"`
	TsExchange string `json:"ts_exchange"`
	Price      string `json:"price"`
	PriceRaw   int64  `json:"price_raw"`
	Size       string `json:"size"`
	Side       string `json:"side"`
	TradeID    string `json:"trade_id"`
}

// TestParseMessageGolden runs testdata/*.input.json through parseMessage.
// Regenerate with: go test ./internal/venue/kraken -update
func TestParseMessageGolden(t *testing.T) {
	inputs, _ := filepath.Glob(filepath.Join("testdata", "*.input.json"))
	if len(inputs) == 0 {
		t.Fatal("no golden inputs")
	}
	for _, inPath := range inputs {
		name := strings.TrimSuffix(filepath.Base(inPath), ".input.json")
		if strings.HasPrefix(name, "book_") {
			continue // book frames are covered by the book/checksum tests
		}
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(inPath)
			if err != nil {
				t.Fatal(err)
			}
			trades, err := parseMessage(raw, tsReceived)
			if err != nil {
				t.Fatalf("parseMessage: %v", err)
			}
			rendered := make([]goldenTrade, len(trades))
			for i, tr := range trades {
				rendered[i] = goldenTrade{
					Venue:      tr.Venue,
					Symbol:     tr.Symbol,
					TsExchange: tr.TsExchange.UTC().Format(time.RFC3339Nano),
					Price:      norm.FormatFixed(tr.Price, norm.PriceDecimals),
					PriceRaw:   tr.Price,
					Size:       norm.FormatFixed(tr.Size, norm.SizeDecimals),
					Side:       tr.Side.String(),
					TradeID:    tr.TradeID,
				}
			}
			got, _ := json.MarshalIndent(map[string]any{"trades": rendered}, "", "  ")
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
				t.Fatalf("missing golden (run -update): %v", err)
			}
			if string(got) != string(want) {
				t.Errorf("golden mismatch %s\ngot:\n%s\nwant:\n%s", name, got, want)
			}
		})
	}
}

func TestParseMessageErrors(t *testing.T) {
	tests := []struct{ name, raw, wantSub string }{
		{"bad json", `{"channel":"trade",`, "bad json"},
		{"subscribe rejected", `{"method":"subscribe","success":false,"error":"Subscription error"}`, "venue error"},
		{"bad side", `{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"hold","price":1,"qty":1,"trade_id":1,"timestamp":"2026-07-23T22:56:48Z"}]}`, "side"},
		{"bad time", `{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":1,"qty":1,"trade_id":1,"timestamp":"nope"}]}`, "time"},
		{"non-positive price", `{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":0,"qty":1,"trade_id":1,"timestamp":"2026-07-23T22:56:48Z"}]}`, "non-positive price"},
		{"missing symbol", `{"channel":"trade","type":"update","data":[{"side":"buy","price":1,"qty":1,"trade_id":1,"timestamp":"2026-07-23T22:56:48Z"}]}`, "symbol"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseMessage([]byte(tt.raw), tsReceived)
			if err == nil {
				t.Fatalf("parseMessage(%s) = nil error, want one", tt.raw)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not contain %q", err, tt.wantSub)
			}
		})
	}
}

// TestKrakenSideNotFlipped pins that Kraken's taker side maps straight through,
// unlike Coinbase's maker side.
func TestKrakenSideNotFlipped(t *testing.T) {
	raw := `{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":100,"qty":1,"trade_id":1,"timestamp":"2026-07-23T22:56:48Z"}]}`
	trades, err := parseMessage([]byte(raw), tsReceived)
	if err != nil || len(trades) != 1 {
		t.Fatalf("parse: %v, n=%d", err, len(trades))
	}
	if trades[0].Side != norm.Buy {
		t.Fatalf("side = %v, want buy (taker side, no flip)", trades[0].Side)
	}
}
