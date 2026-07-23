package okx

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

var tsReceived = time.Date(2026, 7, 23, 23, 0, 0, 0, time.UTC)

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
// Regenerate with: go test ./internal/venue/okx -update
func TestParseMessageGolden(t *testing.T) {
	inputs, _ := filepath.Glob(filepath.Join("testdata", "*.input.json"))
	if len(inputs) == 0 {
		t.Fatal("no golden inputs")
	}
	for _, inPath := range inputs {
		name := strings.TrimSuffix(filepath.Base(inPath), ".input.json")
		if strings.HasPrefix(name, "book") {
			continue // book frames covered by the book/checksum tests
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
		{"bad json", `{"arg":`, "bad json"},
		{"error event", `{"event":"error","code":"60012","msg":"Invalid request"}`, "venue error"},
		{"bad side", `{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"1","px":"1","sz":"1","side":"hold","ts":"1784849123233"}]}`, "side"},
		{"bad time", `{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"1","px":"1","sz":"1","side":"buy","ts":"notime"}]}`, "time"},
		{"non-positive price", `{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"1","px":"0","sz":"1","side":"buy","ts":"1784849123233"}]}`, "non-positive price"},
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

// TestPongIgnored ensures the bare "pong" heartbeat reply is a no-op.
func TestPongIgnored(t *testing.T) {
	trades, err := parseMessage([]byte("pong"), tsReceived)
	if err != nil || trades != nil {
		t.Fatalf("pong: trades=%v err=%v, want nil/nil", trades, err)
	}
}
