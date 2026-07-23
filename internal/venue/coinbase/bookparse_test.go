package coinbase

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/elkinal/tickstore/internal/norm"
)

// renderedLevel and rendered* mirror parsed level2 output with fixed-point
// values formatted back to decimal strings, so goldens are readable.
type renderedLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

func renderLevels(ls []norm.Level) []renderedLevel {
	out := make([]renderedLevel, len(ls))
	for i, l := range ls {
		out[i] = renderedLevel{
			Price: norm.FormatFixed(l.Price, norm.PriceDecimals),
			Size:  norm.FormatFixed(l.Size, norm.SizeDecimals),
		}
	}
	return out
}

// TestParseLevel2Golden runs level2_*.input.json through parseLevel2. Regenerate
// with: go test ./internal/venue/coinbase -update
func TestParseLevel2Golden(t *testing.T) {
	inputs, err := filepath.Glob(filepath.Join("testdata", "level2_*.input.json"))
	if err != nil || len(inputs) == 0 {
		t.Fatalf("no level2 golden inputs: %v", err)
	}
	for _, inPath := range inputs {
		name := strings.TrimSuffix(filepath.Base(inPath), ".input.json")
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(inPath)
			if err != nil {
				t.Fatal(err)
			}
			snap, ups, err := parseLevel2(raw, tsReceived)
			if err != nil {
				t.Fatalf("parseLevel2: %v", err)
			}

			out := map[string]any{}
			if snap != nil {
				out["snapshot"] = map[string]any{
					"symbol": snap.Symbol,
					"bids":   renderLevels(snap.Bids),
					"asks":   renderLevels(snap.Asks),
				}
			}
			if ups != nil {
				rendered := make([]map[string]string, len(ups))
				for i, u := range ups {
					rendered[i] = map[string]string{
						"side":  u.Side.String(),
						"price": norm.FormatFixed(u.Price, norm.PriceDecimals),
						"size":  norm.FormatFixed(u.Size, norm.SizeDecimals),
					}
				}
				out["updates"] = rendered
			}
			got, _ := json.MarshalIndent(out, "", "  ")
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
				t.Errorf("golden mismatch for %s\ngot:\n%s\nwant:\n%s", name, got, want)
			}
		})
	}
}

// TestParseLevel2Errors covers frames that must be rejected.
func TestParseLevel2Errors(t *testing.T) {
	tests := []struct {
		name, raw, wantSub string
	}{
		{"bad json", `{"type":"snapshot",`, "bad json"},
		{"venue error", `{"type":"error","message":"nope"}`, "level2"},
		{"unknown type", `{"type":"ticker"}`, "unexpected level2 type"},
		{"bad side", `{"type":"l2update","product_id":"BTC-USD","changes":[["hold","1","1"]]}`, "side"},
		{"bad price", `{"type":"l2update","product_id":"BTC-USD","changes":[["buy","x","1"]]}`, "price"},
		{"snapshot missing product", `{"type":"snapshot","bids":[],"asks":[]}`, "product_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parseLevel2([]byte(tt.raw), tsReceived)
			if err == nil {
				t.Fatalf("parseLevel2(%s) = nil error, want one", tt.raw)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not contain %q", err, tt.wantSub)
			}
		})
	}
}
