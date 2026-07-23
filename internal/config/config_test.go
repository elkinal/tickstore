package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadValid(t *testing.T) {
	path := writeTemp(t, `
clickhouse:
  addr: 127.0.0.1:9000
  database: tickstore
  username: tickstore
  password: tickstore
sink:
  max_rows: 5000
  max_delay: 250ms
  buffer: 10000
venues:
  - name: coinbase
    symbols: [BTC-USD, ETH-USD]
  - name: kraken
    symbols: [BTC/USD]
`)
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.ClickHouse.Addr != "127.0.0.1:9000" || c.Sink.MaxRows != 5000 {
		t.Fatalf("unexpected values: %+v", c)
	}
	if time.Duration(c.Sink.MaxDelay) != 250*time.Millisecond {
		t.Fatalf("max_delay = %v, want 250ms", time.Duration(c.Sink.MaxDelay))
	}
	if len(c.Venues) != 2 || c.Venues[1].Name != "kraken" {
		t.Fatalf("venues wrong: %+v", c.Venues)
	}
}

func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name, body, wantSub string
	}{
		{"no venues", "clickhouse:\n  addr: x\n", "no venues"},
		{"venue no name", "venues:\n  - symbols: [BTC-USD]\n", "no name"},
		{"venue no symbols", "venues:\n  - name: coinbase\n", "no symbols"},
		{"duplicate venue", "venues:\n  - name: okx\n    symbols: [BTC-USDT]\n  - name: okx\n    symbols: [ETH-USDT]\n", "twice"},
		{"bad duration", "sink:\n  max_delay: nonsense\nvenues:\n  - name: okx\n    symbols: [BTC-USDT]\n", "invalid duration"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tt.body))
			if err == nil {
				t.Fatal("Load = nil error, want one")
			}
			if got := err.Error(); !strings.Contains(got, tt.wantSub) {
				t.Errorf("error %q does not contain %q", got, tt.wantSub)
			}
		})
	}
}
