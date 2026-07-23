package kraken

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/elkinal/tickstore/internal/book"
	"github.com/elkinal/tickstore/internal/norm"
)

// TestBookChecksum validates the CRC32 algorithm against real captured
// snapshots: build the book from each and confirm our computed checksum equals
// the checksum Kraken sent in the same frame. Two pairs cover both price
// precisions (BTC/USD = 1, ETH/USD = 2).
func TestBookChecksum(t *testing.T) {
	tests := []struct {
		file      string
		pricePrec int
		qtyPrec   int
	}{
		{"testdata/book_snapshot.input.json", 1, 8},     // BTC/USD
		{"testdata/book_snapshot_eth.input.json", 2, 8}, // ETH/USD
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			raw, err := os.ReadFile(tt.file)
			if err != nil {
				t.Fatal(err)
			}
			var env struct {
				Data []bookDataWire `json:"data"`
			}
			if err := json.Unmarshal(raw, &env); err != nil {
				t.Fatal(err)
			}
			d := env.Data[0]

			bids, err := parseBookLevels(d.Bids)
			if err != nil {
				t.Fatal(err)
			}
			asks, err := parseBookLevels(d.Asks)
			if err != nil {
				t.Fatal(err)
			}

			b := book.New(Name, d.Symbol)
			b.ApplySnapshot(norm.BookSnapshot{Symbol: d.Symbol, Bids: bids, Asks: asks, Seq: 1})

			gotBids, gotAsks := b.Depth(10)
			got := bookChecksum(gotBids, gotAsks, tt.pricePrec, tt.qtyPrec)
			if got != d.Checksum {
				t.Fatalf("checksum = %d, want %d (Kraken's own)", got, d.Checksum)
			}
		})
	}
}
