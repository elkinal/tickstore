package live

import (
	"testing"

	"github.com/elkinal/tickstore/internal/book"
	"github.com/elkinal/tickstore/internal/norm"
)

func TestBaseAsset(t *testing.T) {
	tests := []struct{ symbol, want string }{
		{"BTC-USD", "BTC"},
		{"BTC/USD", "BTC"},
		{"BTC-USDT", "BTC"},
		{"ETH-USDT", "ETH"},
		{"SOLUSD", "SOLUSD"}, // no separator: returned as-is
	}
	for _, tt := range tests {
		if got := baseAsset(tt.symbol); got != tt.want {
			t.Errorf("baseAsset(%q) = %q, want %q", tt.symbol, got, tt.want)
		}
	}
}

// TestRegistryFromBook feeds a seeded book through OnBook and checks the derived
// quote (best bid/ask) and the cumulative-size ladder.
func TestRegistryFromBook(t *testing.T) {
	b := book.New("okx", "BTC-USDT")
	b.ApplySnapshot(norm.BookSnapshot{
		Venue: "okx", Symbol: "BTC-USDT", Seq: 1,
		Bids: []norm.Level{
			{Price: 64000_00000000, Size: 1_00000000},
			{Price: 63999_00000000, Size: 2_00000000},
		},
		Asks: []norm.Level{
			{Price: 64001_00000000, Size: 3_00000000},
			{Price: 64002_00000000, Size: 4_00000000},
		},
	})

	r := NewRegistry()
	r.OnBook(b)

	quotes := r.Quotes()
	if len(quotes) != 1 {
		t.Fatalf("got %d quotes, want 1", len(quotes))
	}
	q := quotes[0]
	if q.Base != "BTC" || q.Bid != 64000 || q.Ask != 64001 {
		t.Errorf("quote = base %q bid %v ask %v, want BTC/64000/64001", q.Base, q.Bid, q.Ask)
	}
	if q.Spread != 1 {
		t.Errorf("spread = %v, want 1", q.Spread)
	}

	bids, asks, ok := r.Depth("okx", "BTC-USDT")
	if !ok {
		t.Fatal("depth not found")
	}
	// bids are best-first; the second level's cumulative size is 1 + 2 = 3.
	if len(bids) != 2 || bids[1].Cum != 3 {
		t.Errorf("bid ladder cum = %+v, want second cum 3", bids)
	}
	if len(asks) != 2 || asks[0].Price != 64001 {
		t.Errorf("ask ladder = %+v, want best ask 64001", asks)
	}

	if _, _, ok := r.Depth("okx", "NOPE"); ok {
		t.Error("expected Depth for unknown market to report not found")
	}
}
