package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/elkinal/tickstore/internal/live"
	"github.com/elkinal/tickstore/internal/sink"
)

// fakeStore / fakeLive stand in for ClickHouse and the live registry so the
// handlers can be exercised without a database or running engine.
type fakeStore struct{}

func (fakeStore) TableStats(context.Context) ([]sink.TableStat, error) {
	return []sink.TableStat{{Name: "trades", Rows: 10, Bytes: 100}, {Name: "book_updates", Rows: 90, Bytes: 900}}, nil
}
func (fakeStore) RecentTrades(_ context.Context, since int64, _ int) ([]sink.FeedRow, error) {
	return []sink.FeedRow{{TsNanos: 42, Time: "12:00:00", Venue: "okx", Symbol: "BTC-USDT", Side: "buy", Size: "0.5", Price: "64000"}}, nil
}
func (fakeStore) PriceHistory(_ context.Context, _, _ time.Time, _ int) ([]sink.PricePoint, error) {
	return []sink.PricePoint{{Venue: "coinbase", Base: "BTC", Quote: "USD", TMs: 1, Price: 64000}}, nil
}
func (fakeStore) LatencyHistory(_ context.Context, _, _ time.Time, _ int) ([]sink.LatencyPoint, error) {
	return []sink.LatencyPoint{{Venue: "coinbase", TMs: 1, P50: 55, P99: 97}}, nil
}
func (fakeStore) ThroughputHistory(_ context.Context, _, _ time.Time, _ int) ([]sink.RatePoint, error) {
	return []sink.RatePoint{{Venue: "coinbase", Source: "trades", TMs: 1, Rate: 14}}, nil
}

type fakeLive struct{}

func (fakeLive) Quotes() []live.Quote {
	return []live.Quote{{Venue: "okx", Symbol: "BTC-USDT", Base: "BTC", Bid: 64000, Ask: 64001, Spread: 1}}
}
func (fakeLive) Depth(venue, symbol string) ([]live.DepthRow, []live.DepthRow, bool) {
	if symbol != "BTC-USDT" {
		return nil, nil, false
	}
	return []live.DepthRow{{Price: 64000, Size: 1, Cum: 1}}, []live.DepthRow{{Price: 64001, Size: 2, Cum: 2}}, true
}
func (fakeLive) AllDepth() map[string]live.BookDepth {
	return map[string]live.BookDepth{"okx|BTC-USDT": {
		Bids: []live.DepthRow{{Price: 64000, Size: 1, Cum: 1}},
		Asks: []live.DepthRow{{Price: 64001, Size: 2, Cum: 2}},
	}}
}
func (fakeLive) TradesSince(cursor int64, limit int) ([]live.TradeRow, int64) {
	return []live.TradeRow{{Time: "12:00:00", Venue: "okx", Symbol: "BTC-USDT", Side: "buy", Size: "0.5", Price: "64000"}}, cursor + 1
}
func (fakeLive) LatestTradeID() int64 { return 0 }

func newTestHandler() *handler { return &handler{store: fakeStore{}, reg: fakeLive{}} }

// TestBookEndpoint verifies the quote grid JSON keys the frontend reads.
func TestBookEndpoint(t *testing.T) {
	w := httptest.NewRecorder()
	newTestHandler().book(w, httptest.NewRequest("GET", "/api/book", nil))
	var got struct {
		Quotes []struct {
			Base string  `json:"base"`
			Bid  float64 `json:"bid"`
			Ask  float64 `json:"ask"`
		} `json:"quotes"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Quotes) != 1 || got.Quotes[0].Base != "BTC" || got.Quotes[0].Bid != 64000 {
		t.Fatalf("unexpected quotes: %s", w.Body.String())
	}
}

// TestDepthEndpoint checks the ladder response, including the not-found path.
func TestDepthEndpoint(t *testing.T) {
	w := httptest.NewRecorder()
	newTestHandler().depth(w, httptest.NewRequest("GET", "/api/depth?venue=okx&symbol=BTC-USDT", nil))
	var got depthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.Found || len(got.Bids) != 1 || got.Bids[0].Cum != 1 || got.Asks[0].Price != 64001 {
		t.Fatalf("unexpected depth: %s", w.Body.String())
	}

	w2 := httptest.NewRecorder()
	newTestHandler().depth(w2, httptest.NewRequest("GET", "/api/depth?venue=okx&symbol=NOPE", nil))
	var miss depthResponse
	json.Unmarshal(w2.Body.Bytes(), &miss)
	if miss.Found {
		t.Fatal("expected found=false for unknown market")
	}
}

// TestTapeAndStats checks the tape and stats bodies decode with the expected keys.
func TestTapeAndStats(t *testing.T) {
	h := newTestHandler()

	wt := httptest.NewRecorder()
	h.tape(wt, httptest.NewRequest("GET", "/api/tape?ts=0", nil))
	var tape struct {
		Trades []sink.FeedRow `json:"trades"`
	}
	if err := json.Unmarshal(wt.Body.Bytes(), &tape); err != nil || len(tape.Trades) != 1 {
		t.Fatalf("tape decode: %v body=%s", err, wt.Body.String())
	}

	ws := httptest.NewRecorder()
	h.stats(ws, httptest.NewRequest("GET", "/api/stats", nil))
	var st statsResponse
	if err := json.Unmarshal(ws.Body.Bytes(), &st); err != nil {
		t.Fatalf("stats decode: %v", err)
	}
	if st.TotalRows != 100 || st.TotalBytes != 1000 {
		t.Fatalf("stats totals = %d/%d, want 100/1000", st.TotalRows, st.TotalBytes)
	}
}

// TestQueryInt checks the feed cursor parsing: a good value passes through, and
// anything missing or malformed falls back to 0 ("most recent rows").
func TestQueryInt(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want int64
	}{
		{"absent", "/api/feed", 0},
		{"valid", "/api/feed?ts=1785014617675069000", 1785014617675069000},
		{"zero", "/api/feed?ts=0", 0},
		{"negative", "/api/feed?ts=-5", 0},
		{"garbage", "/api/feed?ts=abc", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", tt.url, nil)
			if got := queryInt(r, "ts"); got != tt.want {
				t.Errorf("queryInt(%q) = %d, want %d", tt.url, got, tt.want)
			}
		})
	}
}

// TestBucketFor checks the custom-range bucket picker snaps to friendly steps
// and lands the raw/rollup split (>= 60s uses the rollups) where expected.
func TestBucketFor(t *testing.T) {
	tests := []struct{ span, want int }{
		{180, 2},       // 3-minute custom span -> sub-minute raw bucket
		{3600, 60},     // 1h -> 1-minute rollup bucket
		{7200, 120},    // 2h
		{86400, 1800},  // 24h
		{259200, 3600}, // 3 days
	}
	for _, tt := range tests {
		if got := bucketFor(tt.span); got != tt.want {
			t.Errorf("bucketFor(%d) = %d, want %d", tt.span, got, tt.want)
		}
	}
}

// TestHistoryEndpoint covers the /api/history handler for a preset range, a
// custom from/to window, and the invalid (from >= to) case.
func TestHistoryEndpoint(t *testing.T) {
	h := newTestHandler()
	decode := func(body []byte) struct {
		Price      []sink.PricePoint   `json:"price"`
		Latency    []sink.LatencyPoint `json:"latency"`
		Throughput []sink.RatePoint    `json:"throughput"`
		FromMs     int64               `json:"from_ms"`
		ToMs       int64               `json:"to_ms"`
	} {
		var r struct {
			Price      []sink.PricePoint   `json:"price"`
			Latency    []sink.LatencyPoint `json:"latency"`
			Throughput []sink.RatePoint    `json:"throughput"`
			FromMs     int64               `json:"from_ms"`
			ToMs       int64               `json:"to_ms"`
		}
		if err := json.Unmarshal(body, &r); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return r
	}

	// Preset range: all three series present, from < to.
	w := httptest.NewRecorder()
	h.history(w, httptest.NewRequest("GET", "/api/history?range=6h", nil))
	got := decode(w.Body.Bytes())
	if len(got.Price) != 1 || len(got.Latency) != 1 || len(got.Throughput) != 1 {
		t.Fatalf("preset series counts: %s", w.Body.String())
	}
	if !(got.FromMs < got.ToMs) {
		t.Fatalf("preset from/to = %d/%d", got.FromMs, got.ToMs)
	}

	// Custom window: the returned bounds echo the request (to clamped to <= now).
	from := time.Now().Add(-2 * time.Hour).UnixMilli()
	to := time.Now().Add(-1 * time.Hour).UnixMilli()
	wc := httptest.NewRecorder()
	h.history(wc, httptest.NewRequest("GET", fmt.Sprintf("/api/history?from=%d&to=%d", from, to), nil))
	c := decode(wc.Body.Bytes())
	if c.FromMs != from || c.ToMs != to {
		t.Fatalf("custom bounds = %d/%d, want %d/%d", c.FromMs, c.ToMs, from, to)
	}

	// Invalid window (from >= to) is rejected.
	wb := httptest.NewRecorder()
	h.history(wb, httptest.NewRequest("GET", fmt.Sprintf("/api/history?from=%d&to=%d", to, from), nil))
	if wb.Code != http.StatusBadRequest {
		t.Fatalf("bad range status = %d, want 400", wb.Code)
	}
}
