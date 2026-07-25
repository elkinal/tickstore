// Package live keeps the current state of the engine in memory — the latest
// order book (top-of-book and a few levels of depth) and last trade for each
// venue+symbol — so the dashboard can render always-current views without
// replaying the tick history. State is O(venues × symbols), independent of feed
// volume, which is what lets it stay readable under the book firehose.
package live

import (
	"sort"
	"sync"
	"time"

	"github.com/elkinal/tickstore/internal/book"
	"github.com/elkinal/tickstore/internal/norm"
)

// depthLevels is how many levels per side the registry keeps for the ladder.
const depthLevels = 12

// Registry holds the latest book and trade state per market. Its OnBook method
// satisfies each venue package's BookObserver, and OnTrade is called from the
// trade handler; both run on connector read loops, so they only take a brief
// write lock and copy small slices.
type Registry struct {
	mu     sync.RWMutex
	books  map[string]*bookState
	trades map[string]tradeState
}

type bookState struct {
	venue, symbol string
	bids, asks    []norm.Level // top depthLevels, best-first
	seq           uint64
	gaps, resyncs uint64
	synced        bool
	updated       time.Time
}

type tradeState struct {
	price   int64
	side    norm.Side
	size    int64
	updated time.Time
}

// NewRegistry returns an empty registry ready for observers to write to.
func NewRegistry() *Registry {
	return &Registry{
		books:  make(map[string]*bookState),
		trades: make(map[string]tradeState),
	}
}

func key(venue, symbol string) string { return venue + "|" + symbol }

// OnBook records the current state of a book. It implements the venue packages'
// BookObserver interface and runs on the connector's read loop.
func (r *Registry) OnBook(b *book.Book) {
	bids, asks := b.Depth(depthLevels)
	gaps, resyncs, _ := b.Stats()
	st := &bookState{
		venue:   b.Venue(),
		symbol:  b.Symbol(),
		bids:    bids,
		asks:    asks,
		seq:     b.LastSeq(),
		gaps:    gaps,
		resyncs: resyncs,
		synced:  b.Synced(),
		updated: time.Now(),
	}
	r.mu.Lock()
	r.books[key(st.venue, st.symbol)] = st
	r.mu.Unlock()
}

// OnTrade records the last trade for a market.
func (r *Registry) OnTrade(t norm.Trade) {
	r.mu.Lock()
	r.trades[key(t.Venue, t.Symbol)] = tradeState{
		price: t.Price, side: t.Side, size: t.Size, updated: t.TsReceived,
	}
	r.mu.Unlock()
}

// Quote is one market's current best bid/ask plus its last trade, formatted for
// the dashboard. Prices and sizes are decimal (fixed-point rendered as float)
// for display and comparison in the browser.
type Quote struct {
	Venue     string  `json:"venue"`
	Symbol    string  `json:"symbol"`
	Base      string  `json:"base"`  // BTC, ETH
	Quote     string  `json:"quote"` // USD, USDT — with Base, the cross-venue key
	Bid       float64 `json:"bid"`
	Ask       float64 `json:"ask"`
	BidSize   float64 `json:"bid_size"`
	AskSize   float64 `json:"ask_size"`
	Spread    float64 `json:"spread"`
	Mid       float64 `json:"mid"`
	Last      float64 `json:"last"`
	LastSide  string  `json:"last_side"`
	Seq       uint64  `json:"seq"`
	Gaps      uint64  `json:"gaps"`
	Resyncs   uint64  `json:"resyncs"`
	Synced    bool    `json:"synced"`
	AgeMillis int64   `json:"age_ms"` // since last book update
}

// Quotes returns the current quote for every known market, sorted by base asset
// then venue so the grid is stable across polls.
func (r *Registry) Quotes() []Quote {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]Quote, 0, len(r.books))
	now := time.Now()
	for k, b := range r.books {
		base, quote := splitSymbol(b.symbol)
		q := Quote{
			Venue: b.venue, Symbol: b.symbol, Base: base, Quote: quote,
			Seq: b.seq, Gaps: b.gaps, Resyncs: b.resyncs, Synced: b.synced,
			AgeMillis: now.Sub(b.updated).Milliseconds(),
		}
		if len(b.bids) > 0 {
			q.Bid = px(b.bids[0].Price)
			q.BidSize = sz(b.bids[0].Size)
		}
		if len(b.asks) > 0 {
			q.Ask = px(b.asks[0].Price)
			q.AskSize = sz(b.asks[0].Size)
		}
		if q.Bid > 0 && q.Ask > 0 {
			q.Spread = q.Ask - q.Bid
			q.Mid = (q.Ask + q.Bid) / 2
		}
		if t, ok := r.trades[k]; ok {
			q.Last = px(t.price)
			q.LastSide = t.side.String()
		}
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Base != out[j].Base {
			return out[i].Base < out[j].Base
		}
		return out[i].Venue < out[j].Venue
	})
	return out
}

// DepthRow is one ladder level with the running cumulative size from the top of
// book outward, used to draw the depth bars.
type DepthRow struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
	Cum   float64 `json:"cum"`
}

// Depth returns the ladder (up to depthLevels per side) for one market. ok is
// false if that market isn't known yet.
func (r *Registry) Depth(venue, symbol string) (bids, asks []DepthRow, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.books[key(venue, symbol)]
	if !ok {
		return nil, nil, false
	}
	return ladder(b.bids), ladder(b.asks), true
}

// ladder converts levels to display rows with a cumulative-size running total.
func ladder(levels []norm.Level) []DepthRow {
	rows := make([]DepthRow, 0, len(levels))
	var cum int64
	for _, l := range levels {
		cum += l.Size
		rows = append(rows, DepthRow{Price: px(l.Price), Size: sz(l.Size), Cum: sz(cum)})
	}
	return rows
}

// splitSymbol separates a venue symbol into base and quote currency at the
// separator: BTC-USD -> (BTC, USD), BTC/USD -> (BTC, USD), BTC-USDT -> (BTC,
// USDT). Grouping the grid by (base, quote) keeps USD and USDT markets apart, so
// a cross-venue best bid/ask never mixes quote currencies (which would show a
// bogus negative spread from the USDT basis). A symbol with no separator returns
// an empty quote.
func splitSymbol(symbol string) (base, quote string) {
	for i, c := range symbol {
		if c == '-' || c == '/' {
			return symbol[:i], symbol[i+1:]
		}
	}
	return symbol, ""
}

// px and sz render fixed-point ints as decimals for display only; storage stays
// integer everywhere else.
func px(v int64) float64 { return float64(v) / 1e8 }
func sz(v int64) float64 { return float64(v) / 1e8 }
