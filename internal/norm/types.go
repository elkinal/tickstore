// Package norm defines the canonical, venue-independent market data types.
//
// Venue connectors normalize their wire formats into these types as early as
// possible, so everything downstream (book engine, sink, metrics) speaks only
// norm. Prices and sizes are fixed-point int64 (see ParseFixed/FormatFixed),
// never float64, for exact equality and no accumulation drift.
package norm

import "time"

// Side is the direction of a trade, from the taker (aggressor) perspective.
type Side uint8

// Side values. SideUnknown is the zero value and must never survive parsing.
const (
	SideUnknown Side = iota
	Buy
	Sell
)

// String returns "buy", "sell", or "unknown".
func (s Side) String() string {
	switch s {
	case Buy:
		return "buy"
	case Sell:
		return "sell"
	default:
		return "unknown"
	}
}

// Trade is one normalized trade (a match between a taker and a maker).
//
// Side is the taker side: Buy means a buyer crossed the spread. Venues that
// report the maker side (Coinbase does) are flipped during normalization so
// every venue agrees on this convention.
type Trade struct {
	Venue      string    // canonical venue id, e.g. "coinbase"
	Symbol     string    // venue product id, e.g. "BTC-USD"
	TsExchange time.Time // venue-reported event time
	TsReceived time.Time // local time the frame was read, for latency
	Price      int64     // fixed-point, PriceDecimals places
	Size       int64     // fixed-point, SizeDecimals places
	Side       Side
	TradeID    string // string because venues disagree on its type
}

// PriceDecimals and SizeDecimals are the fixed-point scales for Trade.Price
// and Trade.Size. v1 uses one global pair; a per-symbol registry arrives with
// config-driven symbols (milestone 5). 8 places covers every supported venue.
const (
	PriceDecimals = 8
	SizeDecimals  = 8
)
