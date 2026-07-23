// Package norm defines the canonical, venue-independent market data types.
//
// All venue connectors normalize their wire formats into these types as
// early as possible; everything downstream (book engine, sink, metrics)
// speaks only norm types.
//
// Prices and sizes are fixed-point int64 values with an explicit number of
// decimal places (see ParseFixed/FormatFixed), never float64: fixed-point
// gives exact equality and no accumulation drift.
package norm

import "time"

// Side is the direction of an order or trade.
type Side uint8

// Side values.
const (
	// SideUnknown is the zero value; a parsed message must never carry it.
	SideUnknown Side = iota
	// Buy means the aggressor bought (uptick).
	Buy
	// Sell means the aggressor sold (downtick).
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
// Side is the taker (aggressor) side: Buy means a buyer crossed the spread.
// Venues that report the maker side (Coinbase does) are flipped during
// normalization so all venues agree on this convention.
type Trade struct {
	// Venue is the canonical venue identifier, e.g. "coinbase".
	Venue string
	// Symbol is the venue's product identifier, e.g. "BTC-USD".
	Symbol string
	// TsExchange is the venue-reported event time.
	TsExchange time.Time
	// TsReceived is the local wall-clock time the message was read off
	// the socket, for end-to-end latency measurement.
	TsReceived time.Time
	// Price is the trade price as fixed-point int64 with PriceDecimals
	// decimal places.
	Price int64
	// Size is the trade size as fixed-point int64 with SizeDecimals
	// decimal places.
	Size int64
	// Side is the taker (aggressor) side.
	Side Side
	// TradeID is the venue-assigned trade identifier, kept as a string
	// because venues disagree on its type (Coinbase: int, Kraken: string).
	TradeID string
}

// Fixed-point scales. v1 uses one global scale pair; a per-symbol scale
// registry arrives with config-driven symbols (milestone 5).
const (
	// PriceDecimals is the number of decimal places carried by Trade.Price.
	// 8 covers every tick size on the supported venues.
	PriceDecimals = 8
	// SizeDecimals is the number of decimal places carried by Trade.Size.
	// Coinbase reports sizes with up to 8 fractional digits.
	SizeDecimals = 8
)
