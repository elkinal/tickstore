// Package norm holds the canonical market data types. Each venue converts its
// own wire format into these, so the rest of the app never sees venue quirks.
//
// Prices and sizes are fixed-point int64, not float64, so two ticks that
// should be equal always compare equal. See ParseFixed and FormatFixed.
package norm

import "time"

// Side is the side of the aggressor (the taker) in a trade.
type Side uint8

// The possible values of Side. SideUnknown is the zero value; a parsed trade
// should never have it.
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

// Trade is one normalized trade.
//
// Side is the taker's side: Buy means a buyer crossed the spread. Some venues
// report the maker's side instead (Coinbase does), so their connector flips it
// before building the Trade.
type Trade struct {
	Venue      string    // e.g. "coinbase"
	Symbol     string    // e.g. "BTC-USD"
	TsExchange time.Time // when the venue says it happened
	TsReceived time.Time // when we read it off the socket
	Price      int64     // fixed-point, PriceDecimals places
	Size       int64     // fixed-point, SizeDecimals places
	Side       Side
	TradeID    string // a string because venues can't agree on int vs. string
}

// Decimal places for the fixed-point Price and Size fields. One global scale
// for now; per-symbol scales come with config-driven symbols (milestone 5).
const (
	PriceDecimals = 8
	SizeDecimals  = 8
)
