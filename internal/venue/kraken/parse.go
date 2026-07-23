package kraken

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/elkinal/tickstore/internal/norm"
)

// errVenueError marks a rejected subscription (Kraken replied success:false),
// which ends the session rather than being skipped like a bad frame.
var errVenueError = errors.New("kraken: venue error")

// envelope is the common outer shape of Kraken v2 frames. Channel data frames
// set Channel/Type/Data; a response to a method (e.g. subscribe) sets Method.
type envelope struct {
	Channel string          `json:"channel"`
	Type    string          `json:"type"`
	Method  string          `json:"method"`
	Success *bool           `json:"success"`
	Error   string          `json:"error"`
	Data    json.RawMessage `json:"data"`
}

// wireTrade is one trade in a "trade" channel frame. Price and Qty are
// json.Number so the exact decimal text reaches ParseFixed without a float.
type wireTrade struct {
	Symbol    string      `json:"symbol"`
	Side      string      `json:"side"`
	Price     json.Number `json:"price"`
	Qty       json.Number `json:"qty"`
	TradeID   int64       `json:"trade_id"`
	Timestamp string      `json:"timestamp"`
}

// parseMessage decodes one frame into zero or more trades. Non-trade frames
// (acks, heartbeats, status, trade snapshots) return nil; a rejected
// subscription or malformed input returns an error.
func parseMessage(raw []byte, tsReceived time.Time) ([]norm.Trade, error) {
	var e envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, fmt.Errorf("kraken: bad json: %w", err)
	}
	// A response to a method call (e.g. our subscribe).
	if e.Method != "" {
		if e.Success != nil && !*e.Success {
			return nil, fmt.Errorf("%w: %s", errVenueError, e.Error)
		}
		return nil, nil
	}
	switch e.Channel {
	case "trade":
		if e.Type == "snapshot" {
			// Recent-trades replay on subscribe; drop it, like Coinbase
			// last_match (DECISIONS.md D11).
			return nil, nil
		}
		return parseTrades(e.Data, tsReceived)
	default:
		// heartbeat, status, and any channel we didn't subscribe to.
		return nil, nil
	}
}

func parseTrades(data json.RawMessage, tsReceived time.Time) ([]norm.Trade, error) {
	var rows []wireTrade
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("kraken: trade data: %w", err)
	}
	trades := make([]norm.Trade, 0, len(rows))
	for i := range rows {
		t, err := normalizeTrade(&rows[i], tsReceived)
		if err != nil {
			return nil, err
		}
		trades = append(trades, *t)
	}
	return trades, nil
}

// normalizeTrade converts one wire trade to a norm.Trade. Kraken reports the
// taker (aggressor) side directly, which is our canonical convention — no flip
// (unlike Coinbase, which reports the maker side; see DECISIONS.md D2).
func normalizeTrade(w *wireTrade, tsReceived time.Time) (*norm.Trade, error) {
	if w.Symbol == "" {
		return nil, fmt.Errorf("kraken: trade missing symbol")
	}
	var side norm.Side
	switch w.Side {
	case "buy":
		side = norm.Buy
	case "sell":
		side = norm.Sell
	default:
		return nil, fmt.Errorf("kraken: unexpected side %q", w.Side)
	}
	price, err := norm.ParseFixed(w.Price.String(), norm.PriceDecimals)
	if err != nil {
		return nil, fmt.Errorf("kraken: price: %w", err)
	}
	if price <= 0 {
		return nil, fmt.Errorf("kraken: non-positive price %q", w.Price)
	}
	size, err := norm.ParseFixed(w.Qty.String(), norm.SizeDecimals)
	if err != nil {
		return nil, fmt.Errorf("kraken: size: %w", err)
	}
	if size <= 0 {
		return nil, fmt.Errorf("kraken: non-positive size %q", w.Qty)
	}
	ts, err := time.Parse(time.RFC3339Nano, w.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("kraken: time: %w", err)
	}
	return &norm.Trade{
		Venue:      Name,
		Symbol:     w.Symbol,
		TsExchange: ts,
		TsReceived: tsReceived,
		Price:      price,
		Size:       size,
		Side:       side,
		TradeID:    strconv.FormatInt(w.TradeID, 10),
	}, nil
}
