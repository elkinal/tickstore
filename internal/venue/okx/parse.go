package okx

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/elkinal/tickstore/internal/norm"
)

// errVenueError marks an OKX error event, which ends the session.
var errVenueError = errors.New("okx: venue error")

// envelope is the common outer shape of OKX frames. Data frames set arg/data
// (and action, for books); control frames set event.
type envelope struct {
	Event  string          `json:"event"`
	Code   string          `json:"code"`
	Msg    string          `json:"msg"`
	Arg    argInfo         `json:"arg"`
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data"`
}

type argInfo struct {
	Channel string `json:"channel"`
	InstID  string `json:"instId"`
}

// wireTrade is one trade in a "trades" frame. Prices/sizes are already strings.
type wireTrade struct {
	InstID  string `json:"instId"`
	TradeID string `json:"tradeId"`
	Px      string `json:"px"`
	Sz      string `json:"sz"`
	Side    string `json:"side"`
	Ts      string `json:"ts"` // milliseconds since epoch
}

// parseMessage decodes one frame into zero or more trades. OKX also sends the
// bare text "pong" (heartbeat reply) and subscribe acks, which return nil; an
// error event returns an error.
func parseMessage(raw []byte, tsReceived time.Time) ([]norm.Trade, error) {
	if string(raw) == "pong" {
		return nil, nil
	}
	var e envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, fmt.Errorf("okx: bad json: %w", err)
	}
	switch e.Event {
	case "error":
		return nil, fmt.Errorf("%w: %s (%s)", errVenueError, e.Msg, e.Code)
	case "subscribe", "unsubscribe", "channel-conn-count":
		return nil, nil
	}
	if e.Arg.Channel != "trades" || len(e.Data) == 0 {
		return nil, nil
	}
	return parseTrades(e.Data, tsReceived)
}

func parseTrades(data json.RawMessage, tsReceived time.Time) ([]norm.Trade, error) {
	var rows []wireTrade
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("okx: trade data: %w", err)
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

// normalizeTrade converts one wire trade. OKX reports the taker side directly,
// our canonical convention — no flip (like Kraken, unlike Coinbase; D2).
func normalizeTrade(w *wireTrade, tsReceived time.Time) (*norm.Trade, error) {
	if w.InstID == "" {
		return nil, fmt.Errorf("okx: trade missing instId")
	}
	var side norm.Side
	switch w.Side {
	case "buy":
		side = norm.Buy
	case "sell":
		side = norm.Sell
	default:
		return nil, fmt.Errorf("okx: unexpected side %q", w.Side)
	}
	price, err := norm.ParseFixed(w.Px, norm.PriceDecimals)
	if err != nil {
		return nil, fmt.Errorf("okx: price: %w", err)
	}
	if price <= 0 {
		return nil, fmt.Errorf("okx: non-positive price %q", w.Px)
	}
	size, err := norm.ParseFixed(w.Sz, norm.SizeDecimals)
	if err != nil {
		return nil, fmt.Errorf("okx: size: %w", err)
	}
	if size <= 0 {
		return nil, fmt.Errorf("okx: non-positive size %q", w.Sz)
	}
	ts, err := parseMillis(w.Ts)
	if err != nil {
		return nil, fmt.Errorf("okx: time: %w", err)
	}
	return &norm.Trade{
		Venue:      Name,
		Symbol:     w.InstID,
		TsExchange: ts,
		TsReceived: tsReceived,
		Price:      price,
		Size:       size,
		Side:       side,
		TradeID:    w.TradeID,
	}, nil
}

// parseMillis parses a millisecond epoch string into a UTC time.
func parseMillis(s string) (time.Time, error) {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms).UTC(), nil
}
