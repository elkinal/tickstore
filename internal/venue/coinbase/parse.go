package coinbase

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/elkinal/tickstore/internal/norm"
)

// wireMessage is the superset of fields tickstore reads from Coinbase
// Exchange websocket feed messages. Coinbase sends one JSON object per
// frame with a "type" discriminator.
type wireMessage struct {
	Type      string `json:"type"`
	ProductID string `json:"product_id"`
	TradeID   int64  `json:"trade_id"`
	Side      string `json:"side"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Time      string `json:"time"`
	// Message and Reason carry the details of "error" frames.
	Message string `json:"message"`
	Reason  string `json:"reason"`
}

// parseMessage decodes one raw feed frame into a normalized trade.
//
// It returns (nil, nil) for frames that are valid but not trades
// (subscription acks, heartbeats), a Trade for "match" and "last_match"
// frames, and an error for venue-reported errors or malformed input.
func parseMessage(raw []byte, tsReceived time.Time) (*norm.Trade, error) {
	var m wireMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("coinbase: bad json: %w", err)
	}
	switch m.Type {
	case "match", "last_match":
		return normalize(&m, tsReceived)
	case "subscriptions", "heartbeat":
		return nil, nil
	case "error":
		return nil, fmt.Errorf("coinbase: venue error: %s (%s)", m.Message, m.Reason)
	default:
		return nil, fmt.Errorf("coinbase: unexpected message type %q", m.Type)
	}
}

// normalize converts a match frame into a norm.Trade.
//
// Coinbase reports the MAKER side; the canonical convention is the taker
// (aggressor) side, so the side is flipped here: a maker "sell" was lifted
// by a buying taker (uptick) and vice versa.
func normalize(m *wireMessage, tsReceived time.Time) (*norm.Trade, error) {
	var side norm.Side
	switch m.Side {
	case "sell":
		side = norm.Buy
	case "buy":
		side = norm.Sell
	default:
		return nil, fmt.Errorf("coinbase: unexpected side %q", m.Side)
	}
	price, err := norm.ParseFixed(m.Price, norm.PriceDecimals)
	if err != nil {
		return nil, fmt.Errorf("coinbase: price: %w", err)
	}
	size, err := norm.ParseFixed(m.Size, norm.SizeDecimals)
	if err != nil {
		return nil, fmt.Errorf("coinbase: size: %w", err)
	}
	tsExchange, err := time.Parse(time.RFC3339Nano, m.Time)
	if err != nil {
		return nil, fmt.Errorf("coinbase: time: %w", err)
	}
	if m.ProductID == "" {
		return nil, fmt.Errorf("coinbase: match frame missing product_id")
	}
	return &norm.Trade{
		Venue:      Name,
		Symbol:     m.ProductID,
		TsExchange: tsExchange,
		TsReceived: tsReceived,
		Price:      price,
		Size:       size,
		Side:       side,
		TradeID:    strconv.FormatInt(m.TradeID, 10),
	}, nil
}
