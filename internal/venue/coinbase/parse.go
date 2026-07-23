package coinbase

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/elkinal/tickstore/internal/norm"
)

// errVenueError is for "error" frames, where Coinbase rejects something (like
// a bad product id). Unlike a malformed frame, this kills the session.
var errVenueError = errors.New("coinbase: venue error")

// wireMessage covers the fields we read from a Coinbase feed frame. Coinbase
// sends one JSON object per frame, tagged by its "type".
type wireMessage struct {
	Type      string `json:"type"`
	ProductID string `json:"product_id"`
	TradeID   int64  `json:"trade_id"`
	Side      string `json:"side"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Time      string `json:"time"`
	Message   string `json:"message"` // only set on "error" frames
	Reason    string `json:"reason"`  // only set on "error" frames
}

// parseMessage turns one raw frame into a Trade. Frames that are fine but
// aren't trades (acks, heartbeats) return (nil, nil); bad or rejected frames
// return an error.
func parseMessage(raw []byte, tsReceived time.Time) (*norm.Trade, error) {
	var m wireMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("coinbase: bad json: %w", err)
	}
	switch m.Type {
	case "match":
		return normalize(&m, tsReceived)
	case "last_match", "subscriptions", "heartbeat":
		// last_match replays the trade from before we connected. Dropping it
		// avoids duplicates and misleading partial gap samples in the sink;
		// see docs/DECISIONS.md D11.
		return nil, nil
	case "error":
		return nil, fmt.Errorf("%w: %s (%s)", errVenueError, m.Message, m.Reason)
	default:
		return nil, fmt.Errorf("coinbase: unexpected message type %q", m.Type)
	}
}

// normalize builds a Trade from a match frame. Coinbase gives the maker's
// side, but we store the taker's, so it's flipped here: a maker "sell" means a
// taker bought.
func normalize(m *wireMessage, tsReceived time.Time) (*norm.Trade, error) {
	if m.ProductID == "" {
		return nil, fmt.Errorf("coinbase: match frame missing product_id")
	}
	var side norm.Side
	switch m.Side {
	case "sell":
		side = norm.Buy
	case "buy":
		side = norm.Sell
	default:
		return nil, fmt.Errorf("coinbase: unexpected side %q", m.Side)
	}
	// ParseFixed allows zero and negatives (book updates will need them), but a
	// trade must have a positive price and size, so reject anything else here.
	price, err := norm.ParseFixed(m.Price, norm.PriceDecimals)
	if err != nil {
		return nil, fmt.Errorf("coinbase: price: %w", err)
	}
	if price <= 0 {
		return nil, fmt.Errorf("coinbase: non-positive price %q", m.Price)
	}
	size, err := norm.ParseFixed(m.Size, norm.SizeDecimals)
	if err != nil {
		return nil, fmt.Errorf("coinbase: size: %w", err)
	}
	if size <= 0 {
		return nil, fmt.Errorf("coinbase: non-positive size %q", m.Size)
	}
	tsExchange, err := time.Parse(time.RFC3339Nano, m.Time)
	if err != nil {
		return nil, fmt.Errorf("coinbase: time: %w", err)
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
