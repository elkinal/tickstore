package coinbase

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/elkinal/tickstore/internal/norm"
)

// level2Wire is the shape of Coinbase level2_batch frames. A "snapshot" fills
// Bids/Asks; an "l2update" fills Changes ([side, price, size] triples).
type level2Wire struct {
	Type      string      `json:"type"`
	ProductID string      `json:"product_id"`
	Time      string      `json:"time"`
	Bids      [][2]string `json:"bids"`
	Asks      [][2]string `json:"asks"`
	Changes   [][3]string `json:"changes"`
}

// parseLevel2 decodes one level2_batch frame. Exactly one of the returns is
// non-nil for a data frame: a snapshot, or a slice of updates. Acks return all
// nils; bad or venue-error frames return an error.
//
// The returned updates carry no sequence number — the level2_batch feed has
// none. The BookConnector assigns a monotonic seq so the engine sees a
// contiguous stream (so its gap detection never false-fires here; real gap
// detection needs a venue that sequences its feed, like Kraken).
func parseLevel2(raw []byte, tsReceived time.Time) (*norm.BookSnapshot, []norm.BookUpdate, error) {
	var m level2Wire
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, nil, fmt.Errorf("coinbase: level2 bad json: %w", err)
	}
	switch m.Type {
	case "snapshot":
		snap, err := level2Snapshot(&m, tsReceived)
		return snap, nil, err
	case "l2update":
		ups, err := level2Updates(&m, tsReceived)
		return nil, ups, err
	case "subscriptions":
		return nil, nil, nil
	case "error":
		return nil, nil, fmt.Errorf("%w: level2 subscribe rejected", errVenueError)
	default:
		return nil, nil, fmt.Errorf("coinbase: unexpected level2 type %q", m.Type)
	}
}

func level2Snapshot(m *level2Wire, tsReceived time.Time) (*norm.BookSnapshot, error) {
	if m.ProductID == "" {
		return nil, fmt.Errorf("coinbase: level2 snapshot missing product_id")
	}
	bids, err := parseLevels(m.Bids)
	if err != nil {
		return nil, fmt.Errorf("coinbase: snapshot bids: %w", err)
	}
	asks, err := parseLevels(m.Asks)
	if err != nil {
		return nil, fmt.Errorf("coinbase: snapshot asks: %w", err)
	}
	return &norm.BookSnapshot{
		Venue:      Name,
		Symbol:     m.ProductID,
		TsReceived: tsReceived,
		Bids:       bids,
		Asks:       asks,
	}, nil
}

func level2Updates(m *level2Wire, tsReceived time.Time) ([]norm.BookUpdate, error) {
	if m.ProductID == "" {
		return nil, fmt.Errorf("coinbase: l2update missing product_id")
	}
	// The venue timestamp is per-batch; every change in the batch shares it.
	var tsExchange time.Time
	if m.Time != "" {
		t, err := time.Parse(time.RFC3339Nano, m.Time)
		if err != nil {
			return nil, fmt.Errorf("coinbase: l2update time: %w", err)
		}
		tsExchange = t
	}
	ups := make([]norm.BookUpdate, 0, len(m.Changes))
	for _, c := range m.Changes {
		// c is [side, price, size]. Here "buy"/"sell" name the book side
		// directly (bid/ask), unlike a trade's taker side — no flip.
		var side norm.Side
		switch c[0] {
		case "buy":
			side = norm.Buy
		case "sell":
			side = norm.Sell
		default:
			return nil, fmt.Errorf("coinbase: l2update side %q", c[0])
		}
		price, err := norm.ParseFixed(c[1], norm.PriceDecimals)
		if err != nil {
			return nil, fmt.Errorf("coinbase: l2update price: %w", err)
		}
		size, err := norm.ParseFixed(c[2], norm.SizeDecimals)
		if err != nil {
			return nil, fmt.Errorf("coinbase: l2update size: %w", err)
		}
		ups = append(ups, norm.BookUpdate{
			Venue:      Name,
			Symbol:     m.ProductID,
			TsExchange: tsExchange,
			TsReceived: tsReceived,
			Side:       side,
			Price:      price,
			Size:       size,
		})
	}
	return ups, nil
}

// parseLevels converts [price, size] string pairs into fixed-point levels.
func parseLevels(pairs [][2]string) ([]norm.Level, error) {
	levels := make([]norm.Level, 0, len(pairs))
	for _, p := range pairs {
		price, err := norm.ParseFixed(p[0], norm.PriceDecimals)
		if err != nil {
			return nil, fmt.Errorf("price %q: %w", p[0], err)
		}
		size, err := norm.ParseFixed(p[1], norm.SizeDecimals)
		if err != nil {
			return nil, fmt.Errorf("size %q: %w", p[1], err)
		}
		levels = append(levels, norm.Level{Price: price, Size: size})
	}
	return levels, nil
}
