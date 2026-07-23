package kraken

import (
	"encoding/json"
	"fmt"

	"github.com/elkinal/tickstore/internal/norm"
)

// bookLevelWire is one {price, qty} level. json.Number keeps the exact decimal
// text so ParseFixed never sees a float.
type bookLevelWire struct {
	Price json.Number `json:"price"`
	Qty   json.Number `json:"qty"`
}

// bookDataWire is the payload of a book snapshot or update frame.
type bookDataWire struct {
	Symbol    string          `json:"symbol"`
	Bids      []bookLevelWire `json:"bids"`
	Asks      []bookLevelWire `json:"asks"`
	Checksum  uint32          `json:"checksum"`
	Timestamp string          `json:"timestamp"`
}

// parseBookLevels converts wire levels to fixed-point norm levels.
func parseBookLevels(ws []bookLevelWire) ([]norm.Level, error) {
	out := make([]norm.Level, len(ws))
	for i, w := range ws {
		price, err := norm.ParseFixed(w.Price.String(), norm.PriceDecimals)
		if err != nil {
			return nil, fmt.Errorf("price %q: %w", w.Price, err)
		}
		qty, err := norm.ParseFixed(w.Qty.String(), norm.SizeDecimals)
		if err != nil {
			return nil, fmt.Errorf("qty %q: %w", w.Qty, err)
		}
		out[i] = norm.Level{Price: price, Size: qty}
	}
	return out, nil
}
