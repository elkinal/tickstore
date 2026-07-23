package okx

import (
	"fmt"

	"github.com/elkinal/tickstore/internal/norm"
)

// bookData is one book payload. Levels are [price, size, deprecated, numOrders];
// we read price and size. OKX's public books channel always sends checksum 0
// (real checksums require the auth-only tick-by-tick channels), so integrity
// here comes from the seqId/prevSeqId linkage, not the checksum.
type bookData struct {
	Asks      [][]string `json:"asks"`
	Bids      [][]string `json:"bids"`
	Ts        string     `json:"ts"`
	SeqID     int64      `json:"seqId"`
	PrevSeqID int64      `json:"prevSeqId"`
}

// parseBookLevels converts OKX [price, size, ...] rows to fixed-point levels.
func parseBookLevels(rows [][]string) ([]norm.Level, error) {
	out := make([]norm.Level, len(rows))
	for i, r := range rows {
		if len(r) < 2 {
			return nil, fmt.Errorf("okx: book level has %d fields, want >= 2", len(r))
		}
		price, err := norm.ParseFixed(r[0], norm.PriceDecimals)
		if err != nil {
			return nil, fmt.Errorf("okx: book price %q: %w", r[0], err)
		}
		size, err := norm.ParseFixed(r[1], norm.SizeDecimals)
		if err != nil {
			return nil, fmt.Errorf("okx: book size %q: %w", r[1], err)
		}
		out[i] = norm.Level{Price: price, Size: size}
	}
	return out, nil
}
