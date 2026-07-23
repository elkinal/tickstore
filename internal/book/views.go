package book

import (
	"sort"

	"github.com/elkinal/tickstore/internal/norm"
)

// TopOfBook returns the best bid (highest) and best ask (lowest). ok is false if
// either side is empty.
func (b *Book) TopOfBook() (bid, ask norm.Level, ok bool) {
	bestBid, okBid := extreme(b.bids, true)
	bestAsk, okAsk := extreme(b.asks, false)
	if !okBid || !okAsk {
		return norm.Level{}, norm.Level{}, false
	}
	return bestBid, bestAsk, true
}

// Depth returns up to n price levels per side: bids highest-first, asks
// lowest-first. n <= 0 returns every level.
func (b *Book) Depth(n int) (bids, asks []norm.Level) {
	bids = sortedLevels(b.bids, true)
	asks = sortedLevels(b.asks, false)
	if n > 0 {
		if len(bids) > n {
			bids = bids[:n]
		}
		if len(asks) > n {
			asks = asks[:n]
		}
	}
	return bids, asks
}

// extreme returns the highest (desc) or lowest (asc) level of a side.
func extreme(side map[int64]int64, desc bool) (norm.Level, bool) {
	first := true
	var best int64
	for price := range side {
		if first || (desc && price > best) || (!desc && price < best) {
			best, first = price, false
		}
	}
	if first {
		return norm.Level{}, false
	}
	return norm.Level{Price: best, Size: side[best]}, true
}

// sortedLevels materializes a side as levels, ordered by price: descending for
// bids, ascending for asks.
func sortedLevels(side map[int64]int64, desc bool) []norm.Level {
	levels := make([]norm.Level, 0, len(side))
	for price, size := range side {
		levels = append(levels, norm.Level{Price: price, Size: size})
	}
	sort.Slice(levels, func(i, j int) bool {
		if desc {
			return levels[i].Price > levels[j].Price
		}
		return levels[i].Price < levels[j].Price
	})
	return levels
}
