package norm

import "time"

// Level is one price level in an order book: the total resting size at Price.
// Price and Size are fixed-point int64, same scales as Trade.
type Level struct {
	Price int64
	Size  int64
}

// BookSnapshot is a full picture of one book at a moment: every price level on
// each side, valid as of Seq. A connector emits one to seed or resync a book.
//
// Bids and Asks are given in the venue's order; the book engine sorts them, so
// callers needn't.
type BookSnapshot struct {
	Venue      string
	Symbol     string
	TsExchange time.Time
	TsReceived time.Time
	Bids       []Level
	Asks       []Level
	Seq        uint64
}

// BookUpdate is a single change to one price level: set the size at Price on
// Side to Size, or remove the level when Size is zero.
//
// Seq is the venue's sequence number for this update. The engine expects them
// contiguous; a jump means updates were missed and the book must resync. Venues
// that don't sequence their L2 feed leave Seq monotonic so no gap is flagged.
type BookUpdate struct {
	Venue      string
	Symbol     string
	TsExchange time.Time
	TsReceived time.Time
	Side       Side
	Price      int64
	Size       int64
	Seq        uint64
}
