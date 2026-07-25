package venue

import "github.com/elkinal/tickstore/internal/norm"

// BookSink receives L2 book-level changes for persistence: incremental deltas
// (IsSnapshot false) and full-snapshot levels (IsSnapshot true). It's called
// from a connector's read loop, so it should be quick (e.g. hand off to a
// batcher). A nil BookSink means "don't persist books".
type BookSink interface {
	OnBookUpdate(norm.BookUpdate)
}

// EmitSnapshot sends every level of a snapshot to the sink as an IsSnapshot
// book update (bids as Buy, asks as Sell). It's a no-op when sink is nil.
func EmitSnapshot(sink BookSink, s norm.BookSnapshot) {
	if sink == nil {
		return
	}
	emit := func(levels []norm.Level, side norm.Side) {
		for _, l := range levels {
			sink.OnBookUpdate(norm.BookUpdate{
				Venue: s.Venue, Symbol: s.Symbol,
				TsExchange: s.TsExchange, TsReceived: s.TsReceived,
				Side: side, Price: l.Price, Size: l.Size,
				Seq: s.Seq, IsSnapshot: true,
			})
		}
	}
	emit(s.Bids, norm.Buy)
	emit(s.Asks, norm.Sell)
}
