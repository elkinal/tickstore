package kraken

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/coder/websocket"

	"github.com/elkinal/tickstore/internal/book"
	"github.com/elkinal/tickstore/internal/norm"
	"github.com/elkinal/tickstore/internal/venue"
)

const (
	// bookReadLimit is larger than the trade feed's: the instrument snapshot
	// lists every pair.
	bookReadLimit = 32 << 20
	// bookDepth is the subscribed order book depth (levels per side).
	bookDepth = 10
)

// BookObserver is notified after each book frame is applied and validated. It
// runs on the read loop, so it should be quick; the *book.Book is read-only.
type BookObserver interface {
	OnBook(b *book.Book)
}

// precision holds a pair's display precision, needed to reproduce Kraken's
// checksum string.
type precision struct{ price, qty int }

// BookConnector maintains L2 books from Kraken and validates each one against
// Kraken's CRC32 checksum. Unlike Coinbase, this is real live integrity
// checking: a checksum mismatch means we missed or misapplied an update, and we
// resync by reconnecting for a fresh snapshot.
type BookConnector struct {
	url      string
	symbols  []string
	observer BookObserver
	log      *slog.Logger
}

// NewBook builds a BookConnector. A nil logger uses slog.Default(); observer
// may be nil.
func NewBook(symbols []string, observer BookObserver, log *slog.Logger) *BookConnector {
	if log == nil {
		log = slog.Default()
	}
	return &BookConnector{
		url:      FeedURL,
		symbols:  symbols,
		observer: observer,
		log:      log.With("venue", Name, "feed", "book"),
	}
}

// Name returns "kraken".
func (c *BookConnector) Name() string { return Name }

// Run keeps the book feed alive until ctx is canceled, reconnecting with
// backoff. Each session reseeds books from fresh snapshots.
func (c *BookConnector) Run(ctx context.Context) error {
	return venue.RunWithReconnect(ctx, c.log, c.session)
}

// seqBook pairs a book with the monotonic seq we assign (v2 book frames carry no
// sequence number; integrity comes from the checksum instead).
type seqBook struct {
	book *book.Book
	seq  uint64
}

type instrumentPair struct {
	Symbol         string `json:"symbol"`
	PricePrecision int    `json:"price_precision"`
	QtyPrecision   int    `json:"qty_precision"`
}

func (c *BookConnector) session(ctx context.Context) (gotData bool, err error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	conn, _, err := websocket.Dial(dialCtx, c.url, nil)
	cancel()
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "shutting down")
	conn.SetReadLimit(bookReadLimit)

	// instrument gives us per-pair precision (for the checksum); book gives the
	// data. Subscribe to instrument first so precisions are usually known before
	// the first book frame.
	for _, req := range [][]byte{
		mustJSON(subscribeRequest{Method: "subscribe", Params: subscribeParams{Channel: "instrument"}}),
		mustJSON(subscribeRequest{Method: "subscribe", Params: subscribeParams{Channel: "book", Symbol: c.symbols, Depth: bookDepth}}),
	} {
		if err := conn.Write(ctx, websocket.MessageText, req); err != nil {
			return false, fmt.Errorf("write subscribe: %w", err)
		}
	}
	c.log.Info("subscribed", "symbols", c.symbols)

	precisions := make(map[string]precision)
	books := make(map[string]*seqBook)

	for {
		readCtx, cancel := context.WithTimeout(ctx, readTimeout)
		_, raw, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			return gotData, fmt.Errorf("read: %w", err)
		}

		var e envelope
		if err := json.Unmarshal(raw, &e); err != nil {
			c.log.Error("book bad json", "error", err)
			continue
		}
		if e.Method != "" {
			if e.Success != nil && !*e.Success {
				return gotData, fmt.Errorf("%w: %s", errVenueError, e.Error)
			}
			continue
		}
		switch e.Channel {
		case "instrument":
			var data struct {
				Pairs []instrumentPair `json:"pairs"`
			}
			if err := json.Unmarshal(e.Data, &data); err != nil {
				c.log.Error("instrument decode", "error", err)
				continue
			}
			for _, p := range data.Pairs {
				precisions[p.Symbol] = precision{price: p.PricePrecision, qty: p.QtyPrecision}
			}
		case "book":
			gotData = true
			var rows []bookDataWire
			if err := json.Unmarshal(e.Data, &rows); err != nil {
				c.log.Error("book decode", "error", err)
				continue
			}
			for i := range rows {
				if err := c.applyBook(&rows[i], e.Type == "snapshot", precisions, books); err != nil {
					// A checksum mismatch (or bad frame) means the book is
					// corrupt; reconnect for a fresh snapshot.
					return gotData, err
				}
			}
		}
	}
}

// applyBook applies one book frame to its book and validates the checksum.
func (c *BookConnector) applyBook(d *bookDataWire, isSnapshot bool, precisions map[string]precision, books map[string]*seqBook) error {
	sb := books[d.Symbol]
	if sb == nil {
		sb = &seqBook{book: book.New(Name, d.Symbol)}
		books[d.Symbol] = sb
	}

	if isSnapshot {
		bids, err := parseBookLevels(d.Bids)
		if err != nil {
			return fmt.Errorf("kraken: snapshot bids: %w", err)
		}
		asks, err := parseBookLevels(d.Asks)
		if err != nil {
			return fmt.Errorf("kraken: snapshot asks: %w", err)
		}
		sb.seq++
		sb.book.ApplySnapshot(norm.BookSnapshot{Symbol: d.Symbol, Bids: bids, Asks: asks, Seq: sb.seq})
	} else {
		if err := applyChanges(sb, d.Bids, norm.Buy); err != nil {
			return err
		}
		if err := applyChanges(sb, d.Asks, norm.Sell); err != nil {
			return err
		}
	}

	// We subscribed at depth 10, so keep only the top 10 per side: the feed
	// maintains a window, and untrimmed levels that left it would corrupt the
	// checksum. Trim before validating.
	sb.book.Trim(bookDepth)

	// Validate against Kraken's checksum once we know the pair's precision.
	if prec, ok := precisions[d.Symbol]; ok {
		bids, asks := sb.book.Depth(10)
		if got := bookChecksum(bids, asks, prec.price, prec.qty); got != d.Checksum {
			return fmt.Errorf("kraken: %s checksum mismatch: got %d, want %d", d.Symbol, got, d.Checksum)
		}
	}
	if c.observer != nil {
		c.observer.OnBook(sb.book)
	}
	return nil
}

// applyChanges applies one side's level changes (qty 0 removes a level).
func applyChanges(sb *seqBook, levels []bookLevelWire, side norm.Side) error {
	for _, w := range levels {
		price, err := norm.ParseFixed(w.Price.String(), norm.PriceDecimals)
		if err != nil {
			return fmt.Errorf("kraken: update price %q: %w", w.Price, err)
		}
		qty, err := norm.ParseFixed(w.Qty.String(), norm.SizeDecimals)
		if err != nil {
			return fmt.Errorf("kraken: update qty %q: %w", w.Qty, err)
		}
		sb.seq++
		sb.book.Apply(norm.BookUpdate{Side: side, Price: price, Size: qty, Seq: sb.seq})
	}
	return nil
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // subscribe structs are fixed shapes; marshal can't fail
	}
	return b
}
