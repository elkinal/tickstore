package coinbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/coder/websocket"

	"github.com/elkinal/tickstore/internal/book"
	"github.com/elkinal/tickstore/internal/norm"
)

// BookObserver is notified after each level2 frame is applied, with the current
// state of the affected book. It's called from the read loop, so it should be
// quick; the *book.Book is for reading (TopOfBook/Depth) only.
type BookObserver interface {
	OnBook(b *book.Book)
}

// BookConnector maintains L2 order books for a set of products from Coinbase's
// public level2_batch feed. It implements venue.Venue's lifecycle shape but
// deals in books rather than trades.
//
// The feed has no per-update sequence number, so the connector assigns a
// monotonic seq per book: the stream is always contiguous and the engine's gap
// detection never fires here. Recovery is snapshot-driven — a reconnect yields a
// fresh snapshot that reseeds the book.
type BookConnector struct {
	url      string
	symbols  []string
	observer BookObserver
	log      *slog.Logger
}

// NewBook builds a BookConnector for the given products. A nil logger defaults
// to slog.Default(); observer may be nil.
func NewBook(symbols []string, observer BookObserver, log *slog.Logger) *BookConnector {
	if log == nil {
		log = slog.Default()
	}
	return &BookConnector{
		url:      FeedURL,
		symbols:  symbols,
		observer: observer,
		log:      log.With("venue", Name, "feed", "level2"),
	}
}

// Name implements venue.Venue.
func (c *BookConnector) Name() string { return Name }

// Run keeps the level2 feed alive until ctx is canceled, reconnecting with
// jittered backoff. Each new session rebuilds the books from a fresh snapshot.
func (c *BookConnector) Run(ctx context.Context) error {
	backoff := backoffMin
	for {
		gotData, err := c.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if gotData {
			backoff = backoffMin
		}
		sleep := rand.N(backoff)
		c.log.Warn("level2 session ended, reconnecting",
			"error", err, "sleep", sleep.Round(time.Millisecond))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
		if backoff *= 2; backoff > backoffMax {
			backoff = backoffMax
		}
	}
}

// seqBook pairs a book with the monotonic sequence counter we assign to its
// updates (the feed provides none).
type seqBook struct {
	book *book.Book
	seq  uint64
}

func (c *BookConnector) session(ctx context.Context) (gotData bool, err error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	conn, _, err := websocket.Dial(dialCtx, c.url, nil)
	cancel()
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "shutting down")
	conn.SetReadLimit(readLimit)

	sub, err := json.Marshal(subscribeRequest{
		Type:       "subscribe",
		ProductIDs: c.symbols,
		Channels:   []string{"level2_batch"},
	})
	if err != nil {
		return false, fmt.Errorf("marshal subscribe: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, sub); err != nil {
		return false, fmt.Errorf("write subscribe: %w", err)
	}
	c.log.Info("subscribed", "symbols", c.symbols)

	// Fresh books each session; the first snapshot per symbol seeds them.
	books := make(map[string]*seqBook, len(c.symbols))

	for {
		readCtx, cancel := context.WithTimeout(ctx, readTimeout)
		_, raw, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			return gotData, fmt.Errorf("read: %w", err)
		}
		snap, ups, err := parseLevel2(raw, time.Now())
		if err != nil {
			if errors.Is(err, errVenueError) {
				return gotData, err
			}
			c.log.Error("level2 parse error", "error", err)
			continue
		}
		gotData = true
		c.dispatch(books, snap, ups)
	}
}

// dispatch applies a snapshot or a batch of updates to the right book, assigns
// sequence numbers, and notifies the observer.
func (c *BookConnector) dispatch(books map[string]*seqBook, snap *norm.BookSnapshot, ups []norm.BookUpdate) {
	if snap != nil {
		sb := books[snap.Symbol]
		if sb == nil {
			sb = &seqBook{book: book.New(Name, snap.Symbol)}
			books[snap.Symbol] = sb
		}
		sb.seq++
		snap.Seq = sb.seq
		sb.book.ApplySnapshot(*snap)
		c.notify(sb.book)
		return
	}
	if len(ups) == 0 {
		return
	}
	sb := books[ups[0].Symbol]
	if sb == nil {
		// Updates before the snapshot: buffer them so the engine replays them
		// once the snapshot arrives.
		sb = &seqBook{book: book.New(Name, ups[0].Symbol)}
		books[ups[0].Symbol] = sb
	}
	for i := range ups {
		sb.seq++
		ups[i].Seq = sb.seq
		sb.book.Apply(ups[i])
	}
	c.notify(sb.book)
}

func (c *BookConnector) notify(b *book.Book) {
	if c.observer != nil {
		c.observer.OnBook(b)
	}
}
