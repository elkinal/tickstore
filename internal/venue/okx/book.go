package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/coder/websocket"

	"github.com/elkinal/tickstore/internal/book"
	"github.com/elkinal/tickstore/internal/metrics"
	"github.com/elkinal/tickstore/internal/norm"
	"github.com/elkinal/tickstore/internal/venue"
)

// snapshotPrevSeq is the prevSeqId OKX uses on a snapshot (the start marker).
const snapshotPrevSeq = -1

// BookObserver is notified after each book frame is applied. It runs on the read
// loop, so keep it quick; the *book.Book is read-only.
type BookObserver interface {
	OnBook(b *book.Book)
}

// BookConnector maintains L2 books from OKX's public books feed. Integrity is by
// sequence linkage: each update's prevSeqId must equal the last applied seqId; a
// break means we missed an update and resync from a fresh snapshot. (The public
// feed's checksum is always 0, so it isn't used.)
type BookConnector struct {
	url      string
	symbols  []string
	observer BookObserver
	log      *slog.Logger
}

// NewBook builds a BookConnector. A nil logger uses slog.Default(); observer may
// be nil.
func NewBook(symbols []string, observer BookObserver, log *slog.Logger) *BookConnector {
	if log == nil {
		log = slog.Default()
	}
	return &BookConnector{
		url:      FeedURL,
		symbols:  symbols,
		observer: observer,
		log:      log.With("venue", Name, "feed", "books"),
	}
}

// Name returns "okx".
func (c *BookConnector) Name() string { return Name }

// Run keeps the book feed alive until ctx is canceled, reconnecting with
// backoff. Each session reseeds books from fresh snapshots.
func (c *BookConnector) Run(ctx context.Context) error {
	return venue.RunWithReconnect(ctx, c.log, c.session)
}

// seqBook tracks a book plus the OKX seqId of the last applied frame (for gap
// detection) and a monotonic seq for the engine (which wants contiguous seqs).
type seqBook struct {
	book      *book.Book
	engineSeq uint64
	lastSeqID int64
	seeded    bool
}

// contiguous reports whether an update with prevSeqId follows what we last
// applied. OKX repeats seqId when a frame carries no book change; that's still
// contiguous.
func (sb *seqBook) contiguous(prevSeqID int64) bool {
	return sb.seeded && prevSeqID == sb.lastSeqID
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

	if err := subscribe(ctx, conn, "books", c.symbols); err != nil {
		return false, err
	}
	c.log.Info("subscribed", "symbols", c.symbols)

	pingCtx, stopPing := context.WithCancel(ctx)
	defer stopPing()
	go pinger(pingCtx, conn)

	books := make(map[string]*seqBook)

	for {
		readCtx, cancel := context.WithTimeout(ctx, readTimeout)
		_, raw, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			return gotData, fmt.Errorf("read: %w", err)
		}
		if string(raw) == "pong" {
			continue
		}
		var e envelope
		if err := json.Unmarshal(raw, &e); err != nil {
			c.log.Error("book bad json", "error", err)
			continue
		}
		if e.Event == "error" {
			return gotData, fmt.Errorf("%w: %s (%s)", errVenueError, e.Msg, e.Code)
		}
		if e.Arg.Channel != "books" || len(e.Data) == 0 {
			continue
		}
		var rows []bookData
		if err := json.Unmarshal(e.Data, &rows); err != nil {
			c.log.Error("book decode", "error", err)
			continue
		}
		gotData = true
		for i := range rows {
			if err := c.applyBook(e.Arg.InstID, e.Action, &rows[i], books); err != nil {
				return gotData, err // gap or bad frame: reconnect for a fresh snapshot
			}
		}
	}
}

func (c *BookConnector) applyBook(instID, action string, d *bookData, books map[string]*seqBook) error {
	sb := books[instID]
	if sb == nil {
		sb = &seqBook{book: book.New(Name, instID)}
		books[instID] = sb
	}

	if action == "snapshot" {
		bids, err := parseBookLevels(d.Bids)
		if err != nil {
			return err
		}
		asks, err := parseBookLevels(d.Asks)
		if err != nil {
			return err
		}
		sb.engineSeq++
		sb.book.ApplySnapshot(norm.BookSnapshot{Symbol: instID, Bids: bids, Asks: asks, Seq: sb.engineSeq})
		sb.lastSeqID = d.SeqID
		sb.seeded = true
	} else {
		if !sb.contiguous(d.PrevSeqID) {
			metrics.Gaps.WithLabelValues(Name).Inc()
			metrics.Resyncs.WithLabelValues(Name).Inc()
			return fmt.Errorf("okx: %s seq gap: prevSeqId %d, have %d", instID, d.PrevSeqID, sb.lastSeqID)
		}
		if err := c.applyChanges(sb, d.Bids, norm.Buy); err != nil {
			return err
		}
		if err := c.applyChanges(sb, d.Asks, norm.Sell); err != nil {
			return err
		}
		sb.lastSeqID = d.SeqID
	}

	if c.observer != nil {
		c.observer.OnBook(sb.book)
	}
	return nil
}

func (c *BookConnector) applyChanges(sb *seqBook, rows [][]string, side norm.Side) error {
	levels, err := parseBookLevels(rows)
	if err != nil {
		return err
	}
	for _, l := range levels {
		sb.engineSeq++
		sb.book.Apply(norm.BookUpdate{Side: side, Price: l.Price, Size: l.Size, Seq: sb.engineSeq})
	}
	return nil
}
