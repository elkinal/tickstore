// Package sink batches normalized trades and writes them to storage.
//
// The Batcher collects trades and flushes them in groups, so storage sees a few
// large inserts instead of a flood of tiny ones. The actual write is an
// Inserter, so the batching logic here is independent of ClickHouse (and easy to
// test with a fake).
package sink

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/elkinal/tickstore/internal/norm"
)

// Inserter writes a batch of trades to storage. Insert must be safe to retry:
// the Batcher calls it again with the same batch if it fails.
type Inserter interface {
	Insert(ctx context.Context, trades []norm.Trade) error
	Close() error
}

// Defaults used when a Config field is left zero.
const (
	defaultMaxRows  = 10_000
	defaultMaxDelay = 250 * time.Millisecond
	retryMin        = 500 * time.Millisecond
	retryMax        = 30 * time.Second
)

// Config tunes a Batcher. Any zero field takes its default.
type Config struct {
	MaxRows  int           // flush once the buffer holds this many trades
	MaxDelay time.Duration // flush at least this often, even when partly full
	Buffer   int           // in-flight channel capacity; the backpressure bound
	RetryMin time.Duration // first retry wait after a failed insert
	RetryMax time.Duration // cap on the retry wait
	Logger   *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.MaxRows <= 0 {
		c.MaxRows = defaultMaxRows
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = defaultMaxDelay
	}
	if c.Buffer <= 0 {
		c.Buffer = 2 * c.MaxRows
	}
	if c.RetryMin <= 0 {
		c.RetryMin = retryMin
	}
	if c.RetryMax <= 0 {
		c.RetryMax = retryMax
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// Batcher buffers trades and flushes them to an Inserter. It implements
// venue.Handler, so a connector can stream straight into it.
type Batcher struct {
	in       chan norm.Trade
	inserter Inserter
	cfg      Config

	opCtx    context.Context // inserts run under this; canceled to give up
	opCancel context.CancelFunc

	stop     chan struct{} // closed by Close to trigger the final drain + flush
	stopped  chan struct{} // closed by the run loop when it exits
	stopOnce sync.Once
}

// NewBatcher returns a started Batcher. Call Close to flush and release it.
func NewBatcher(inserter Inserter, cfg Config) *Batcher {
	cfg = cfg.withDefaults()
	opCtx, opCancel := context.WithCancel(context.Background())
	b := &Batcher{
		in:       make(chan norm.Trade, cfg.Buffer),
		inserter: inserter,
		cfg:      cfg,
		opCtx:    opCtx,
		opCancel: opCancel,
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	go b.run()
	return b
}

// OnTrade queues a trade. If the buffer is full (storage can't keep up) it
// blocks, pushing backpressure up to the connector — better than growing memory
// without bound. It returns early only if the Batcher is closing.
func (b *Batcher) OnTrade(t norm.Trade) {
	select {
	case b.in <- t:
	case <-b.stop:
	}
}

// run owns the buffer. Keeping it in one goroutine means no locks: trades, the
// flush timer, and shutdown all funnel through this select.
func (b *Batcher) run() {
	defer close(b.stopped)
	ticker := time.NewTicker(b.cfg.MaxDelay)
	defer ticker.Stop()

	buf := make([]norm.Trade, 0, b.cfg.MaxRows)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		b.insertWithRetry(buf)
		buf = buf[:0]
	}

	for {
		select {
		case t := <-b.in:
			buf = append(buf, t)
			if len(buf) >= b.cfg.MaxRows {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-b.stop:
			// Drain whatever is still queued, then flush the remainder so a
			// clean shutdown loses nothing.
			for {
				select {
				case t := <-b.in:
					buf = append(buf, t)
					if len(buf) >= b.cfg.MaxRows {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// insertWithRetry keeps trying until the batch lands or opCtx is canceled
// (Close hit its deadline). Storage hiccups shouldn't drop data, so the only way
// out without success is giving up at shutdown.
func (b *Batcher) insertWithRetry(trades []norm.Trade) {
	backoff := b.cfg.RetryMin
	for attempt := 1; ; attempt++ {
		start := time.Now()
		err := b.inserter.Insert(b.opCtx, trades)
		if err == nil {
			b.cfg.Logger.Debug("flushed batch",
				"rows", len(trades), "took", time.Since(start).Round(time.Millisecond))
			return
		}
		if b.opCtx.Err() != nil {
			b.cfg.Logger.Error("dropping batch at shutdown",
				"rows", len(trades), "error", err)
			return
		}
		b.cfg.Logger.Error("insert failed, retrying",
			"attempt", attempt, "rows", len(trades), "error", err)
		select {
		case <-time.After(rand.N(backoff)): // full jitter, same idea as the connector
		case <-b.opCtx.Done():
			return
		}
		if backoff *= 2; backoff > b.cfg.RetryMax {
			backoff = b.cfg.RetryMax
		}
	}
}

// Close stops accepting trades, flushes what's buffered, and closes the
// Inserter. ctx bounds the shutdown: if it fires first, in-flight inserts are
// canceled and the pending batch is dropped.
func (b *Batcher) Close(ctx context.Context) error {
	b.stopOnce.Do(func() { close(b.stop) })
	select {
	case <-b.stopped:
	case <-ctx.Done():
		b.opCancel() // deadline exceeded: unblock the run loop
		<-b.stopped
		b.inserter.Close()
		return ctx.Err()
	}
	b.opCancel()
	return b.inserter.Close()
}
