package sink

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/elkinal/tickstore/internal/norm"
)

// fakeInserter records the batches it's handed and can be told to fail the
// first failN calls, to exercise the retry path.
type fakeInserter struct {
	mu       sync.Mutex
	batches  [][]norm.Trade
	failN    int
	inserted chan int // batch size, sent on each successful insert
}

func (f *fakeInserter) Insert(ctx context.Context, trades []norm.Trade) error {
	f.mu.Lock()
	if f.failN > 0 {
		f.failN--
		f.mu.Unlock()
		return errors.New("boom")
	}
	// Copy: the Batcher reuses its buffer after we return.
	cp := append([]norm.Trade(nil), trades...)
	f.batches = append(f.batches, cp)
	f.mu.Unlock()
	if f.inserted != nil {
		f.inserted <- len(cp)
	}
	return nil
}

func (f *fakeInserter) Close() error { return nil }

func (f *fakeInserter) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, b := range f.batches {
		n += len(b)
	}
	return n
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func tradeN(i int) norm.Trade {
	return norm.Trade{Venue: "test", Symbol: "BTC-USD", Price: int64(i), Size: 1, Side: norm.Buy}
}

// waitFor reads one value from ch or fails after a generous timeout, so a stuck
// test fails loudly instead of hanging.
func waitFor[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for insert")
		panic("unreachable")
	}
}

func TestFlushOnSize(t *testing.T) {
	f := &fakeInserter{inserted: make(chan int, 4)}
	// Long delay so only the size trigger can fire.
	b := NewBatcher(f, Config{MaxRows: 5, MaxDelay: time.Hour, Logger: quietLogger()})
	defer b.Close(context.Background())

	for i := 0; i < 5; i++ {
		b.OnTrade(tradeN(i))
	}
	if got := waitFor(t, f.inserted); got != 5 {
		t.Fatalf("batch size = %d, want 5", got)
	}
}

func TestFlushOnDelay(t *testing.T) {
	f := &fakeInserter{inserted: make(chan int, 4)}
	// Big size so only the timer can fire.
	b := NewBatcher(f, Config{MaxRows: 1000, MaxDelay: 20 * time.Millisecond, Logger: quietLogger()})
	defer b.Close(context.Background())

	start := time.Now()
	for i := 0; i < 3; i++ {
		b.OnTrade(tradeN(i))
	}
	if got := waitFor(t, f.inserted); got != 3 {
		t.Fatalf("batch size = %d, want 3", got)
	}
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond {
		t.Fatalf("flushed after %v, expected to wait for the timer", elapsed)
	}
}

func TestCloseFlushesRemainder(t *testing.T) {
	f := &fakeInserter{}
	// Neither trigger fires on its own; only Close should flush.
	b := NewBatcher(f, Config{MaxRows: 1000, MaxDelay: time.Hour, Logger: quietLogger()})

	for i := 0; i < 3; i++ {
		b.OnTrade(tradeN(i))
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := f.total(); got != 3 {
		t.Fatalf("flushed %d trades, want 3", got)
	}
}

func TestRetryUntilSuccess(t *testing.T) {
	f := &fakeInserter{failN: 2, inserted: make(chan int, 1)}
	b := NewBatcher(f, Config{
		MaxRows: 1, MaxDelay: time.Hour,
		RetryMin: time.Millisecond, RetryMax: 5 * time.Millisecond,
		Logger: quietLogger(),
	})
	defer b.Close(context.Background())

	b.OnTrade(tradeN(1))
	if got := waitFor(t, f.inserted); got != 1 {
		t.Fatalf("batch size = %d, want 1", got)
	}
	// Two failures were consumed before the success.
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failN != 0 {
		t.Fatalf("failN = %d, want 0 (retries should have consumed both)", f.failN)
	}
}

// TestNoDataLossUnderBackpressure floods the batcher with far more trades than
// its buffer holds, forcing backpressure, and checks every trade lands in order.
func TestNoDataLossUnderBackpressure(t *testing.T) {
	f := &fakeInserter{}
	b := NewBatcher(f, Config{
		MaxRows: 100, MaxDelay: 5 * time.Millisecond, Buffer: 64,
		Logger: quietLogger(),
	})

	const n = 2500
	for i := 0; i < n; i++ {
		b.OnTrade(tradeN(i))
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := f.total(); got != n {
		t.Fatalf("delivered %d trades, want %d", got, n)
	}
	// Order must be preserved: Price carries the sequence index.
	f.mu.Lock()
	defer f.mu.Unlock()
	want := int64(0)
	for _, batch := range f.batches {
		for _, tr := range batch {
			if tr.Price != want {
				t.Fatalf("out-of-order trade: got %d, want %d", tr.Price, want)
			}
			want++
		}
	}
}
