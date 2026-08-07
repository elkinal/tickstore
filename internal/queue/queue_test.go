package queue

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/elkinal/tickstore/internal/norm"
)

// fakeInserter stands in for the ClickHouse sink: it just collects rows.
type fakeInserter struct {
	mu   sync.Mutex
	rows []norm.Trade
}

func (f *fakeInserter) Insert(_ context.Context, rows []norm.Trade) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, rows...)
	return nil
}
func (f *fakeInserter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows)
}
func (f *fakeInserter) Close() error { return nil }

// TestPublishConsume verifies a published batch is durably logged and drained
// into the inserter. It needs a JetStream-enabled NATS; set NATS_TEST_URL to run.
func TestPublishConsume(t *testing.T) {
	url := os.Getenv("NATS_TEST_URL")
	if url == "" {
		t.Skip("set NATS_TEST_URL (nats://host:4222) to run the queue integration test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	bus, err := Open(ctx, url, "TICKS_TEST", log)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bus.Close()

	ins := &fakeInserter{}
	go func() { _ = Consume[norm.Trade](ctx, bus, SubjectTrades, "test-trade-sink", ins, log) }()

	pub := NewPublisher[norm.Trade](bus, SubjectTrades)
	const n = 300
	for i := 0; i < n; i++ {
		pub.Add(norm.Trade{Venue: "coinbase", Symbol: "BTC-USD", Price: int64(i), Size: 1})
	}

	deadline := time.Now().Add(10 * time.Second)
	for ins.count() < n && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got := ins.count(); got != n {
		t.Fatalf("consumed %d rows, want %d", got, n)
	}
}
