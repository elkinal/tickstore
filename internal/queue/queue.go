// Package queue puts a durable NATS JetStream log between the venue feed handlers
// and the ClickHouse sink. Trades and book updates are published to a work-queue
// stream on ingest and drained into ClickHouse by a consumer that acks only after
// a successful insert — so a crash can't lose rows that already reached the log
// (they're redelivered and re-inserted). The dashboard's live registry is still
// fed directly, so the queue sits in front of persistence only. See DECISIONS.md.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/elkinal/tickstore/internal/sink"
)

// Subjects the tick stream carries — one per feed, so their sink consumers don't
// overlap (a requirement of work-queue retention).
const (
	SubjectTrades = "ticks.trades"
	SubjectBook   = "ticks.book"
)

// Bus is a NATS JetStream connection with the tick stream ensured.
type Bus struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	stream string
	log    *slog.Logger
}

// Open connects to NATS and ensures a file-backed work-queue stream over the tick
// subjects. Work-queue retention drops each message once its sink consumer acks
// it, so the log holds only what hasn't yet reached ClickHouse.
func Open(ctx context.Context, url, stream string, log *slog.Logger) (*Bus, error) {
	nc, err := nats.Connect(url,
		nats.Name("tickstore"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("queue: connect %s: %w", url, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("queue: jetstream: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      stream,
		Subjects:  []string{"ticks.>"},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		MaxAge:    24 * time.Hour, // safety cap if the sink ever falls badly behind
	}); err != nil {
		nc.Close()
		return nil, fmt.Errorf("queue: stream %s: %w", stream, err)
	}
	return &Bus{nc: nc, js: js, stream: stream, log: log}, nil
}

// Close flushes pending async publishes, then drains and closes the connection.
func (b *Bus) Close() error {
	select {
	case <-b.js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
	}
	return b.nc.Drain()
}

// Publisher publishes values of T to a subject. Its Add matches the sink Batcher's
// method, so it drops into the ingest path in place of a direct ClickHouse sink.
type Publisher[T any] struct {
	js      jetstream.JetStream
	subject string
	log     *slog.Logger
}

// NewPublisher returns a publisher for subject on the bus.
func NewPublisher[T any](b *Bus, subject string) *Publisher[T] {
	return &Publisher[T]{js: b.js, subject: subject, log: b.log}
}

// Add publishes v to the stream. It's asynchronous, so it never blocks the venue
// read loop; JetStream persists the message and the sink consumer picks it up.
func (p *Publisher[T]) Add(v T) {
	data, err := json.Marshal(v)
	if err != nil {
		p.log.Warn("queue: marshal", "error", err)
		return
	}
	if _, err := p.js.PublishAsync(p.subject, data); err != nil {
		p.log.Warn("queue: publish", "error", err)
	}
}

// Consume drains subject into ins in batches, acking each batch only after a
// successful insert. A crash between insert and ack redelivers the batch
// (at-least-once), so rows that reached the log are never lost. It runs until ctx
// is cancelled; anything undelivered stays in the stream for the next start.
func Consume[T any](ctx context.Context, b *Bus, subject, durable string, ins sink.Inserter[T], log *slog.Logger) error {
	cons, err := b.js.CreateOrUpdateConsumer(ctx, b.stream, jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 20000,
	})
	if err != nil {
		return fmt.Errorf("queue: consumer %s: %w", durable, err)
	}
	for ctx.Err() == nil {
		batch, err := cons.Fetch(500, jetstream.FetchMaxWait(250*time.Millisecond))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Warn("queue: fetch", "error", err)
			time.Sleep(time.Second)
			continue
		}
		var rows []T
		var msgs []jetstream.Msg
		for m := range batch.Messages() {
			var v T
			if err := json.Unmarshal(m.Data(), &v); err != nil {
				_ = m.Ack() // poison message: drop rather than redeliver forever
				continue
			}
			rows = append(rows, v)
			msgs = append(msgs, m)
		}
		if err := batch.Error(); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("queue: batch", "error", err)
		}
		if len(rows) == 0 {
			continue
		}
		if err := ins.Insert(ctx, rows); err != nil {
			log.Error("queue: insert", "error", err, "rows", len(rows))
			for _, m := range msgs {
				_ = m.Nak() // leave un-acked so it's redelivered
			}
			continue
		}
		for _, m := range msgs {
			_ = m.Ack()
		}
	}
	return nil
}
