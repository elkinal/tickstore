// Package venue defines the interface every exchange connector implements.
// One subpackage per exchange (coinbase, kraken, okx) satisfies it.
package venue

import (
	"context"

	"github.com/elkinal/tickstore/internal/norm"
)

// Handler receives normalized market data events from a venue connector.
// Implementations must be fast or hand off to a channel; connectors call
// these methods from their read loop.
type Handler interface {
	// OnTrade is called once per normalized trade.
	OnTrade(norm.Trade)
}

// Venue is a streaming market data connector for one exchange.
type Venue interface {
	// Name returns the canonical venue identifier, e.g. "coinbase".
	Name() string
	// Run connects to the venue and streams normalized events to h until
	// ctx is canceled. Transient failures (disconnects, parse errors) are
	// handled internally with reconnect + backoff; Run returns ctx.Err()
	// on cancellation, or a non-nil error only for unrecoverable failures.
	Run(ctx context.Context, h Handler) error
}
