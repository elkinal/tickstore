// Package venue defines the interface every exchange connector implements.
// One subpackage per exchange (coinbase, kraken, okx) satisfies it.
package venue

import (
	"context"

	"github.com/elkinal/tickstore/internal/norm"
)

// Handler is where a connector sends the trades it parses. Connectors call it
// straight from the read loop, so it should return quickly.
type Handler interface {
	OnTrade(norm.Trade)
}

// Venue is a live market data connector for one exchange.
type Venue interface {
	Name() string // e.g. "coinbase"

	// Run streams trades to h until ctx is canceled. It handles disconnects
	// and bad frames itself (reconnecting as needed), so it returns only when
	// ctx is canceled or something unrecoverable happens.
	Run(ctx context.Context, h Handler) error
}
