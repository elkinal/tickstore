// Package kraken streams public market data from the Kraken v2 websocket feed
// and normalizes it into norm types.
package kraken

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/coder/websocket"

	"github.com/elkinal/tickstore/internal/metrics"
	"github.com/elkinal/tickstore/internal/venue"
)

// Name is Kraken's id in norm types.
const Name = "kraken"

// FeedURL is Kraken's public v2 websocket endpoint.
const FeedURL = "wss://ws.kraken.com/v2"

const (
	dialTimeout = 15 * time.Second
	readTimeout = 30 * time.Second // heartbeats keep this honest in quiet markets
	readLimit   = 16 << 20         // 16 MiB; book snapshots are the largest frames
)

// Connector streams trades for a set of symbols (Kraken format, e.g. "BTC/USD").
// It implements venue.Venue.
type Connector struct {
	url     string
	symbols []string
	log     *slog.Logger
}

// New builds a Connector for the given symbols. A nil logger uses slog.Default().
func New(symbols []string, log *slog.Logger) *Connector {
	if log == nil {
		log = slog.Default()
	}
	return &Connector{url: FeedURL, symbols: symbols, log: log.With("venue", Name)}
}

// Name returns "kraken".
func (c *Connector) Name() string { return Name }

// Run streams trades to h until ctx is canceled, reconnecting with backoff.
func (c *Connector) Run(ctx context.Context, h venue.Handler) error {
	return venue.RunWithReconnect(ctx, c.log, func(ctx context.Context) (bool, error) {
		return c.session(ctx, h)
	})
}

// subscribeRequest is a Kraken v2 subscribe command.
type subscribeRequest struct {
	Method string          `json:"method"`
	Params subscribeParams `json:"params"`
}

type subscribeParams struct {
	Channel  string   `json:"channel"`
	Symbol   []string `json:"symbol,omitempty"`
	Depth    int      `json:"depth,omitempty"`
	Snapshot *bool    `json:"snapshot,omitempty"` // pointer: trade sends false, book omits (defaults true)
}

func (c *Connector) session(ctx context.Context, h venue.Handler) (gotData bool, err error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	conn, _, err := websocket.Dial(dialCtx, c.url, nil)
	cancel()
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "shutting down")
	conn.SetReadLimit(readLimit)

	noSnapshot := false // skip the recent-trades replay (DECISIONS.md D11)
	sub, err := json.Marshal(subscribeRequest{
		Method: "subscribe",
		Params: subscribeParams{
			Channel:  "trade",
			Symbol:   c.symbols,
			Snapshot: &noSnapshot,
		},
	})
	if err != nil {
		return false, fmt.Errorf("marshal subscribe: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, sub); err != nil {
		return false, fmt.Errorf("write subscribe: %w", err)
	}
	c.log.Info("subscribed", "symbols", c.symbols)

	for {
		readCtx, cancel := context.WithTimeout(ctx, readTimeout)
		_, raw, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			return gotData, fmt.Errorf("read: %w", err)
		}
		metrics.Messages.WithLabelValues(Name).Inc()
		trades, err := parseMessage(raw, time.Now())
		if err != nil {
			if errors.Is(err, errVenueError) {
				return gotData, err
			}
			metrics.ParseErrors.WithLabelValues(Name).Inc()
			c.log.Error("parse error", "error", err, "raw", string(raw))
			continue
		}
		gotData = true
		for i := range trades {
			h.OnTrade(trades[i])
		}
	}
}
