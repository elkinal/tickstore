// Package coinbase streams public market data from the Coinbase Exchange
// websocket feed and normalizes it into norm types.
package coinbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"nhooyr.io/websocket"

	"github.com/elkinal/tickstore/internal/venue"
)

// Name is the canonical venue identifier for Coinbase.
const Name = "coinbase"

// FeedURL is the public Coinbase Exchange websocket endpoint.
const FeedURL = "wss://ws-feed.exchange.coinbase.com"

// Reconnect/backoff and liveness tuning.
const (
	dialTimeout = 15 * time.Second
	// readTimeout bounds one Read call. The heartbeat channel delivers a
	// frame per product every second, so a quiet 30s means a dead peer.
	readTimeout = 30 * time.Second
	backoffMin  = time.Second
	backoffMax  = time.Minute
	// readLimit is the max accepted frame size. Match frames are tiny but
	// L2 snapshots (a later milestone) run to megabytes; 8 MiB is safe.
	readLimit = 8 << 20
)

// Connector streams trades for a fixed set of products from Coinbase.
// It implements venue.Venue.
type Connector struct {
	url     string
	symbols []string
	log     *slog.Logger
}

// New returns a Connector for the given product ids (e.g. "BTC-USD").
// A nil logger defaults to slog.Default().
func New(symbols []string, log *slog.Logger) *Connector {
	if log == nil {
		log = slog.Default()
	}
	return &Connector{url: FeedURL, symbols: symbols, log: log.With("venue", Name)}
}

// Name implements venue.Venue.
func (c *Connector) Name() string { return Name }

// Run implements venue.Venue: it dials the feed, subscribes, and streams
// normalized trades to h until ctx is canceled, reconnecting on any
// session failure with exponential backoff plus full jitter.
func (c *Connector) Run(ctx context.Context, h venue.Handler) error {
	backoff := backoffMin
	for {
		gotData, err := c.session(ctx, h)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if gotData {
			backoff = backoffMin
		}
		// Full jitter: sleep a uniform random slice of the current cap to
		// avoid thundering-herd reconnects.
		sleep := rand.N(backoff)
		c.log.Warn("session ended, reconnecting",
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

// subscribeRequest is the wire format of a Coinbase subscribe message.
type subscribeRequest struct {
	Type       string   `json:"type"`
	ProductIDs []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

// session runs one connect-subscribe-read cycle. It returns whether any
// frame was successfully processed (used to reset backoff) and the error
// that ended the session.
func (c *Connector) session(ctx context.Context, h venue.Handler) (gotData bool, err error) {
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
		// matches carries trades; heartbeat keeps the read loop's timeout
		// honest during quiet markets.
		Channels: []string{"matches", "heartbeat"},
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
		trade, err := parseMessage(raw, time.Now())
		if err != nil {
			// A venue-reported error frame means the subscription itself is
			// broken (e.g. bad product id): end the session. Anything else
			// is one bad frame: log it and keep reading.
			if errors.Is(err, errVenueError) {
				return gotData, err
			}
			c.log.Error("parse error", "error", err, "raw", string(raw))
			continue
		}
		gotData = true
		if trade != nil {
			h.OnTrade(*trade)
		}
	}
}
