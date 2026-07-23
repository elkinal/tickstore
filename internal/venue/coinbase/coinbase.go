// Package coinbase connects to the Coinbase Exchange websocket feed and turns
// its trade messages into norm.Trade values.
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

// Name is Coinbase's id in norm types.
const Name = "coinbase"

// FeedURL is Coinbase's public websocket endpoint.
const FeedURL = "wss://ws-feed.exchange.coinbase.com"

const (
	dialTimeout = 15 * time.Second
	readTimeout = 30 * time.Second // heartbeats come ~1/s, so 30s of silence means a dead peer
	backoffMin  = time.Second
	backoffMax  = time.Minute
	readLimit   = 8 << 20 // 8 MiB, room for the big L2 snapshots a later milestone needs
)

// Connector streams trades for a fixed set of products. It implements venue.Venue.
type Connector struct {
	url     string
	symbols []string
	log     *slog.Logger
}

// New builds a Connector for the given product ids (e.g. "BTC-USD"). Pass a nil
// logger to use slog.Default().
func New(symbols []string, log *slog.Logger) *Connector {
	if log == nil {
		log = slog.Default()
	}
	return &Connector{url: FeedURL, symbols: symbols, log: log.With("venue", Name)}
}

// Name returns "coinbase".
func (c *Connector) Name() string { return Name }

// Run keeps a session alive until ctx is canceled. When one drops, it waits a
// bit and reconnects; the wait doubles each time (capped at backoffMax) and
// resets once a session actually receives data.
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
		// Sleep a random slice of the backoff (full jitter) so many clients
		// don't all reconnect at the same instant.
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

type subscribeRequest struct {
	Type       string   `json:"type"`
	ProductIDs []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

// session dials, subscribes, and reads frames until the connection breaks. It
// reports whether it ever got data (so Run can reset its backoff) and why it
// stopped.
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
		Channels:   []string{"matches", "heartbeat"}, // trades, plus heartbeats for liveness
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
			// A venue error means the subscription is broken, so give up and
			// let Run reconnect. A single bad frame isn't worth dropping the
			// connection over: log it and move on.
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
