// Package okx streams public market data from the OKX v5 websocket feed and
// normalizes it into norm types.
package okx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/coder/websocket"

	"github.com/elkinal/tickstore/internal/venue"
)

// Name is OKX's id in norm types.
const Name = "okx"

// FeedURL is OKX's public v5 websocket endpoint.
const FeedURL = "wss://ws.okx.com:8443/ws/v5/public"

const (
	dialTimeout = 15 * time.Second
	// readTimeout must exceed pingInterval so a healthy connection (kept alive
	// by our own pings) never trips it.
	readTimeout  = 40 * time.Second
	pingInterval = 20 * time.Second // OKX drops idle connections after ~30s
	readLimit    = 32 << 20
)

// Connector streams trades for a set of instruments (OKX format, e.g.
// "BTC-USDT"). It implements venue.Venue.
type Connector struct {
	url     string
	symbols []string
	log     *slog.Logger
}

// New builds a Connector for the given instruments. A nil logger uses
// slog.Default().
func New(symbols []string, log *slog.Logger) *Connector {
	if log == nil {
		log = slog.Default()
	}
	return &Connector{url: FeedURL, symbols: symbols, log: log.With("venue", Name)}
}

// Name returns "okx".
func (c *Connector) Name() string { return Name }

// Run streams trades to h until ctx is canceled, reconnecting with backoff.
func (c *Connector) Run(ctx context.Context, h venue.Handler) error {
	return venue.RunWithReconnect(ctx, c.log, func(ctx context.Context) (bool, error) {
		return c.session(ctx, h)
	})
}

// subscribeRequest is an OKX subscribe command.
type subscribeRequest struct {
	Op   string    `json:"op"`
	Args []argInfo `json:"args"`
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

	if err := subscribe(ctx, conn, "trades", c.symbols); err != nil {
		return false, err
	}
	c.log.Info("subscribed", "symbols", c.symbols)

	// OKX has no server heartbeat; keep the connection alive with our own pings.
	// The pinger is the only writer after subscribe, so it doesn't race reads.
	pingCtx, stopPing := context.WithCancel(ctx)
	defer stopPing()
	go pinger(pingCtx, conn)

	for {
		readCtx, cancel := context.WithTimeout(ctx, readTimeout)
		_, raw, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			return gotData, fmt.Errorf("read: %w", err)
		}
		trades, err := parseMessage(raw, time.Now())
		if err != nil {
			if errors.Is(err, errVenueError) {
				return gotData, err
			}
			c.log.Error("parse error", "error", err, "raw", string(raw))
			continue
		}
		gotData = true
		for i := range trades {
			h.OnTrade(trades[i])
		}
	}
}

// subscribe sends one subscribe command for the channel across all symbols.
func subscribe(ctx context.Context, conn *websocket.Conn, channel string, symbols []string) error {
	args := make([]argInfo, len(symbols))
	for i, s := range symbols {
		args[i] = argInfo{Channel: channel, InstID: s}
	}
	msg, err := json.Marshal(subscribeRequest{Op: "subscribe", Args: args})
	if err != nil {
		return fmt.Errorf("marshal subscribe: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, msg); err != nil {
		return fmt.Errorf("write subscribe: %w", err)
	}
	return nil
}

// pinger sends "ping" until ctx is canceled. Write errors are ignored; the read
// loop will surface the disconnect.
func pinger(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = conn.Write(ctx, websocket.MessageText, []byte("ping"))
		}
	}
}
