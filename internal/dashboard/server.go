// Package dashboard serves a small live web view of the running engine: a
// scrolling trade and order-book feed plus storage stats. The live views
// (quotes, depth ladder, tape) are read from the engine's in-memory state
// (internal/live); only the storage stats (row counts, disk usage) come from
// ClickHouse. Updates are pushed to the browser over one SSE connection.
package dashboard

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/elkinal/tickstore/internal/live"
	"github.com/elkinal/tickstore/internal/sink"
)

// streamInterval is how often the SSE stream pushes a fresh snapshot.
const streamInterval = 500 * time.Millisecond

// feedLimit caps how many rows one feed poll returns, so a burst (or a fresh
// page loading from an empty cursor) can't pull an unbounded result.
const feedLimit = 200

//go:embed index.html
var indexHTML []byte

// Store is the persistent (ClickHouse) read side: storage stats, plus the legacy
// /api/tape endpoint. The live SSE tape comes from the in-memory registry, not
// from here. *sink.ClickHouse satisfies it.
type Store interface {
	TableStats(ctx context.Context) ([]sink.TableStat, error)
	RecentTrades(ctx context.Context, sinceNanos int64, limit int) ([]sink.FeedRow, error)
	PriceHistory(ctx context.Context, windowSec, bucketSec int) ([]sink.PricePoint, error)
	LatencyHistory(ctx context.Context, windowSec, bucketSec int) ([]sink.LatencyPoint, error)
	ThroughputHistory(ctx context.Context, windowSec, bucketSec int) ([]sink.RatePoint, error)
}

// Live is the in-memory read side: current books and recent trades, for the
// quote grid, depth ladder, and tape. *live.Registry satisfies it.
type Live interface {
	Quotes() []live.Quote
	Depth(venue, symbol string) (bids, asks []live.DepthRow, ok bool)
	AllDepth() map[string]live.BookDepth
	TradesSince(cursor int64, limit int) ([]live.TradeRow, int64)
	LatestTradeID() int64
}

// Serve runs the dashboard HTTP server at addr until ctx is canceled. A blank
// addr is a no-op, so the dashboard is opt-in via config.
func Serve(ctx context.Context, addr string, store Store, reg Live, log *slog.Logger) {
	if addr == "" {
		return
	}
	h := &handler{store: store, reg: reg, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("/", h.index)
	mux.HandleFunc("/api/stream", h.stream)
	mux.HandleFunc("/api/stats", h.stats)
	mux.HandleFunc("/api/tape", h.tape)
	mux.HandleFunc("/api/book", h.book)
	mux.HandleFunc("/api/depth", h.depth)
	mux.HandleFunc("/api/pricehistory", h.priceHistory)
	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("dashboard listening", "addr", addr, "url", "http://"+addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("dashboard server", "error", err)
	}
}

type handler struct {
	store Store
	reg   Live
	log   *slog.Logger
}

func (h *handler) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store") // always serve the latest page
	w.Write(indexHTML)
}

// statsResponse is the /api/stats body: one entry per table plus totals, so the
// page can show both the breakdown and headline numbers.
type statsResponse struct {
	Tables     []sink.TableStat `json:"tables"`
	TotalRows  uint64           `json:"total_rows"`
	TotalBytes uint64           `json:"total_bytes"`
	ServerTime string           `json:"server_time"`
}

func (h *handler) buildStats(ctx context.Context) (statsResponse, error) {
	tables, err := h.store.TableStats(ctx)
	if err != nil {
		return statsResponse{}, err
	}
	resp := statsResponse{Tables: tables, ServerTime: time.Now().UTC().Format("15:04:05")}
	for _, t := range tables {
		resp.TotalRows += t.Rows
		resp.TotalBytes += t.Bytes
	}
	return resp, nil
}

func (h *handler) stats(w http.ResponseWriter, r *http.Request) {
	resp, err := h.buildStats(r.Context())
	if err != nil {
		h.fail(w, "stats", err)
		return
	}
	writeJSON(w, resp)
}

// stream is the live push channel: one Server-Sent Events connection over which
// the server sends the current quotes and books, new tape trades, and (less
// often) storage stats. It replaces per-endpoint polling — the browser holds
// one connection instead of hammering four endpoints on timers.
func (h *handler) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // don't let any proxy buffer the stream

	ctx := r.Context()
	ticker := time.NewTicker(streamInterval)
	defer ticker.Stop()

	// Seed the tape cursor near the end so a new client gets the last few trades,
	// not the whole ring.
	cursor := h.reg.LatestTradeID()
	if cursor > 25 {
		cursor -= 25
	} else {
		cursor = 0
	}

	for tick := 0; ; tick++ {
		payload := map[string]any{
			"quotes": h.reg.Quotes(),
			"depth":  h.reg.AllDepth(),
		}
		var trades []live.TradeRow
		trades, cursor = h.reg.TradesSince(cursor, 100)
		if len(trades) > 0 {
			payload["trades"] = trades
		}
		// Stats and latency percentiles come from ClickHouse; refresh them every
		// ~2s, not every tick. The first tick (0) includes them, so a fresh client
		// gets a populated chart immediately.
		if tick%4 == 0 {
			if s, err := h.buildStats(ctx); err == nil {
				payload["stats"] = s
			}
			if lat, err := h.store.LatencyHistory(ctx, 180, 3); err == nil {
				payload["latency"] = lat
			}
			if rate, err := h.store.ThroughputHistory(ctx, 180, 3); err == nil {
				payload["throughput"] = rate
			}
		}
		b, err := json.Marshal(payload)
		if err == nil {
			if _, werr := fmt.Fprintf(w, "data: %s\n\n", b); werr != nil {
				return // client went away
			}
			flusher.Flush()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// tape serves new trades (the time & sales tape) since the client's cursor.
// Trades are low volume, so this scrolls comfortably straight from ClickHouse.
func (h *handler) tape(w http.ResponseWriter, r *http.Request) {
	trades, err := h.store.RecentTrades(r.Context(), queryInt(r, "ts"), feedLimit)
	if err != nil {
		h.fail(w, "trade tape", err)
		return
	}
	writeJSON(w, map[string]any{"trades": trades})
}

// book serves the cross-venue quote grid: the current best bid/ask per market,
// from live in-memory state (so it stays current regardless of feed volume).
func (h *handler) book(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"quotes": h.reg.Quotes()})
}

// depthResponse is one market's order-book ladder.
type depthResponse struct {
	Venue  string          `json:"venue"`
	Symbol string          `json:"symbol"`
	Bids   []live.DepthRow `json:"bids"`
	Asks   []live.DepthRow `json:"asks"`
	Found  bool            `json:"found"`
}

// depth serves the ladder for one market, selected by ?venue=&symbol=.
func (h *handler) depth(w http.ResponseWriter, r *http.Request) {
	venue := r.URL.Query().Get("venue")
	symbol := r.URL.Query().Get("symbol")
	bids, asks, ok := h.reg.Depth(venue, symbol)
	writeJSON(w, depthResponse{Venue: venue, Symbol: symbol, Bids: bids, Asks: asks, Found: ok})
}

// priceHistory returns the last ~3 minutes of per-venue BTC prices from
// ClickHouse, so the dashboard's price chart loads already populated.
func (h *handler) priceHistory(w http.ResponseWriter, r *http.Request) {
	pts, err := h.store.PriceHistory(r.Context(), 180, 2)
	if err != nil {
		h.fail(w, "price history", err)
		return
	}
	writeJSON(w, map[string]any{"points": pts})
}

func (h *handler) fail(w http.ResponseWriter, what string, err error) {
	h.log.Error("dashboard query failed", "what", what, "error", err)
	http.Error(w, "query failed", http.StatusInternalServerError)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// queryInt reads a non-negative int64 query param, defaulting to 0 when absent
// or malformed (0 means "give me the most recent rows").
func queryInt(r *http.Request, key string) int64 {
	n, err := strconv.ParseInt(r.URL.Query().Get(key), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
