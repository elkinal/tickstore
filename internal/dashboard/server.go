// Package dashboard serves a small live web view of the running engine: a
// scrolling trade and order-book feed plus storage stats (row counts and disk
// usage per table, and the live insert rate), all read from ClickHouse.
package dashboard

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/elkinal/tickstore/internal/live"
	"github.com/elkinal/tickstore/internal/sink"
)

// feedLimit caps how many rows one feed poll returns, so a burst (or a fresh
// page loading from an empty cursor) can't pull an unbounded result.
const feedLimit = 200

//go:embed index.html
var indexHTML []byte

// Store is the persistent read side the dashboard needs for storage stats and
// the trade tape. *sink.ClickHouse satisfies it.
type Store interface {
	TableStats(ctx context.Context) ([]sink.TableStat, error)
	RecentTrades(ctx context.Context, sinceNanos int64, limit int) ([]sink.FeedRow, error)
}

// Live is the in-memory read side: current books and last trades, for the BBO
// grid and depth ladder. *live.Registry satisfies it.
type Live interface {
	Quotes() []live.Quote
	Depth(venue, symbol string) (bids, asks []live.DepthRow, ok bool)
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
	mux.HandleFunc("/api/stats", h.stats)
	mux.HandleFunc("/api/tape", h.tape)
	mux.HandleFunc("/api/book", h.book)
	mux.HandleFunc("/api/depth", h.depth)
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

func (h *handler) stats(w http.ResponseWriter, r *http.Request) {
	tables, err := h.store.TableStats(r.Context())
	if err != nil {
		h.fail(w, "stats", err)
		return
	}
	resp := statsResponse{Tables: tables, ServerTime: time.Now().UTC().Format("15:04:05")}
	for _, t := range tables {
		resp.TotalRows += t.Rows
		resp.TotalBytes += t.Bytes
	}
	writeJSON(w, resp)
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
