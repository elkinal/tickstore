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

	"github.com/elkinal/tickstore/internal/sink"
)

// feedLimit caps how many rows one feed poll returns, so a burst (or a fresh
// page loading from an empty cursor) can't pull an unbounded result.
const feedLimit = 200

//go:embed index.html
var indexHTML []byte

// Store is the read side the dashboard needs. *sink.ClickHouse satisfies it.
type Store interface {
	TableStats(ctx context.Context) ([]sink.TableStat, error)
	RecentTrades(ctx context.Context, sinceNanos int64, limit int) ([]sink.FeedRow, error)
	RecentBookUpdates(ctx context.Context, sinceNanos int64, limit int) ([]sink.FeedRow, error)
}

// Serve runs the dashboard HTTP server at addr until ctx is canceled. A blank
// addr is a no-op, so the dashboard is opt-in via config.
func Serve(ctx context.Context, addr string, store Store, log *slog.Logger) {
	if addr == "" {
		return
	}
	h := &handler{store: store, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("/", h.index)
	mux.HandleFunc("/api/stats", h.stats)
	mux.HandleFunc("/api/feed", h.feed)
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
	log   *slog.Logger
}

func (h *handler) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
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

// feedResponse carries new rows for both streams since the client's cursors.
type feedResponse struct {
	Trades []sink.FeedRow `json:"trades"`
	Book   []sink.FeedRow `json:"book"`
}

func (h *handler) feed(w http.ResponseWriter, r *http.Request) {
	tradesSince := queryInt(r, "ts")
	bookSince := queryInt(r, "bs")

	trades, err := h.store.RecentTrades(r.Context(), tradesSince, feedLimit)
	if err != nil {
		h.fail(w, "trades feed", err)
		return
	}
	book, err := h.store.RecentBookUpdates(r.Context(), bookSince, feedLimit)
	if err != nil {
		h.fail(w, "book feed", err)
		return
	}
	writeJSON(w, feedResponse{Trades: trades, Book: book})
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
