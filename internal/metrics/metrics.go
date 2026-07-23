// Package metrics defines the Prometheus collectors tickstore exposes. They are
// package globals (the idiomatic Prometheus pattern) so any package can record
// to them without threading a metrics object through every call.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Messages counts frames received per venue.
	Messages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tickstore_messages_total",
		Help: "WebSocket frames received, per venue.",
	}, []string{"venue"})

	// ParseErrors counts frames that failed to parse, per venue.
	ParseErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tickstore_parse_errors_total",
		Help: "Frames that failed to parse, per venue.",
	}, []string{"venue"})

	// Trades counts normalized trades produced, per venue.
	Trades = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tickstore_trades_total",
		Help: "Normalized trades produced, per venue.",
	}, []string{"venue"})

	// Gaps counts detected order-book sequence gaps / checksum mismatches.
	Gaps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tickstore_book_gaps_total",
		Help: "Order book gaps detected (bad checksum or sequence break), per venue.",
	}, []string{"venue"})

	// Resyncs counts order-book resyncs triggered by a gap.
	Resyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tickstore_book_resyncs_total",
		Help: "Order book resyncs triggered by a gap, per venue.",
	}, []string{"venue"})

	// SinkBatchRows is the distribution of rows per ClickHouse insert.
	SinkBatchRows = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "tickstore_sink_batch_rows",
		Help:    "Rows per ClickHouse insert.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 16), // 1 .. ~32k
	})

	// SinkFlushSeconds is the distribution of insert latency.
	SinkFlushSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "tickstore_sink_flush_seconds",
		Help:    "ClickHouse insert latency, seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// E2ELatencySeconds is the distribution of ts_received - ts_exchange, per
	// venue. Sensitive to local clock skew vs. the exchange; read as relative.
	E2ELatencySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "tickstore_e2e_latency_seconds",
		Help:    "Exchange-to-received latency (ts_received - ts_exchange), per venue.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"venue"})
)
