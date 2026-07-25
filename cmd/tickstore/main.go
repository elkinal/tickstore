// Command tickstore runs the market data engine.
//
// It connects to Coinbase and either prints each trade to stdout or batches it
// into ClickHouse, depending on -clickhouse. More venues, the order book, and
// config come in later milestones.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/elkinal/tickstore/internal/book"
	"github.com/elkinal/tickstore/internal/config"
	"github.com/elkinal/tickstore/internal/metrics"
	"github.com/elkinal/tickstore/internal/norm"
	"github.com/elkinal/tickstore/internal/sink"
	"github.com/elkinal/tickstore/internal/venue"
	"github.com/elkinal/tickstore/internal/venue/coinbase"
	"github.com/elkinal/tickstore/internal/venue/kraken"
	"github.com/elkinal/tickstore/internal/venue/okx"
)

// shutdownTimeout bounds the final sink flush so a wedged ClickHouse can't hang
// exit forever.
const shutdownTimeout = 10 * time.Second

// stdoutHandler prints each trade as one line.
type stdoutHandler struct{}

// OnTrade prints one trade.
func (stdoutHandler) OnTrade(t norm.Trade) {
	latency := t.TsReceived.Sub(t.TsExchange).Round(time.Microsecond)
	fmt.Printf("%s %s %s %s %s @ %s trade_id=%s latency=%s\n",
		t.TsExchange.Format("15:04:05.000000"),
		t.Venue,
		t.Symbol,
		t.Side,
		norm.FormatFixed(t.Size, norm.SizeDecimals),
		norm.FormatFixed(t.Price, norm.PriceDecimals),
		t.TradeID,
		latency,
	)
}

// tradeSink adapts a trade Batcher to venue.Handler: it records per-trade
// metrics (the Batcher itself is metric-agnostic) and queues the row.
type tradeSink struct{ b *sink.Batcher[norm.Trade] }

// OnTrade records metrics and enqueues the trade for batched insertion.
func (s tradeSink) OnTrade(t norm.Trade) {
	metrics.Trades.WithLabelValues(t.Venue).Inc()
	metrics.E2ELatencySeconds.WithLabelValues(t.Venue).
		Observe(t.TsReceived.Sub(t.TsExchange).Seconds())
	s.b.Add(t)
}

// bookSink adapts a book Batcher to venue.BookSink: it queues each L2 change for
// batched insertion into book_updates.
type bookSink struct {
	b *sink.Batcher[norm.BookUpdate]
}

// OnBookUpdate enqueues one book-level change.
func (s bookSink) OnBookUpdate(u norm.BookUpdate) { s.b.Add(u) }

func main() {
	symbolsFlag := flag.String("symbols", "BTC-USD,ETH-USD",
		"comma-separated Coinbase product ids")
	chAddr := flag.String("clickhouse", "",
		"ClickHouse host:port; if empty, trades are printed to stdout")
	chUser := flag.String("clickhouse-user", "tickstore", "ClickHouse username")
	chPass := flag.String("clickhouse-password", "tickstore", "ClickHouse password")
	chDB := flag.String("clickhouse-db", "tickstore", "ClickHouse database")
	bookMode := flag.Bool("book", false,
		"stream L2 order books and print top-of-book, instead of trades")
	venueName := flag.String("venue", "coinbase", "venue: coinbase, kraken, or okx")
	configPath := flag.String("config", "",
		"YAML config path; runs every listed venue into the sink (overrides the single-venue flags)")
	flag.Parse()
	symbols := strings.Split(*symbolsFlag, ",")

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *configPath != "" {
		runConfig(ctx, *configPath, log)
		return
	}

	if *bookMode {
		runBookMode(ctx, *venueName, symbols, log)
		return
	}

	conn, err := tradeConnector(*venueName, symbols, log)
	if err != nil {
		log.Error("startup failed", "error", err)
		os.Exit(1)
	}

	// Pick the destination for trades. onShutdown flushes it, if needed.
	chCfg := sink.ClickHouseConfig{
		Addr: *chAddr, Database: *chDB, Username: *chUser, Password: *chPass,
	}
	handler, ch, onShutdown, err := buildHandler(ctx, chCfg, sink.Config{Logger: log}, log)
	if err != nil {
		log.Error("startup failed", "error", err)
		os.Exit(1)
	}

	log.Info("starting", "venue", conn.Name(), "symbols", symbols)
	runErr := conn.Run(ctx, handler)

	if onShutdown != nil {
		onShutdown()
	}
	if ch != nil {
		ch.Close()
	}
	if runErr != nil && ctx.Err() == nil {
		log.Error("venue failed", "error", runErr)
		os.Exit(1)
	}
	log.Info("shut down cleanly")
}

// runConfig loads the config and runs every listed venue concurrently, all
// feeding one shared sink. Each venue runs in its own goroutine so one venue
// failing can't take down the others.
func runConfig(ctx context.Context, path string, log *slog.Logger) {
	cfg, err := config.Load(path)
	if err != nil {
		log.Error("config", "error", err)
		os.Exit(1)
	}

	chCfg := sink.ClickHouseConfig{
		Addr:     cfg.ClickHouse.Addr,
		Database: cfg.ClickHouse.Database,
		Username: cfg.ClickHouse.Username,
		Password: cfg.ClickHouse.Password,
	}
	sinkCfg := sink.Config{
		MaxRows:  cfg.Sink.MaxRows,
		MaxDelay: time.Duration(cfg.Sink.MaxDelay),
		Buffer:   cfg.Sink.Buffer,
	}
	handler, ch, onShutdown, err := buildHandler(ctx, chCfg, sinkCfg, log)
	if err != nil {
		log.Error("startup failed", "error", err)
		os.Exit(1)
	}

	// Optionally persist order books ("the firehose"): a second batcher on the
	// same connection, feeding book connectors alongside the trade ones.
	var books venue.BookSink
	var flushBooks func()
	if cfg.PersistBooks && ch != nil {
		sinkCfg.Logger = log
		bookBatcher := sink.NewBatcher[norm.BookUpdate](ch.Books(), sinkCfg)
		books = bookSink{bookBatcher}
		flushBooks = func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			if err := bookBatcher.Close(shutdownCtx); err != nil {
				log.Error("book sink did not flush cleanly", "error", err)
			}
		}
		log.Info("persisting order books to clickhouse")
	}

	go metrics.Serve(ctx, cfg.Metrics.Addr, log)

	// Build every connector before starting, so a bad venue name fails fast.
	conns := make([]venue.Venue, 0, len(cfg.Venues))
	for _, v := range cfg.Venues {
		conn, err := tradeConnector(v.Name, v.Symbols, log)
		if err != nil {
			log.Error("startup failed", "error", err)
			os.Exit(1)
		}
		conns = append(conns, conn)
	}
	// Book connectors run independently of the trade handler, each streaming its
	// venue's L2 book into the shared book sink.
	var bookConns []bookRunner
	if books != nil {
		for _, v := range cfg.Venues {
			bc, err := bookConnector(v.Name, v.Symbols, books, log)
			if err != nil {
				log.Error("startup failed", "error", err)
				os.Exit(1)
			}
			bookConns = append(bookConns, bc)
		}
	}

	var wg sync.WaitGroup
	for i, conn := range conns {
		conn := conn
		symbols := cfg.Venues[i].Symbols
		log.Info("starting", "venue", conn.Name(), "symbols", symbols)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := conn.Run(ctx, handler); err != nil && ctx.Err() == nil {
				log.Error("venue failed", "venue", conn.Name(), "error", err)
			}
		}()
	}
	for _, bc := range bookConns {
		bc := bc
		log.Info("starting books", "venue", bc.Name())
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := bc.Run(ctx); err != nil && ctx.Err() == nil {
				log.Error("book venue failed", "venue", bc.Name(), "error", err)
			}
		}()
	}
	wg.Wait()

	// Flush trades and books before closing the shared connection.
	if onShutdown != nil {
		onShutdown()
	}
	if flushBooks != nil {
		flushBooks()
	}
	if ch != nil {
		ch.Close()
	}
	log.Info("shut down cleanly")
}

// bookRunner is a venue's book connector: it streams L2 books until ctx ends.
type bookRunner interface {
	Name() string
	Run(context.Context) error
}

// bookConnector builds the book connector for the named venue, wired to sink.
func bookConnector(name string, symbols []string, sink venue.BookSink, log *slog.Logger) (bookRunner, error) {
	switch name {
	case "coinbase":
		return coinbase.NewBook(symbols, nil, sink, log), nil
	case "kraken":
		return kraken.NewBook(symbols, nil, sink, log), nil
	case "okx":
		return okx.NewBook(symbols, nil, sink, log), nil
	default:
		return nil, fmt.Errorf("unknown venue %q (want coinbase, kraken, or okx)", name)
	}
}

// tradeConnector builds the trade connector for the named venue.
func tradeConnector(name string, symbols []string, log *slog.Logger) (venue.Venue, error) {
	switch name {
	case "coinbase":
		return coinbase.New(symbols, log), nil
	case "kraken":
		return kraken.New(symbols, log), nil
	case "okx":
		return okx.New(symbols, log), nil
	default:
		return nil, fmt.Errorf("unknown venue %q (want coinbase, kraken, or okx)", name)
	}
}

// runBookMode streams L2 order books for the named venue and prints each book's
// top-of-book, throttled to once a second per symbol.
func runBookMode(ctx context.Context, venueName string, symbols []string, log *slog.Logger) {
	printer := &bookPrinter{last: map[string]time.Time{}, every: time.Second}
	var runner interface{ Run(context.Context) error }
	switch venueName {
	case "coinbase":
		runner = coinbase.NewBook(symbols, printer, nil, log)
	case "kraken":
		runner = kraken.NewBook(symbols, printer, nil, log)
	case "okx":
		runner = okx.NewBook(symbols, printer, nil, log)
	default:
		log.Error("unknown venue for book mode", "venue", venueName)
		os.Exit(1)
	}
	log.Info("starting book mode", "venue", venueName, "symbols", symbols)
	if err := runner.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("book feed failed", "error", err)
		os.Exit(1)
	}
	log.Info("shut down cleanly")
}

// bookPrinter prints top-of-book, at most once per `every` per symbol so a busy
// feed doesn't flood the terminal.
type bookPrinter struct {
	last  map[string]time.Time
	every time.Duration
}

// OnBook implements coinbase.BookObserver. It runs on the feed's read loop.
func (p *bookPrinter) OnBook(b *book.Book) {
	now := time.Now()
	if t, ok := p.last[b.Symbol()]; ok && now.Sub(t) < p.every {
		return
	}
	p.last[b.Symbol()] = now

	bid, ask, ok := b.TopOfBook()
	if !ok {
		return
	}
	gaps, resyncs, _ := b.Stats()
	fmt.Printf("%s %s  bid %s x %s | ask %s x %s  spread=%s seq=%d gaps=%d resyncs=%d\n",
		b.Venue(), b.Symbol(),
		norm.FormatFixed(bid.Price, norm.PriceDecimals), norm.FormatFixed(bid.Size, norm.SizeDecimals),
		norm.FormatFixed(ask.Price, norm.PriceDecimals), norm.FormatFixed(ask.Size, norm.SizeDecimals),
		norm.FormatFixed(ask.Price-bid.Price, norm.PriceDecimals),
		b.LastSeq(), gaps, resyncs)
}

// buildHandler returns the trade destination, the ClickHouse handle (nil when
// printing to stdout, so callers can build a book sink on the same connection),
// and a hook that flushes the trade batcher after the connector stops. The
// caller owns closing ch (after every batcher on it has flushed).
func buildHandler(ctx context.Context, cfg sink.ClickHouseConfig, sinkCfg sink.Config, log *slog.Logger) (venue.Handler, *sink.ClickHouse, func(), error) {
	if cfg.Addr == "" {
		return stdoutHandler{}, nil, nil, nil
	}

	ch, err := sink.OpenClickHouse(ctx, cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := ch.Migrate(ctx); err != nil {
		ch.Close()
		return nil, nil, nil, err
	}
	sinkCfg.Logger = log
	batcher := sink.NewBatcher[norm.Trade](ch.Trades(), sinkCfg)
	log.Info("writing trades to clickhouse", "addr", cfg.Addr)

	flush := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := batcher.Close(shutdownCtx); err != nil {
			log.Error("sink did not flush cleanly", "error", err)
		}
	}
	return tradeSink{batcher}, ch, flush, nil
}
