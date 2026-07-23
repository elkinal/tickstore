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
	"syscall"
	"time"

	"github.com/elkinal/tickstore/internal/book"
	"github.com/elkinal/tickstore/internal/norm"
	"github.com/elkinal/tickstore/internal/sink"
	"github.com/elkinal/tickstore/internal/venue"
	"github.com/elkinal/tickstore/internal/venue/coinbase"
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
	flag.Parse()
	symbols := strings.Split(*symbolsFlag, ",")

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *bookMode {
		runBookMode(ctx, symbols, log)
		return
	}

	// Pick the destination for trades. onShutdown flushes it, if needed.
	chCfg := sink.ClickHouseConfig{
		Addr: *chAddr, Database: *chDB, Username: *chUser, Password: *chPass,
	}
	handler, onShutdown, err := buildHandler(ctx, chCfg, log)
	if err != nil {
		log.Error("startup failed", "error", err)
		os.Exit(1)
	}

	log.Info("starting", "symbols", symbols)
	runErr := coinbase.New(symbols, log).Run(ctx, handler)

	if onShutdown != nil {
		onShutdown()
	}
	if runErr != nil && ctx.Err() == nil {
		log.Error("venue failed", "error", runErr)
		os.Exit(1)
	}
	log.Info("shut down cleanly")
}

// runBookMode streams L2 order books and prints each book's top-of-book,
// throttled to once a second per symbol.
func runBookMode(ctx context.Context, symbols []string, log *slog.Logger) {
	printer := &bookPrinter{last: map[string]time.Time{}, every: time.Second}
	log.Info("starting book mode", "symbols", symbols)
	err := coinbase.NewBook(symbols, printer, log).Run(ctx)
	if err != nil && ctx.Err() == nil {
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

// buildHandler returns the trade destination and an optional shutdown hook that
// must run after the connector stops. With no ClickHouse address it's the
// stdout printer and there's nothing to flush.
func buildHandler(ctx context.Context, cfg sink.ClickHouseConfig, log *slog.Logger) (venue.Handler, func(), error) {
	if cfg.Addr == "" {
		return stdoutHandler{}, nil, nil
	}

	ch, err := sink.OpenClickHouse(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	if err := ch.Migrate(ctx); err != nil {
		ch.Close()
		return nil, nil, err
	}
	batcher := sink.NewBatcher(ch, sink.Config{Logger: log})
	log.Info("writing trades to clickhouse", "addr", cfg.Addr)

	flush := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := batcher.Close(shutdownCtx); err != nil {
			log.Error("sink did not flush cleanly", "error", err)
		}
	}
	return batcher, flush, nil
}
