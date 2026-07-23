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
	flag.Parse()
	symbols := strings.Split(*symbolsFlag, ",")

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Pick the destination for trades. onShutdown flushes it, if needed.
	handler, onShutdown, err := buildHandler(ctx, *chAddr, log)
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

// buildHandler returns the trade destination and an optional shutdown hook that
// must run after the connector stops. With no ClickHouse address it's the
// stdout printer and there's nothing to flush.
func buildHandler(ctx context.Context, chAddr string, log *slog.Logger) (venue.Handler, func(), error) {
	if chAddr == "" {
		return stdoutHandler{}, nil, nil
	}

	ch, err := sink.OpenClickHouse(ctx, sink.ClickHouseConfig{Addr: chAddr, Database: "default"})
	if err != nil {
		return nil, nil, err
	}
	if err := ch.Migrate(ctx); err != nil {
		ch.Close()
		return nil, nil, err
	}
	batcher := sink.NewBatcher(ch, sink.Config{Logger: log})
	log.Info("writing trades to clickhouse", "addr", chAddr)

	flush := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := batcher.Close(shutdownCtx); err != nil {
			log.Error("sink did not flush cleanly", "error", err)
		}
	}
	return batcher, flush, nil
}
