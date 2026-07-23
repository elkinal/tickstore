// Command tickstore runs the market data engine.
//
// Milestone 1 scope: connect to Coinbase, print normalized trades to
// stdout, one per line. YAML config, more venues, the book engine and the
// ClickHouse sink arrive in later milestones.
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
	"github.com/elkinal/tickstore/internal/venue/coinbase"
)

// stdoutHandler prints each normalized trade as one line to stdout.
type stdoutHandler struct{}

// OnTrade implements venue.Handler.
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
	flag.Parse()
	symbols := strings.Split(*symbolsFlag, ",")

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("starting", "symbols", symbols)
	err := coinbase.New(symbols, log).Run(ctx, stdoutHandler{})
	if err != nil && ctx.Err() == nil {
		log.Error("venue failed", "error", err)
		os.Exit(1)
	}
	log.Info("shut down cleanly")
}
