package venue

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"
)

// Backoff bounds for reconnect. Exported so connectors can reference them.
const (
	BackoffMin = time.Second
	BackoffMax = time.Minute
)

// Session runs one connect-and-stream cycle. It reports whether it received any
// data (used to reset the backoff) and the error that ended it.
type Session func(ctx context.Context) (gotData bool, err error)

// RunWithReconnect calls session in a loop until ctx is canceled, reconnecting
// with exponential backoff plus full jitter. Backoff resets after a session
// receives data. It returns ctx.Err() on cancellation.
func RunWithReconnect(ctx context.Context, log *slog.Logger, session Session) error {
	backoff := BackoffMin
	for {
		gotData, err := session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if gotData {
			backoff = BackoffMin
		}
		sleep := rand.N(backoff) // full jitter, avoids synchronized reconnects
		log.Warn("session ended, reconnecting",
			"error", err, "sleep", sleep.Round(time.Millisecond))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
		if backoff *= 2; backoff > BackoffMax {
			backoff = BackoffMax
		}
	}
}
