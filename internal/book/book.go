// Package book reconstructs and maintains L2 order books from normalized feeds.
//
// A Book holds one side-by-side view for one symbol on one venue: the resting
// size at every price. It applies incremental updates in sequence order,
// detects gaps (missed updates) by sequence number, and recovers by taking a
// fresh snapshot and replaying the updates buffered since.
package book

import (
	"sort"

	"github.com/elkinal/tickstore/internal/norm"
)

// maxPending caps how many updates are buffered while a book waits for a
// snapshot. A snapshot supersedes older updates anyway, so past the cap we drop
// the oldest rather than grow without bound during a long outage.
const maxPending = 100_000

// Book is one venue+symbol order book. It is not safe for concurrent use; drive
// it from a single goroutine (one per book).
type Book struct {
	venue  string
	symbol string

	bids map[int64]int64 // price -> size
	asks map[int64]int64

	lastSeq uint64 // sequence of the last applied update (or snapshot)
	synced  bool   // true when seeded by a snapshot and no gap is outstanding

	pending []norm.BookUpdate // buffered while unsynced, replayed after a snapshot

	gaps    uint64 // sequence gaps detected
	resyncs uint64 // snapshots applied to recover from a gap
	dropped uint64 // buffered updates discarded at the pending cap
}

// New returns an empty, unsynced Book. It needs a snapshot before it can apply
// updates; feed updates through Apply and they'll be buffered until then.
func New(venue, symbol string) *Book {
	return &Book{
		venue:  venue,
		symbol: symbol,
		bids:   make(map[int64]int64),
		asks:   make(map[int64]int64),
	}
}

// Apply incorporates one update and reports whether the book now needs a
// snapshot to make progress — because it has none yet, or a sequence gap was
// found. While in that state updates are buffered and replayed by the next
// ApplySnapshot. The caller should trigger exactly one snapshot per true.
func (b *Book) Apply(u norm.BookUpdate) (needsSnapshot bool) {
	if !b.synced {
		b.buffer(u)
		return true
	}
	expected := b.lastSeq + 1
	switch {
	case u.Seq < expected:
		return false // already have it; a duplicate or a late replay
	case u.Seq > expected:
		b.gaps++
		b.synced = false
		b.buffer(u)
		return true
	default:
		b.applyDelta(u.Side, u.Price, u.Size)
		b.lastSeq = u.Seq
		return false
	}
}

// ApplySnapshot resets the book to the snapshot's state, then replays any
// buffered updates newer than it. If that replay hits its own gap the book goes
// unsynced again and asks for another snapshot.
func (b *Book) ApplySnapshot(s norm.BookSnapshot) {
	if !b.synced && b.lastSeq != 0 {
		b.resyncs++ // recovering from a gap, not the initial seeding
	}
	b.bids = make(map[int64]int64, len(s.Bids))
	b.asks = make(map[int64]int64, len(s.Asks))
	for _, l := range s.Bids {
		if l.Size != 0 {
			b.bids[l.Price] = l.Size
		}
	}
	for _, l := range s.Asks {
		if l.Size != 0 {
			b.asks[l.Price] = l.Size
		}
	}
	b.lastSeq = s.Seq
	b.synced = true

	// Replay buffered updates in sequence order. Anything at or before the
	// snapshot is already reflected, so it's skipped by Apply's seq check.
	pending := b.pending
	b.pending = nil
	sort.Slice(pending, func(i, j int) bool { return pending[i].Seq < pending[j].Seq })
	for _, u := range pending {
		b.Apply(u)
	}
}

// buffer appends an update to the replay buffer, dropping the oldest if the cap
// is reached (a coming snapshot will cover them anyway).
func (b *Book) buffer(u norm.BookUpdate) {
	if len(b.pending) >= maxPending {
		b.pending = b.pending[1:]
		b.dropped++
	}
	b.pending = append(b.pending, u)
}

// applyDelta sets or clears one price level. Size zero removes the level.
func (b *Book) applyDelta(side norm.Side, price, size int64) {
	m := b.bids
	if side == norm.Sell {
		m = b.asks
	}
	if size == 0 {
		delete(m, price)
	} else {
		m[price] = size
	}
}

// Venue and Symbol identify the book.
func (b *Book) Venue() string  { return b.venue }
func (b *Book) Symbol() string { return b.symbol }

// Synced reports whether the book is seeded and gap-free.
func (b *Book) Synced() bool { return b.synced }

// LastSeq is the sequence number of the most recently applied update.
func (b *Book) LastSeq() uint64 { return b.lastSeq }

// Stats returns the running counters for gaps, resyncs, and dropped updates.
func (b *Book) Stats() (gaps, resyncs, dropped uint64) {
	return b.gaps, b.resyncs, b.dropped
}
