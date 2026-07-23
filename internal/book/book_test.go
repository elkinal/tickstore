package book

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/elkinal/tickstore/internal/norm"
)

// --- reference model ------------------------------------------------------
//
// A plain map-of-maps that applies updates the obvious way. The engine must
// always agree with it. This is the oracle the property tests compare against.

type model struct {
	bids map[int64]int64
	asks map[int64]int64
}

func newModel() *model {
	return &model{bids: map[int64]int64{}, asks: map[int64]int64{}}
}

func (m *model) apply(u norm.BookUpdate) {
	side := m.bids
	if u.Side == norm.Sell {
		side = m.asks
	}
	if u.Size == 0 {
		delete(side, u.Price)
	} else {
		side[u.Price] = u.Size
	}
}

func (m *model) snapshot(seq uint64) norm.BookSnapshot {
	s := norm.BookSnapshot{Seq: seq}
	for p, sz := range m.bids {
		s.Bids = append(s.Bids, norm.Level{Price: p, Size: sz})
	}
	for p, sz := range m.asks {
		s.Asks = append(s.Asks, norm.Level{Price: p, Size: sz})
	}
	return s
}

// assertMatch fails if the engine's levels differ from the model's.
func assertMatch(t *testing.T, b *Book, m *model) {
	t.Helper()
	bids, asks := b.Depth(0)
	wantBids := sortedLevels(m.bids, true)
	wantAsks := sortedLevels(m.asks, false)
	if !reflect.DeepEqual(bids, wantBids) {
		t.Fatalf("bids mismatch:\n got %v\nwant %v", bids, wantBids)
	}
	if !reflect.DeepEqual(asks, wantAsks) {
		t.Fatalf("asks mismatch:\n got %v\nwant %v", asks, wantAsks)
	}
}

// randomStream builds n sequenced updates over a small price range so levels
// collide, get resized, and get removed (size 0).
func randomStream(r *rand.Rand, n int) []norm.BookUpdate {
	ups := make([]norm.BookUpdate, n)
	for i := range ups {
		side := norm.Buy
		if r.Intn(2) == 0 {
			side = norm.Sell
		}
		ups[i] = norm.BookUpdate{
			Side:  side,
			Price: int64(100 + r.Intn(20)),
			Size:  int64(r.Intn(4)), // 0..3, so ~1/4 are removals
			Seq:   uint64(i + 1),
		}
	}
	return ups
}

// --- basic behavior -------------------------------------------------------

func TestApplyInOrder(t *testing.T) {
	b := New("test", "BTC-USD")
	b.ApplySnapshot(norm.BookSnapshot{Seq: 0})

	m := newModel()
	ups := []norm.BookUpdate{
		{Side: norm.Buy, Price: 100, Size: 5, Seq: 1},
		{Side: norm.Sell, Price: 102, Size: 3, Seq: 2},
		{Side: norm.Buy, Price: 101, Size: 2, Seq: 3},
		{Side: norm.Buy, Price: 100, Size: 0, Seq: 4}, // remove the 100 bid
	}
	for _, u := range ups {
		if b.Apply(u) {
			t.Fatalf("seq %d unexpectedly needed a snapshot", u.Seq)
		}
		m.apply(u)
	}
	assertMatch(t, b, m)

	bid, ask, ok := b.TopOfBook()
	if !ok || bid.Price != 101 || ask.Price != 102 {
		t.Fatalf("top of book = %v/%v ok=%v, want 101/102", bid, ask, ok)
	}
}

func TestUnsyncedBuffersUntilSnapshot(t *testing.T) {
	b := New("test", "BTC-USD")
	// No snapshot yet: every update must buffer and ask for one.
	for i := uint64(1); i <= 3; i++ {
		if !b.Apply(norm.BookUpdate{Side: norm.Buy, Price: 100, Size: int64(i), Seq: i}) {
			t.Fatalf("seq %d should have asked for a snapshot", i)
		}
	}
	if b.Synced() {
		t.Fatal("book should be unsynced before any snapshot")
	}
	// Snapshot at seq 0 seeds an empty book; buffered 1..3 replay in order.
	b.ApplySnapshot(norm.BookSnapshot{Seq: 0})
	if !b.Synced() || b.LastSeq() != 3 {
		t.Fatalf("after replay synced=%v lastSeq=%d, want true/3", b.Synced(), b.LastSeq())
	}
	bid, _, ok := b.TopOfBook()
	if ok && bid.Size != 3 {
		t.Fatalf("bid size = %d, want 3 (last update wins)", bid.Size)
	}
}

func TestGapDetectionTriggersResync(t *testing.T) {
	b := New("test", "BTC-USD")
	b.ApplySnapshot(norm.BookSnapshot{Seq: 0})
	b.Apply(norm.BookUpdate{Side: norm.Buy, Price: 100, Size: 1, Seq: 1})

	// Skip seq 2: deliver seq 3 -> gap.
	if !b.Apply(norm.BookUpdate{Side: norm.Buy, Price: 100, Size: 9, Seq: 3}) {
		t.Fatal("gap at seq 3 should have asked for a snapshot")
	}
	if b.Synced() {
		t.Fatal("book should be unsynced after a gap")
	}
	if gaps, _, _ := b.Stats(); gaps != 1 {
		t.Fatalf("gaps = %d, want 1", gaps)
	}
}

func TestDuplicateIgnored(t *testing.T) {
	b := New("test", "BTC-USD")
	b.ApplySnapshot(norm.BookSnapshot{Seq: 5})
	// An update at or before the snapshot seq is already reflected; ignore it.
	if b.Apply(norm.BookUpdate{Side: norm.Buy, Price: 100, Size: 7, Seq: 3}) {
		t.Fatal("old seq should not ask for a snapshot")
	}
	if _, _, ok := b.TopOfBook(); ok {
		t.Fatal("duplicate old update should not have modified the book")
	}
}

func TestDepthOrdering(t *testing.T) {
	b := New("test", "BTC-USD")
	b.ApplySnapshot(norm.BookSnapshot{
		Seq:  0,
		Bids: []norm.Level{{Price: 100, Size: 1}, {Price: 102, Size: 1}, {Price: 101, Size: 1}},
		Asks: []norm.Level{{Price: 105, Size: 1}, {Price: 103, Size: 1}, {Price: 104, Size: 1}},
	})
	bids, asks := b.Depth(2)
	if len(bids) != 2 || bids[0].Price != 102 || bids[1].Price != 101 {
		t.Fatalf("bids = %v, want [102 101] (descending)", bids)
	}
	if len(asks) != 2 || asks[0].Price != 103 || asks[1].Price != 104 {
		t.Fatalf("asks = %v, want [103 104] (ascending)", asks)
	}
}

func TestTrimKeepsBestLevels(t *testing.T) {
	b := New("test", "BTC-USD")
	snap := norm.BookSnapshot{Seq: 0}
	// 15 bids (100..114) and 15 asks (200..214); best bid = 114, best ask = 200.
	for i := int64(0); i < 15; i++ {
		snap.Bids = append(snap.Bids, norm.Level{Price: 100 + i, Size: 1})
		snap.Asks = append(snap.Asks, norm.Level{Price: 200 + i, Size: 1})
	}
	b.ApplySnapshot(snap)

	b.Trim(10)
	bids, asks := b.Depth(0) // all remaining
	if len(bids) != 10 || len(asks) != 10 {
		t.Fatalf("after Trim(10): %d bids, %d asks, want 10 each", len(bids), len(asks))
	}
	// Best 10 bids are 114..105; best 10 asks are 200..209.
	if bids[0].Price != 114 || bids[9].Price != 105 {
		t.Fatalf("trimmed bids kept wrong levels: top=%d bottom=%d", bids[0].Price, bids[9].Price)
	}
	if asks[0].Price != 200 || asks[9].Price != 209 {
		t.Fatalf("trimmed asks kept wrong levels: top=%d bottom=%d", asks[0].Price, asks[9].Price)
	}
}

// --- property tests -------------------------------------------------------

// TestReplayConvergesFromShuffle feeds a whole sequenced stream out of order
// before any snapshot. All updates buffer; the snapshot's replay sorts them, so
// the book must converge to the same state as applying them in order.
func TestReplayConvergesFromShuffle(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for trial := 0; trial < 50; trial++ {
		ups := randomStream(r, 200)

		ref := newModel()
		for _, u := range ups {
			ref.apply(u)
		}

		b := New("test", "BTC-USD")
		shuffled := append([]norm.BookUpdate(nil), ups...)
		r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		for _, u := range shuffled {
			b.Apply(u) // all buffer: unsynced until the snapshot below
		}
		b.ApplySnapshot(norm.BookSnapshot{Seq: 0}) // empty seed, replay does the work

		assertMatch(t, b, ref)
		if b.LastSeq() != uint64(len(ups)) {
			t.Fatalf("lastSeq = %d, want %d", b.LastSeq(), len(ups))
		}
	}
}

// TestResyncConvergesAfterGap is the core property: apply a prefix, drop a run
// of updates to force a gap, buffer the rest, then resync with a snapshot taken
// at the true state just before the buffered tail. The book must converge to
// the full in-order state.
func TestResyncConvergesAfterGap(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for trial := 0; trial < 50; trial++ {
		const n = 300
		ups := randomStream(r, n)

		ref := newModel()
		for _, u := range ups {
			ref.apply(u)
		}

		// Choose a prefix end and a gap end: 1..g applied, g+1..s dropped,
		// s+1..n delivered (gap), then resync with a snapshot at seq s.
		g := 1 + r.Intn(n/3)
		s := g + 1 + r.Intn(n/3) // snapshot seq, inside the dropped run

		b := New("test", "BTC-USD")
		b.ApplySnapshot(norm.BookSnapshot{Seq: 0})
		for _, u := range ups[:g] {
			if b.Apply(u) {
				t.Fatalf("trial %d: prefix seq %d unexpectedly needed snapshot", trial, u.Seq)
			}
		}
		// Deliver the tail after the dropped run; the first one is a gap.
		sawGap := false
		for _, u := range ups[s:] {
			if b.Apply(u) {
				sawGap = true
			}
		}
		if !sawGap {
			t.Fatalf("trial %d: expected a gap after dropping %d..%d", trial, g+1, s)
		}

		// Snapshot representing the true state after applying 1..s.
		snapModel := newModel()
		for _, u := range ups[:s] {
			snapModel.apply(u)
		}
		b.ApplySnapshot(snapModel.snapshot(uint64(s)))

		assertMatch(t, b, ref)
		if !b.Synced() || b.LastSeq() != n {
			t.Fatalf("trial %d: synced=%v lastSeq=%d, want true/%d", trial, b.Synced(), b.LastSeq(), n)
		}
		if gaps, resyncs, _ := b.Stats(); gaps < 1 || resyncs < 1 {
			t.Fatalf("trial %d: gaps=%d resyncs=%d, want >=1 each", trial, gaps, resyncs)
		}
	}
}
