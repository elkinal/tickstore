package okx

import (
	"testing"
	"time"

	"github.com/elkinal/tickstore/internal/norm"
)

// collectSink records every book update it's handed, for asserting emission.
type collectSink struct{ ups []norm.BookUpdate }

func (c *collectSink) OnBookUpdate(u norm.BookUpdate) { c.ups = append(c.ups, u) }

// TestBookEmission checks that applyBook feeds the sink a full snapshot (one
// update per level, IsSnapshot true) and then the delta levels (IsSnapshot
// false), each carrying venue, symbol, and timestamps.
func TestBookEmission(t *testing.T) {
	sink := &collectSink{}
	c := &BookConnector{sink: sink}
	books := map[string]*seqBook{}
	recv := time.Unix(0, 1_700_000_000_000_000_000).UTC()

	snap := &bookData{
		Bids:  [][]string{{"100.0", "1.0", "0", "1"}},
		Asks:  [][]string{{"101.0", "2.0", "0", "1"}},
		Ts:    "1700000000000", // ms since epoch
		SeqID: 10,
	}
	if err := c.applyBook("BTC-USDT", "snapshot", snap, recv, books); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(sink.ups) != 2 {
		t.Fatalf("snapshot emitted %d updates, want 2", len(sink.ups))
	}
	for _, u := range sink.ups {
		if !u.IsSnapshot {
			t.Errorf("snapshot level not flagged IsSnapshot: %+v", u)
		}
		if u.Venue != Name || u.Symbol != "BTC-USDT" || u.TsReceived != recv {
			t.Errorf("snapshot level missing identity fields: %+v", u)
		}
		if u.TsExchange.IsZero() {
			t.Errorf("snapshot level missing ts_exchange: %+v", u)
		}
	}

	sink.ups = nil
	upd := &bookData{
		Bids:      [][]string{{"100.5", "0.5", "0", "1"}},
		Asks:      nil,
		Ts:        "1700000001000",
		SeqID:     11,
		PrevSeqID: 10,
	}
	if err := c.applyBook("BTC-USDT", "update", upd, recv, books); err != nil {
		t.Fatalf("update: %v", err)
	}
	if len(sink.ups) != 1 {
		t.Fatalf("update emitted %d updates, want 1", len(sink.ups))
	}
	if u := sink.ups[0]; u.IsSnapshot || u.Side != norm.Buy || u.Venue != Name {
		t.Errorf("delta update wrong: %+v", u)
	}
}

// TestSeqContiguity checks the prevSeqId gap-detection rule: an update is
// contiguous only when its prevSeqId matches the last applied seqId.
func TestSeqContiguity(t *testing.T) {
	sb := &seqBook{}

	// Before seeding, nothing is contiguous.
	if sb.contiguous(snapshotPrevSeq) {
		t.Fatal("unseeded book should not report contiguous")
	}

	// Seed as a snapshot would: last applied seqId = 100.
	sb.seeded = true
	sb.lastSeqID = 100

	if !sb.contiguous(100) {
		t.Fatal("update with prevSeqId == last seqId should be contiguous")
	}
	if sb.contiguous(99) {
		t.Fatal("update with a stale prevSeqId should be a gap")
	}
	if sb.contiguous(101) {
		t.Fatal("update whose prevSeqId skips ahead should be a gap")
	}
}
