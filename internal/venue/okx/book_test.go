package okx

import "testing"

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
