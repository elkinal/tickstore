package kraken

import (
	"hash/crc32"
	"strconv"
	"strings"

	"github.com/elkinal/tickstore/internal/norm"
)

// pow10[i] == 10^i, for i in [0, 8] (the fixed-point scale range).
var pow10 = [...]int64{
	1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000,
}

// checksumField renders a fixed-point value (scale = norm.PriceDecimals) at the
// venue's display precision, with the decimal point and leading zeros removed —
// the exact form Kraken's book checksum concatenates. Computed by integer
// division from our int64, so no float is involved.
func checksumField(v int64, precision int) string {
	return strconv.FormatInt(v/pow10[norm.PriceDecimals-precision], 10)
}

// bookChecksum computes Kraken's CRC32 book checksum: for the top 10 asks
// (ascending) then the top 10 bids (descending), concatenate each level's price
// then qty via checksumField, and CRC32 (IEEE) the result. bids must be sorted
// descending and asks ascending (as Book.Depth returns them).
func bookChecksum(bids, asks []norm.Level, pricePrec, qtyPrec int) uint32 {
	var sb strings.Builder
	appendSide := func(levels []norm.Level) {
		for i := 0; i < len(levels) && i < 10; i++ {
			sb.WriteString(checksumField(levels[i].Price, pricePrec))
			sb.WriteString(checksumField(levels[i].Size, qtyPrec))
		}
	}
	appendSide(asks)
	appendSide(bids)
	return crc32.ChecksumIEEE([]byte(sb.String()))
}
