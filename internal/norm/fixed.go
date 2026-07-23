package norm

import (
	"fmt"
	"strings"
)

// pow10 holds 10^i for i in [0, 18], every power of ten representable in int64.
var pow10 = [19]int64{
	1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000,
	1_000_000_000, 10_000_000_000, 100_000_000_000, 1_000_000_000_000,
	10_000_000_000_000, 100_000_000_000_000, 1_000_000_000_000_000,
	10_000_000_000_000_000, 100_000_000_000_000_000, 1_000_000_000_000_000_000,
}

// ParseFixed parses a decimal string such as "50123.45" into a fixed-point
// int64 with dp decimal places, without going through float64.
//
// Extra fractional digits are accepted only if they are zeros; nonzero
// digits beyond dp are an error rather than a silent precision loss.
// dp must be in [0, 18]. The representable range is symmetric,
// ±(1<<63 - 1); the lone value MinInt64 is rejected as overflow.
func ParseFixed(s string, dp int) (int64, error) {
	if dp < 0 || dp > 18 {
		return 0, fmt.Errorf("norm: decimal places %d out of range [0, 18]", dp)
	}
	orig := s
	neg := false
	switch {
	case strings.HasPrefix(s, "-"):
		neg = true
		s = s[1:]
	case strings.HasPrefix(s, "+"):
		s = s[1:]
	}
	intPart, fracPart, _ := strings.Cut(s, ".")
	if intPart == "" && fracPart == "" {
		return 0, fmt.Errorf("norm: empty number %q", orig)
	}
	// Nonzero digits beyond dp would be silently dropped; refuse them.
	if len(fracPart) > dp {
		if strings.TrimRight(fracPart[dp:], "0") != "" {
			return 0, fmt.Errorf("norm: %q has more than %d nonzero decimal places", orig, dp)
		}
		fracPart = fracPart[:dp]
	}
	var v int64
	digits := 0
	for _, part := range [2]string{intPart, fracPart} {
		for _, c := range []byte(part) {
			if c < '0' || c > '9' {
				return 0, fmt.Errorf("norm: invalid character %q in number %q", c, orig)
			}
			if digits++; digits > 19 {
				return 0, fmt.Errorf("norm: %q overflows int64 at %d decimal places", orig, dp)
			}
			d := int64(c - '0')
			if v > (1<<63-1-d)/10 {
				return 0, fmt.Errorf("norm: %q overflows int64 at %d decimal places", orig, dp)
			}
			v = v*10 + d
		}
	}
	// Scale up for fractional digits the input did not provide.
	if pad := dp - len(fracPart); pad > 0 {
		if v > (1<<63-1)/pow10[pad] {
			return 0, fmt.Errorf("norm: %q overflows int64 at %d decimal places", orig, dp)
		}
		v *= pow10[pad]
	}
	if neg {
		v = -v
	}
	return v, nil
}

// FormatFixed renders a fixed-point int64 with dp decimal places back into
// a decimal string, e.g. FormatFixed(5012345000000, 8) == "50123.45000000".
// The fractional part is always dp digits wide. dp must be in [0, 18].
func FormatFixed(v int64, dp int) string {
	if dp < 0 || dp > 18 {
		return fmt.Sprintf("!norm: bad decimal places %d", dp)
	}
	sign := ""
	uv := uint64(v)
	if v < 0 {
		sign = "-"
		uv = -uv // two's complement: correct even for MinInt64
	}
	if dp == 0 {
		return fmt.Sprintf("%s%d", sign, uv)
	}
	scale := uint64(pow10[dp])
	return fmt.Sprintf("%s%d.%0*d", sign, uv/scale, dp, uv%scale)
}
