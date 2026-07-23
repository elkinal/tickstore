package norm

import (
	"fmt"
	"strings"
)

// pow10[i] is 10^i, for every power of ten that fits in an int64.
var pow10 = [19]int64{
	1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000,
	1_000_000_000, 10_000_000_000, 100_000_000_000, 1_000_000_000_000,
	10_000_000_000_000, 100_000_000_000_000, 1_000_000_000_000_000,
	10_000_000_000_000_000, 100_000_000_000_000_000, 1_000_000_000_000_000_000,
}

// ParseFixed turns a decimal string like "50123.45" into a fixed-point int64
// scaled to dp decimal places, without ever using float64. So "50123.45" at
// dp=2 gives 5012345.
//
// Digits past dp are only allowed if they're zeros; anything else is an error,
// not a silent round-off. dp must be 0..18. The valid range is ±(1<<63 - 1),
// so plain MinInt64 counts as overflow.
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
	// Reject junk characters up front, so the checks below can assume digits
	// and their errors say what's actually wrong.
	if err := checkDigits(intPart, orig); err != nil {
		return 0, err
	}
	if err := checkDigits(fracPart, orig); err != nil {
		return 0, err
	}
	// Drop fractional digits past dp, but only if they're zeros. A nonzero
	// digit there is real precision we'd be throwing away, so error instead.
	if len(fracPart) > dp {
		if strings.TrimRight(fracPart[dp:], "0") != "" {
			return 0, fmt.Errorf("norm: %q has more than %d nonzero decimal places", orig, dp)
		}
		fracPart = fracPart[:dp]
	}
	// Read every digit into one integer, checking for overflow before each step.
	var v int64
	for _, part := range [2]string{intPart, fracPart} {
		for i := 0; i < len(part); i++ {
			d := int64(part[i] - '0')
			if v > (1<<63-1-d)/10 {
				return 0, fmt.Errorf("norm: %q overflows int64 at %d decimal places", orig, dp)
			}
			v = v*10 + d
		}
	}
	// Pad with zeros for any decimal places the input left off.
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

// checkDigits returns an error if s holds anything but ASCII digits. orig is
// the original input, used only for the error message.
func checkDigits(s, orig string) error {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return fmt.Errorf("norm: invalid character %q in number %q", s[i], orig)
		}
	}
	return nil
}

// FormatFixed is the inverse of ParseFixed: it turns a fixed-point int64 back
// into a decimal string, e.g. FormatFixed(5012345000000, 8) is "50123.45000000".
// The fractional part is always dp digits.
//
// dp must be 0..18. It's always a compile-time constant at call sites, so an
// out-of-range value is a programmer bug, and this panics rather than return a
// bogus string that could slip into output unnoticed.
func FormatFixed(v int64, dp int) string {
	if dp < 0 || dp > 18 {
		panic(fmt.Sprintf("norm: FormatFixed decimal places %d out of range [0, 18]", dp))
	}
	sign := ""
	uv := uint64(v)
	if v < 0 {
		sign = "-"
		uv = -uv // negate in unsigned space so MinInt64 doesn't overflow
	}
	if dp == 0 {
		return fmt.Sprintf("%s%d", sign, uv)
	}
	scale := uint64(pow10[dp])
	return fmt.Sprintf("%s%d.%0*d", sign, uv/scale, dp, uv%scale)
}
