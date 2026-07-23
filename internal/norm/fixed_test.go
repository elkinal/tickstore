package norm

import (
	"math"
	"strings"
	"testing"
)

func TestParseFixed(t *testing.T) {
	tests := []struct {
		name string
		in   string
		dp   int
		want int64
		// errContains, when set, requires an error whose message contains it.
		// A plain wantErr==true (errContains=="") only asserts that it errored.
		wantErr     bool
		errContains string
	}{
		{name: "integer", in: "50123", dp: 8, want: 5_012_300_000_000},
		{name: "typical price", in: "50123.45", dp: 8, want: 5_012_345_000_000},
		{name: "full precision", in: "0.00000001", dp: 8, want: 1},
		{name: "zero", in: "0", dp: 8, want: 0},
		{name: "zero decimal places", in: "42", dp: 0, want: 42},
		{name: "negative", in: "-1.5", dp: 2, want: -150},
		{name: "explicit plus", in: "+1.5", dp: 2, want: 150},
		{name: "leading dot", in: ".25", dp: 2, want: 25},
		{name: "trailing dot", in: "25.", dp: 2, want: 2500},
		{name: "extra zero decimals ok", in: "1.230000000000", dp: 2, want: 123},
		{name: "leading zeros", in: "00000000000000000000123", dp: 0, want: 123},
		{name: "leading zeros with frac", in: "000000000000000001.50", dp: 2, want: 150},
		{name: "max int64", in: "92233720368.54775807", dp: 8, want: 1<<63 - 1},

		{name: "empty", in: "", dp: 8, wantErr: true},
		{name: "bare dot", in: ".", dp: 8, wantErr: true},
		{name: "bare minus", in: "-", dp: 8, wantErr: true},
		{name: "letters", in: "12a4", dp: 8, wantErr: true, errContains: "invalid character"},
		{name: "double dot", in: "1.2.3", dp: 8, wantErr: true, errContains: "invalid character"},
		{name: "invalid char beyond dp", in: "1.2x", dp: 1, wantErr: true, errContains: "invalid character"},
		{name: "nonzero beyond dp", in: "1.234", dp: 2, wantErr: true, errContains: "decimal places"},
		{name: "overflow digits", in: "92233720368.54775808", dp: 8, wantErr: true, errContains: "overflows"},
		{name: "overflow padding", in: "92233720369", dp: 8, wantErr: true, errContains: "overflows"},
		{name: "dp too large", in: "1", dp: 19, wantErr: true, errContains: "out of range"},
		{name: "dp negative", in: "1", dp: -1, wantErr: true, errContains: "out of range"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFixed(tt.in, tt.dp)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseFixed(%q, %d) error = %v, wantErr %v", tt.in, tt.dp, err, tt.wantErr)
			}
			if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
				t.Fatalf("ParseFixed(%q, %d) error = %q, want it to contain %q", tt.in, tt.dp, err, tt.errContains)
			}
			if err == nil && got != tt.want {
				t.Fatalf("ParseFixed(%q, %d) = %d, want %d", tt.in, tt.dp, got, tt.want)
			}
		})
	}
}

func TestFormatFixed(t *testing.T) {
	tests := []struct {
		name string
		v    int64
		dp   int
		want string
	}{
		{name: "typical price", v: 5_012_345_000_000, dp: 8, want: "50123.45000000"},
		{name: "one unit", v: 1, dp: 8, want: "0.00000001"},
		{name: "zero", v: 0, dp: 8, want: "0.00000000"},
		{name: "zero decimal places", v: 42, dp: 0, want: "42"},
		{name: "negative", v: -150, dp: 2, want: "-1.50"},
		{name: "max int64", v: 1<<63 - 1, dp: 8, want: "92233720368.54775807"},
		{name: "min int64", v: -1 << 63, dp: 8, want: "-92233720368.54775808"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatFixed(tt.v, tt.dp); got != tt.want {
				t.Fatalf("FormatFixed(%d, %d) = %q, want %q", tt.v, tt.dp, got, tt.want)
			}
		})
	}
}

// TestFormatFixedBadDPPanics pins the contract that an out-of-range dp (a
// programmer bug) panics rather than returning a bogus string.
func TestFormatFixedBadDPPanics(t *testing.T) {
	for _, dp := range []int{-1, 19} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("FormatFixed(1, %d) did not panic", dp)
				}
			}()
			FormatFixed(1, dp)
		})
	}
}

// TestParseFormatRoundTrip checks parse(format(v)) == v across the full range
// of decimal places, the core exactness guarantee of fixed-point.
func TestParseFormatRoundTrip(t *testing.T) {
	// MinInt64 is deliberately absent: ParseFixed's range is ±(1<<63 - 1).
	values := []int64{0, 1, -1, 99, 5_012_345_000_000, 1<<63 - 1, -(1<<63 - 1)}
	for _, v := range values {
		for dp := 0; dp <= 18; dp++ {
			s := FormatFixed(v, dp)
			got, err := ParseFixed(s, dp)
			if err != nil {
				t.Fatalf("ParseFixed(%q, %d) error: %v", s, dp, err)
			}
			if got != v {
				t.Fatalf("round trip v=%d dp=%d: %q -> %d", v, dp, s, got)
			}
		}
	}
}

// FuzzParseFormat asserts the round-trip identity holds for arbitrary values
// and scales the seed corpus never thought of.
func FuzzParseFormat(f *testing.F) {
	f.Add(int64(0), 0)
	f.Add(int64(5_012_345_000_000), 8)
	f.Add(int64(-1), 2)
	f.Fuzz(func(t *testing.T, v int64, dp int) {
		dp = ((dp % 19) + 19) % 19 // map any int into [0, 18]
		if v == math.MinInt64 {
			return // outside the representable range by design
		}
		s := FormatFixed(v, dp)
		got, err := ParseFixed(s, dp)
		if err != nil {
			t.Fatalf("ParseFixed(%q, %d) error: %v", s, dp, err)
		}
		if got != v {
			t.Fatalf("round trip v=%d dp=%d: %q -> %d", v, dp, s, got)
		}
	})
}
