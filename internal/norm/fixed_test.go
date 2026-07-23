package norm

import "testing"

func TestParseFixed(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		dp      int
		want    int64
		wantErr bool
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
		{name: "max int64", in: "92233720368.54775807", dp: 8, want: 1<<63 - 1},

		{name: "empty", in: "", dp: 8, wantErr: true},
		{name: "bare dot", in: ".", dp: 8, wantErr: true},
		{name: "bare minus", in: "-", dp: 8, wantErr: true},
		{name: "letters", in: "12a4", dp: 8, wantErr: true},
		{name: "double dot", in: "1.2.3", dp: 8, wantErr: true},
		{name: "nonzero beyond dp", in: "1.234", dp: 2, wantErr: true},
		{name: "overflow digits", in: "92233720368.54775808", dp: 8, wantErr: true},
		{name: "overflow padding", in: "92233720369", dp: 8, wantErr: true},
		{name: "dp too large", in: "1", dp: 19, wantErr: true},
		{name: "dp negative", in: "1", dp: -1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFixed(tt.in, tt.dp)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseFixed(%q, %d) error = %v, wantErr %v", tt.in, tt.dp, err, tt.wantErr)
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

// TestParseFormatRoundTrip checks that parse(format(v)) is the identity for
// representative values, the core exactness guarantee of fixed-point.
func TestParseFormatRoundTrip(t *testing.T) {
	// MinInt64 is deliberately absent: ParseFixed's range is ±(1<<63 - 1).
	values := []int64{0, 1, -1, 99, 5_012_345_000_000, 1<<63 - 1, -(1<<63 - 1)}
	for _, v := range values {
		s := FormatFixed(v, 8)
		got, err := ParseFixed(s, 8)
		if err != nil {
			t.Fatalf("ParseFixed(FormatFixed(%d)) error: %v", v, err)
		}
		if got != v {
			t.Fatalf("round trip %d -> %q -> %d", v, s, got)
		}
	}
}
