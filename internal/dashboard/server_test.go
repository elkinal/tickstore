package dashboard

import (
	"net/http/httptest"
	"testing"
)

// TestQueryInt checks the feed cursor parsing: a good value passes through, and
// anything missing or malformed falls back to 0 ("most recent rows").
func TestQueryInt(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want int64
	}{
		{"absent", "/api/feed", 0},
		{"valid", "/api/feed?ts=1785014617675069000", 1785014617675069000},
		{"zero", "/api/feed?ts=0", 0},
		{"negative", "/api/feed?ts=-5", 0},
		{"garbage", "/api/feed?ts=abc", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", tt.url, nil)
			if got := queryInt(r, "ts"); got != tt.want {
				t.Errorf("queryInt(%q) = %d, want %d", tt.url, got, tt.want)
			}
		})
	}
}
