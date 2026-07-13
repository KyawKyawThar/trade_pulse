package internal

import (
	"slices"
	"testing"
)

func TestNormalizeSymbols(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "lowercases mixed case from env/config",
			in:   []string{"BTCUSDT", "EthUsdt"},
			want: []string{"btcusdt", "ethusdt"},
		},
		{
			name: "trims whitespace and drops empties",
			in:   []string{" btcusdt ", "", "   "},
			want: []string{"btcusdt"},
		},
		{
			name: "dedupes preserving first-seen order",
			in:   []string{"ethusdt", "BTCUSDT", "btcusdt", "ethusdt"},
			want: []string{"ethusdt", "btcusdt"},
		},
		{
			name: "empty input stays empty",
			in:   nil,
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeSymbols(tt.in)
			if !slices.Equal(got, tt.want) {
				t.Errorf("normalizeSymbols(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
