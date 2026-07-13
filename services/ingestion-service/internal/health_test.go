package internal

import (
	"context"
	"strings"
	"testing"
)

func TestWSHealth(t *testing.T) {
	ctx := context.Background()

	t.Run("all symbols start disconnected", func(t *testing.T) {
		h := newWSHealth([]string{"ethusdt", "btcusdt"})

		err := h.Check(ctx)
		if err == nil {
			t.Fatal("Check() = nil, want error before any connection")
		}
		// Sorted for a stable /health message regardless of map order.
		if want := "disconnected [btcusdt ethusdt]"; err.Error() != want {
			t.Errorf("Check() = %q, want %q", err, want)
		}
	})

	t.Run("healthy once every symbol connects", func(t *testing.T) {
		h := newWSHealth([]string{"btcusdt", "ethusdt"})
		h.setConnected("btcusdt", true)
		h.setConnected("ethusdt", true)

		if err := h.Check(ctx); err != nil {
			t.Errorf("Check() = %v, want nil", err)
		}
	})

	t.Run("one dropped symbol degrades health", func(t *testing.T) {
		h := newWSHealth([]string{"btcusdt", "ethusdt"})
		h.setConnected("btcusdt", true)
		h.setConnected("ethusdt", true)
		h.setConnected("ethusdt", false)

		err := h.Check(ctx)
		if err == nil {
			t.Fatal("Check() = nil, want error with a symbol down")
		}
		if !strings.Contains(err.Error(), "ethusdt") {
			t.Errorf("Check() = %q, want mention of ethusdt", err)
		}
		if strings.Contains(err.Error(), "btcusdt") {
			t.Errorf("Check() = %q, must not blame the connected btcusdt", err)
		}
	})
}
