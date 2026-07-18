package internal

import (
	"context"
	"strings"
	"testing"
)

func TestWSHealth(t *testing.T) {
	ctx := context.Background()

	t.Run("all symbols start disconnected", func(t *testing.T) {
		h := newWSHealth([]string{"ethusdt", "btcusdt", "solusdt"})

		err := h.Check(ctx)
		if err == nil {
			t.Fatal("Check() = nil, want error before any connection")
		}
		// Sorted for a stable /health message regardless of map order.
		if want := "disconnected [btcusdt ethusdt solusdt]"; err.Error() != want {
			t.Errorf("Check() = %q, want %q", err, want)
		}
	})

	t.Run("healthy once every symbol connects", func(t *testing.T) {
		h := newWSHealth([]string{"btcusdt", "ethusdt", "solusdt"})
		h.setConnected("btcusdt", true)
		h.setConnected("ethusdt", true)
		h.setConnected("solusdt", true)

		if err := h.Check(ctx); err != nil {
			t.Errorf("Check() = %v, want nil", err)
		}
	})

	t.Run("one dropped symbol degrades health", func(t *testing.T) {
		h := newWSHealth([]string{"btcusdt", "ethusdt", "solusdt"})
		h.setConnected("btcusdt", true)
		h.setConnected("ethusdt", true)
		h.setConnected("solusdt", true)
		h.setConnected("btcusdt", false)

		err := h.Check(ctx)
		if err == nil {
			t.Fatal("Check() = nil, want error with a symbol down")
		}
		if !strings.Contains(err.Error(), "btcusdt") {
			t.Errorf("Check() = %q, want mention of btcusdt", err)
		}

		if strings.Contains(err.Error(), "solusdt") {
			t.Errorf("Check() = %q must not blame the connected solusdt", err)
		}

		if strings.Contains(err.Error(), "ethusdt") {
			t.Errorf("Check() = %q must not blame the connected ethusdt", err)
		}
	})
}
