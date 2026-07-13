package internal

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// wsHealth reports per-symbol WebSocket connection state to /health. It is a
// readiness signal, not a liveness one: a symbol sitting in reconnect backoff
// makes Check fail by design, and the right reaction is "stop routing to me /
// alert a human", never "restart the process" — a restart would discard the
// backoff state and redial the exchange immediately (see risk: Binance WS
// bans). Liveness probes belong on /health/live.
type wsHealth struct {
	mu        sync.RWMutex
	connected map[string]bool
}

func newWSHealth(symbols []string) *wsHealth {

	h := &wsHealth{
		connected: make(map[string]bool, len(symbols)),
	}

	for _, sym := range symbols {
		h.connected[sym] = false
	}
	return h
}

func (h *wsHealth) Name() string { return "websocket" }

func (h *wsHealth) setConnected(symbol string, connected bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.connected[symbol] = connected

}

func (h *wsHealth) Check(_ context.Context) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var down []string

	for sym, ok := range h.connected {
		if !ok {
			down = append(down, sym)
		}
	}

	if len(down) == 0 {
		return nil
	}

	sort.Strings(down)

	return fmt.Errorf("disconnected %v", down)
}
