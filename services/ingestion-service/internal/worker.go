package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

const (
	binanceWSBaseURL = "wss://stream.binance.com:9443/ws"

	wsHandshakeTimeout = 10 * time.Second

	wsWriteWait = 10 * time.Second

	// wsPongWait bounds how long we tolerate silence before treating the
	// connection as dead. Binance pings every ~20s, so three missed pings
	// is a clear signal the link dropped rather than a transient stall.

	wsPongWait = 60 * time.Second

	// wsReadLimit caps a single frame; a @trade message is well under 1KB,
	// this just guards against a misbehaving server.
	// this just guards against a misbehaving server.
	// KB = 1 << 10 // 1,024, MB = 1 << 20 // 1,048,576,
	// GB = 1 << 30 // 1,073,741,824
	wsReadLimit = 1 << 20
)

// runSymbol is the per-symbol worker loop: it opens one Binance WebSocket
// connection for symbol's trade stream, reads messages until ctx is
// cancelled or the connection drops, and returns. A dropped connection is
// returned as a plain error for now; Sprint 1 task 5 (reconnect.go) will
// wrap this call with backoff instead of letting it stop the errgroup.
func (s *Service) runSymbol(ctx context.Context, symbol string) error {

	log := s.log.With().Str("symbol", symbol).Logger()

	conn, err := dialBinanceTrade(ctx, symbol)

	if err != nil {
		return fmt.Errorf("dial %s: %w", symbol, err)
	}

	log.Info().Msg("worker connected")

	defer func() {
		_ = conn.Close()
		log.Info().Msg("worker stopped")

	}()

	// ReadMessage below blocks with no ctx awareness of its own, so a
	// watcher goroutine closes the connection out from under it on
	// cancellation. done stops the watcher leaking past a clean read error

	done := make(chan struct{})

	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			deadline := time.Now().Add(wsWriteWait)
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				deadline)

			_ = conn.Close()
		case <-done:
		}
	}()

	conn.SetReadLimit(wsReadLimit)
	_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))

	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(wsWriteWait))
	})

	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(wsPongWait))
	})

	for {

		_, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read %s: %w", symbol, err)
		}

		// TODO(Sprint 1 #3/#4): hand raw to normalizer.go -> publisher.go
		// instead of logging it once those files exist.
		log.Debug().RawJSON("raw", raw).Msg("trade received")
	}
}

func dialBinanceTrade(ctx context.Context, symbol string) (*websocket.Conn, error) {

	dialer := websocket.Dialer{HandshakeTimeout: wsHandshakeTimeout}

	conn, resp, err := dialer.DialContext(ctx, binanceWSBaseURL+"/"+symbol+"@trade", nil)

	if err != nil {
		return nil, err
	}

	_ = resp.Body.Close()
	return conn, nil
}
