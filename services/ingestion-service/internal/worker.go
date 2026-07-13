package internal

import (
	"context"
	"fmt"
	"time"
	"trade_pulse/shared/domain"

	"github.com/gorilla/websocket"
)

// tradePublisher is what the worker loop needs from a publisher: fire one
// normalized trade downstream. Defined here on the consumer side (accept
// interfaces, return structs) so tests can drive the read → normalize →
// publish loop with a fake instead of a live Kafka client; *Publisher is the
// production implementation.
type tradePublisher interface {
	Publish(event domain.TradeEvent) error
}

const (
	binanceWSBaseURL = "wss://stream.binance.com:9443/ws"

	wsHandshakeTimeout = 10 * time.Second

	wsWriteWait = 10 * time.Second

	// wsPongWait bounds how long we tolerate silence before treating the
	// connection as dead. Binance pings every ~20s, so three missed pings
	// is a clear signal the link dropped rather than a transient stall.

	wsPongWait = 60 * time.Second

	// wsReadLimit caps a single frame at 1MiB; a @trade message is well under
	// 1KB, so this just guards against a misbehaving server.
	wsReadLimit = 1 << 20
)

// runSymbol manages a single Binance WebSocket connection for symbol's trade
// stream: it dials, reads messages until ctx is cancelled or the connection
// drops, and returns. A dropped connection is returned as an error and a
// clean ctx-cancelled shutdown returns nil; reconnect.go's
// runSymbolWithReconnect wraps this call, backing off and redialing on the
// error case instead of letting it stop the errgroup.
func (s *Service) runSymbol(ctx context.Context, symbol string, pub tradePublisher) error {

	log := s.log.With().Str("symbol", symbol).Logger()

	conn, err := dialBinanceTrade(ctx, symbol)

	if err != nil {
		return fmt.Errorf("dial %s: %w", symbol, err)
	}

	log.Info().Msg("worker connected")
	s.ws.setConnected(symbol, true)

	defer func() {
		s.ws.setConnected(symbol, false)
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

		event, err := normalizeTrade(raw, time.Now().UTC())
		if err != nil {
			log.Warn().Err(err).RawJSON("raw", raw).Msg("dropping malformed trade message")
			continue
		}

		if err := pub.Publish(event); err != nil {
			log.Error().Err(err).Interface("trade", event).Msg("publish trade event")
			continue
		}

		log.Debug().Interface("event", event).Msg("trade received")
	}
}

func dialBinanceTrade(ctx context.Context, symbol string) (*websocket.Conn, error) {
	dialer := websocket.Dialer{HandshakeTimeout: wsHandshakeTimeout}

	conn, resp, err := dialer.DialContext(ctx, binanceWSBaseURL+"/"+symbol+"@trade", nil)
	if err != nil {
		// On a handshake failure gorilla still returns a non-nil resp whose
		// body holds the server's error; close it so it doesn't leak.
		if resp != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}

	_ = resp.Body.Close()
	return conn, nil
}
