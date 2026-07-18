package internal

import (
	"context"

	"trade_pulse/shared/retry"
)

// runSymbolWithReconnect keeps one symbol's WebSocket worker alive across drops
// by handing runSymbol to the shared jittered-backoff reconnect loop. All the
// retry policy (exponential backoff, full jitter, reset-after-uptime) lives in
// shared/retry so the same behaviour backs Kafka/RabbitMQ/Redis/DB connections
// elsewhere; here we only supply the transport-specific work.
func (s *Service) runSymbolWithReconnect(ctx context.Context, symbol string, pub tradePublisher) error {
	log := s.log.With().Str("symbol", symbol).Logger()

	return retry.WithBackoff(ctx, log, func(ctx context.Context) error {
		return s.runSymbol(ctx, symbol, pub)
	})
}
