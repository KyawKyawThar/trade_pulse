package internal

// Package internal contains processor-service's logic: consume trades.raw from
// Kafka, run them through a worker pool, to the order-book updater /
// Redis writer / whale detector, and write live snapshots to Redis.
//
//
//	consumer.go       — Kafka consumer group on trades.raw
//	pool.go           — worker pool (~100) via errgroup (Pattern 1)
//	fanout.go         — one trade -> N downstream channels (Pattern 2)
//	enricher.go       — add notional (price*qty), metadata
//	orderbook.go      — in-memory book with sync.RWMutex (Pattern 3)
//	redis_writer.go   — latest-trade + price:<symbol> + book snapshot to Redis
//	whale_detector.go — notional > $500K -> publish to RabbitMQ (Sprint 6)

import (
	"context"
	"fmt"
	"trade_pulse/shared/config"
	"trade_pulse/shared/domain"
	"trade_pulse/shared/httpserver"

	"github.com/rs/zerolog"
)

// Service is processor-service's root component.
type Service struct {
	cfg config.Config
	log zerolog.Logger
	ops *httpserver.Server
}

// New constructs the service from loaded config. The Kafka consumer's /health
// checker registers once Run builds the consumer (mirrors ingestion-service).
func New(cfg config.Config, log zerolog.Logger, ops *httpserver.Server) *Service {
	return &Service{cfg: cfg, log: log, ops: ops}
}

func (s *Service) Run(ctx context.Context) error {
	s.log.Info().Msg("processor-service starting")

	consumer, err := NewConsumer(s.cfg.Kafka, s.log)

	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}

	defer consumer.Close()
	s.ops.RegisterChecker(consumer)

	handler := Chain(s.handleTrade, withLogging(s.log))
	err = consumer.Run(ctx, handler)
	s.log.Info().Msg("processor-service stopping")
	return err
}

// handleTrade is the terminal handler in the middleware chain — a no-op
// until Sprint 2's enrichment/fan-out/Redis-write land as the real trade
// processing.
func (s *Service) handleTrade(_ context.Context, event domain.TradeEvent) error {
	s.log.Debug().
		Str("symbol", event.Symbol).
		Float64("price", event.Price).
		Float64("quantity", event.Quantity).
		Str("side", string(event.Side)).
		Int64("trade_id", event.TradeID).
		Msg("consumed trade")
	return nil
}
