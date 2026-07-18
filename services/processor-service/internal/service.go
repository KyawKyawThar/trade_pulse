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
	"golang.org/x/sync/errgroup"
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

// Run builds the trades.raw consumer and a worker pool (pool.go) that drains
// it, then drives both until ctx is cancelled. The consumer's poll loop only
// ever calls pool.Submit, so a slow trade can't stall Poll; each of the pool's
// workers calls the stub handler below. As later Sprint 2 tasks land, that
// stub is replaced by the fan-out to the enricher, order book and Redis
// writer — for now it logs at debug, the hand-off point those files take over.
func (s *Service) Run(ctx context.Context) error {
	s.log.Info().Msg("processor-service starting")

	consumer, err := NewConsumer(s.cfg.Kafka, s.log)

	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}

	defer consumer.Close()
	s.ops.RegisterChecker(consumer)

	pool := NewWorkerPool(s.cfg.Processor.PoolSize, s.handleTrade, s.log)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return pool.Start(ctx) })
	eg.Go(func() error { return consumer.Run(ctx, pool.Submit) })
	err = eg.Wait()
	s.log.Info().Msg("processor-service stopping")

	return err

}

// handleTrade is the temporary per-trade sink each pool worker calls, until
// fanout.go/enricher.go land. It logs each decoded trade at debug so the
// consumer + pool can be verified end-to-end.
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
