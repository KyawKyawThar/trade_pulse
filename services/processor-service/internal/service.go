package internal

// Package internal contains processor-service's logic: consume trades.raw from
// Kafka, run them through a worker pool, fan out to the order-book updater /
// Redis writer / whale detector, and write live snapshots to Redis.
//
// SCOPE: Sprint-0 skeleton. Real work (SPRINT_PLAN.md § Sprint 2 & 6):
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

// drainSink is the temporary consumer for one fan-out sink, until
// orderbook.go/redis_writer.go and api-service's ws/broadcaster.go replace
// it. It logs each trade at debug, tagged with which sink received it, so
// the fan-out can be verified end-to-end; it returns once ctx is cancelled.

func (s *Service) Run(ctx context.Context) error {
	s.log.Info().Msg("processor-service starting")

	consumer, err := NewConsumer(s.cfg.Kafka, s.log)

	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}

	defer consumer.Close()
	s.ops.RegisterChecker(consumer)

	fanOut := NewFanOut(s.cfg.Processor.FanOutBuffer)
	enricher := NewEnricher(fanOut.Publish, NewDefaultMetadataProvider())
	pool := NewWorkerPool(s.cfg.Processor.PoolSize, enricher.Handle, s.log)
	orderBooks := NewOrderBookStore()
	redisWriter, err := NewRedisWriter(s.cfg.Redis, orderBooks, s.log)

	if err != nil {
		return fmt.Errorf("redis writer %w", err)
	}

	defer func() {
		if err := redisWriter.Close(); err != nil {
			s.log.Warn().Err(err).Msg("redis writer close")
		}
	}()

	s.ops.RegisterChecker(redisWriter)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return pool.Start(ctx) })
	eg.Go(func() error { return consumer.Run(ctx, pool.Submit) })

	eg.Go(func() error { orderBooks.Run(ctx, fanOut.OrderBookUpdate()); return nil })
	eg.Go(func() error { s.drainSink(ctx, "redis_writer", fanOut.RedisWriter()); return nil })
	eg.Go(func() error { s.drainSink(ctx, "broadcaster", fanOut.Broadcast()); return nil })
	err = eg.Wait()
	s.log.Info().Msg("processor-service stopping")

	return err

}

// drainSink is the temporary consumer for one fan-out sink, until
// enricher.go/orderbook.go/redis_writer.go and api-service's
// ws/broadcaster.go replace it. It logs each trade at debug, tagged with
// which sink received it, so the fan-out can be verified end-to-end; it
// returns once ctx is cancelled.
func (s *Service) drainSink(ctx context.Context, sink string, ch <-chan domain.TradeEvent) {
	for {
		select {
		case event := <-ch:
			s.log.Debug().
				Str("sink", sink).
				Str("symbol", event.Symbol).
				Float64("price", event.Price).
				Float64("quantity", event.Quantity).
				Float64("notional", event.Notional).
				Str("side", string(event.Side)).
				Int64("trade_id", event.TradeID).
				Str("base_asset", event.Market.BaseAsset).
				Str("exchange", event.Market.Exchange).
				Msg("fan-out sink received trade")
		case <-ctx.Done():
			return
		}
	}
}
