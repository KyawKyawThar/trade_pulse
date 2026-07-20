package internal

// Package internal contains processor-service's logic: consume trades.raw from
// Kafka, run them through a worker pool, to the order-book updater /
// Redis writer / whale detector, and write live snapshots to Redis.
//
//
//	consumer.go       — Kafka consumer group on trades.raw
//	pool.go           — worker pool (~100) via errgroup (Pattern 1)
//	fanout.go         — one trade -> N downstream channels (Pattern 2)
//	enricher.go       — add notional (price*qty), market metadata
//	metadata.go       — static symbol -> base/quote/exchange lookup
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

// Run builds the trades.raw consumer, a worker pool (pool.go) that drains it,
// an enricher (enricher.go) that adds notional and market metadata to each
// trade, and a fan-out (fanout.go) that each enriched trade is published
// into, then drives all of it until ctx is cancelled. The consumer's poll
// loop only ever calls pool.Submit, so a slow trade can't stall Poll; each
// pool worker's handler is enricher.Handle, which computes notional, looks up
// market metadata (metadata.go), and forwards to fanOut.Publish, so a decoded
// trade reaches every downstream sink already enriched. Until
// orderbook.go/redis_writer.go (Sprint 2 tasks 5-6) and api-service's
// ws/broadcaster.go (Sprint 4) exist, the sinks are drained by the logging
// stub below — the hand-off point those files take over.

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

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return pool.Start(ctx) })
	eg.Go(func() error { return consumer.Run(ctx, pool.Submit) })

	eg.Go(func() error { s.drainSink(ctx, "order_book", fanOut.OrderBookUpdate()); return nil })
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
