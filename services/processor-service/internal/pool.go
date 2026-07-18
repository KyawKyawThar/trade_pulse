package internal

import (
	"context"
	"fmt"
	"trade_pulse/shared/domain"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

const defaultPoolSize = 100

type WorkerPool struct {
	numWorkers int
	jobs       chan domain.TradeEvent
	handle     TradeHandler
	log        zerolog.Logger
}

func NewWorkerPool(size int, handle TradeHandler, log zerolog.Logger) *WorkerPool {
	if size <= 0 {
		size = defaultPoolSize
	}

	return &WorkerPool{
		numWorkers: size,
		jobs:       make(chan domain.TradeEvent, size),
		handle:     handle,
		log:        log,
	}
}

// Submit enqueues event for processing. It blocks once every worker is busy
// and the buffer is full — the pool's backpressure, propagated all the way
// back to the Kafka poll loop, since dropping a trade is worse than a
// consumer briefly stalling. Returns ctx.Err() if ctx is cancelled first.
func (p *WorkerPool) Submit(ctx context.Context, event domain.TradeEvent) error {
	select {
	case p.jobs <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Start runs numWorkers goroutines under one errgroup until ctx is cancelled,
// then returns. A worker never fails the group on a bad trade — handle errors
// are logged and processing continues, mirroring consumer.go's stance that one
// poison message can't stop the pipeline.
func (p *WorkerPool) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	for workerID := range p.numWorkers {
		g.Go(func() error {
			return p.runWorker(ctx, workerID)
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("worker pool: %w", err)
	}

	return nil
}

// runWorker dequeues jobs and calls handle on each until ctx is cancelled.
func (p *WorkerPool) runWorker(ctx context.Context, id int) error {
	for {
		select {
		case event := <-p.jobs:
			if err := p.handle(ctx, event); err != nil {
				p.log.Err(err).Int("worker_id", id).Str("symbol", event.Symbol).
					Int64("trade_id", event.TradeID).Msg("process trade failed")
			}
		case <-ctx.Done():
			return nil
		}
	}
}
