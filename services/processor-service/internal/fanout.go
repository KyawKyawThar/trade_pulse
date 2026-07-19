package internal

import (
	"context"
	"trade_pulse/shared/domain"
)

const defaultFanOutBuffer = 256

// FanOut delivers each published trade to every downstream sink (order-book
// updater, Redis writer, broadcaster) — unlike pool.go's WorkerPool, where a
// job goes to exactly one worker. The sink channels are never closed;
// consumers must exit via their own ctx, as service.go's drainSink does.
type FanOut struct {
	orderBook   chan domain.TradeEvent
	redisWriter chan domain.TradeEvent
	broadcast   chan domain.TradeEvent
}

// NewFanOut sizes each sink's buffer to bufferSize, falling back to
// defaultFanOutBuffer when bufferSize <= 0 — the same guard as NewWorkerPool.
func NewFanOut(bufferSize int) *FanOut {
	if bufferSize <= 0 {
		bufferSize = defaultFanOutBuffer
	}

	return &FanOut{
		orderBook:   make(chan domain.TradeEvent, bufferSize),
		redisWriter: make(chan domain.TradeEvent, bufferSize),
		broadcast:   make(chan domain.TradeEvent, bufferSize),
	}
}

func (f *FanOut) OrderBookUpdate() <-chan domain.TradeEvent { return f.orderBook }

func (f *FanOut) RedisWriter() <-chan domain.TradeEvent { return f.redisWriter }

func (f *FanOut) Broadcast() <-chan domain.TradeEvent { return f.broadcast }

// Publish delivers event to all three sinks, blocking until each has accepted
// it — the same backpressure contract as WorkerPool.Submit, propagated back to
// the Kafka poll loop. Each sink is a separate select case that is nil-ed out
// once it accepts (a send on a nil channel blocks forever, dropping that case),
// so sinks receive in whatever order they have room and one full sink never
// delays the others. Returns ctx.Err() if ctx is cancelled first; cancellation
// mid-call means partial delivery — sinks that already accepted keep the event.
func (f *FanOut) Publish(ctx context.Context, event domain.TradeEvent) error {
	ob, rw, bc := f.orderBook, f.redisWriter, f.broadcast

	for ob != nil || rw != nil || bc != nil {
		select {
		case ob <- event:
			ob = nil
		case rw <- event:
			rw = nil
		case bc <- event:
			bc = nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
