package internal

import (
	"context"
	"errors"
	"testing"
	"time"
	"trade_pulse/shared/domain"
)

func drainOne(t *testing.T, ch <-chan domain.TradeEvent) domain.TradeEvent {

	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for event")
		return domain.TradeEvent{}
	}

}

// TestFanOutPublishReachesEverySink checks one Publish call delivers the same
// event to all three sinks, not just one of them (the defining difference
// from pool.go's WorkerPool, where a job goes to exactly one worker)
func TestFanOutPublishReachesEverySink(t *testing.T) {
	f := NewFanOut(0)
	want := domain.TradeEvent{Symbol: "BTCUSDT", TradeID: 42}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- f.Publish(ctx, want) }()

	for name, ch := range map[string]<-chan domain.TradeEvent{
		"order_book":   f.OrderBookUpdate(),
		"redis_writer": f.RedisWriter(),
		"broadcaster":  f.Broadcast(),
	} {

		if got := drainOne(t, ch); got != want {
			t.Errorf("sink %s got %+v, want %+v", name, got, want)
		}
	}

	if err := <-done; err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}
}

// TestFanOutPublishBlocksThenRespectsContext checks Publish blocks once a
// sink's buffer is full and returns ctx.Err() rather than hanging forever once
// ctx is cancelled — the same backpressure contract pool.go's Submit has.
func TestFanOutPublishBlocksThenRespectsContext(t *testing.T) {
	// A small explicit buffer keeps the fill loop short.
	const bufSize = 8
	f := NewFanOut(bufSize)

	for i := range bufSize {
		if err := f.Publish(context.Background(), domain.TradeEvent{TradeID: int64(i)}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		// Drain the other two sinks so only order_book fills up
		<-f.RedisWriter()
		<-f.Broadcast()
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() { errCh <- f.Publish(ctx, domain.TradeEvent{TradeID: -1}) }()

	select {
	case err := <-errCh:
		t.Fatalf("Publish returned early with err=%v, want it to block", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Publish error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not return after its context was cancelled")
	}
}

// TestFanOutSinksAreIndependentChannels checks the three sinks are distinct
// channels rather than aliases of the same underlying channel.
func TestFanOutSinksAreIndependentChannels(t *testing.T) {
	f := NewFanOut(0)

	if err := f.Publish(context.Background(), domain.TradeEvent{TradeID: 1}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// Draining only order_book must not also consume redis_writer's or
	// broadcaster's copy.
	<-f.OrderBookUpdate()

	select {
	case <-f.RedisWriter():
	case <-time.After(1 * time.Second):
		t.Fatal("redis_writer sink did not receive its own copy")
	}

	select {
	case <-f.Broadcast():
	case <-time.After(1 * time.Second):
		t.Fatal("broadcaster sink did not receive its own copy")
	}
}

// TestNewFanOutDefaultsBufferSize checks bufferSize <= 0 falls back to
// defaultFanOutBuffer rather than producing unbuffered sinks.
func TestNewFanOutDefaultsBufferSize(t *testing.T) {
	for _, size := range []int{0, -5} {
		f := NewFanOut(size)
		if got := cap(f.orderBook); got != defaultFanOutBuffer {
			t.Errorf("NewFanOut(%d): buffer cap = %d, want %d", size, got, defaultFanOutBuffer)
		}
	}
}
