package internal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"trade_pulse/shared/domain"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
)

// fakeStorer records StoreMessage calls so dispatch's commit decisions can be
// asserted without a live Kafka client.
type fakeStorer struct {
	calls int
	err   error
}

func (f *fakeStorer) StoreMessage(m *kafka.Message) ([]kafka.TopicPartition, error) {
	f.calls++
	return nil, f.err
}

// fakeKafkaConsumer implements kafkaConsumer so Run/Check/Close can be
// exercised without a live broker. Poll replays events in order; once
// exhausted it invokes afterAll (typically cancelling the test's context) so
// Run's loop can terminate deterministically instead of spinning forever.
type fakeKafkaConsumer struct {
	fakeStorer

	subscribeErr error

	events         []kafka.Event
	idx            int
	afterAll       func()
	calledAfterAll bool

	metadataErr error
	closeErr    error
	closed      bool
}

func (f *fakeKafkaConsumer) Subscribe(topic string, cb kafka.RebalanceCb) error {
	return f.subscribeErr
}

func (f *fakeKafkaConsumer) Poll(timeoutMs int) kafka.Event {
	if f.idx < len(f.events) {
		e := f.events[f.idx]
		f.idx++
		return e
	}
	if !f.calledAfterAll && f.afterAll != nil {
		f.calledAfterAll = true
		f.afterAll()
	}
	return nil
}

func (f *fakeKafkaConsumer) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	if f.metadataErr != nil {
		return nil, f.metadataErr
	}
	return &kafka.Metadata{}, nil
}

func (f *fakeKafkaConsumer) Close() error {
	f.closed = true
	return f.closeErr
}

func tradeMessage(t *testing.T, event domain.TradeEvent) *kafka.Message {
	t.Helper()
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal trade event: %v", err)
	}
	return &kafka.Message{Key: []byte(event.Symbol), Value: payload}
}

func TestDispatch(t *testing.T) {
	trade := domain.TradeEvent{Symbol: "BTCUSDT", Price: 65000, Quantity: 0.1, TradeID: 42}

	t.Run("malformed payload stores offset without calling handler", func(t *testing.T) {
		storer := &fakeStorer{}
		msg := &kafka.Message{Key: []byte("btcusdt"), Value: []byte("not json")}
		handlerCalled := false

		dispatch(context.Background(), storer, zerolog.Nop(), msg, func(context.Context, domain.TradeEvent) error {
			handlerCalled = true
			return nil
		})

		if handlerCalled {
			t.Error("handler should not be called for a malformed payload")
		}
		if storer.calls != 1 {
			t.Errorf("storeOffset calls = %d, want 1 (advance past a message that can never decode)", storer.calls)
		}
	})

	t.Run("handler error leaves offset uncommitted for redelivery", func(t *testing.T) {
		storer := &fakeStorer{}
		msg := tradeMessage(t, trade)

		dispatch(context.Background(), storer, zerolog.Nop(), msg, func(context.Context, domain.TradeEvent) error {
			return errors.New("downstream unavailable")
		})

		if storer.calls != 0 {
			t.Errorf("storeOffset calls = %d, want 0 (offset must stay uncommitted on handler failure)", storer.calls)
		}
	})

	t.Run("successful handling stores offset", func(t *testing.T) {
		storer := &fakeStorer{}
		msg := tradeMessage(t, trade)
		var got domain.TradeEvent

		dispatch(context.Background(), storer, zerolog.Nop(), msg, func(_ context.Context, event domain.TradeEvent) error {
			got = event
			return nil
		})

		if storer.calls != 1 {
			t.Errorf("storeOffset calls = %d, want 1 after successful handling", storer.calls)
		}
		if got != trade {
			t.Errorf("handler received %+v, want %+v", got, trade)
		}
	})

	t.Run("StoreMessage failure is swallowed, not propagated", func(t *testing.T) {
		storer := &fakeStorer{err: errors.New("broker unreachable")}
		msg := tradeMessage(t, trade)

		dispatch(context.Background(), storer, zerolog.Nop(), msg, func(context.Context, domain.TradeEvent) error {
			return nil
		})

		if storer.calls != 1 {
			t.Errorf("storeOffset calls = %d, want 1 (StoreMessage was attempted)", storer.calls)
		}
	})
}

func TestConsumerRun(t *testing.T) {
	t.Run("subscribe failure is returned without polling", func(t *testing.T) {
		fake := &fakeKafkaConsumer{subscribeErr: errors.New("no brokers")}
		c := &Consumer{consumer: fake, log: zerolog.Nop()}

		err := c.Run(context.Background(), func(context.Context, domain.TradeEvent) error { return nil })

		if err == nil {
			t.Fatal("Run() = nil, want error when Subscribe fails")
		}
	})

	t.Run("context cancellation stops the loop cleanly", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		fake := &fakeKafkaConsumer{}
		c := &Consumer{consumer: fake, log: zerolog.Nop()}

		if err := c.Run(ctx, func(context.Context, domain.TradeEvent) error { return nil }); err != nil {
			t.Errorf("Run() = %v, want nil on a cancelled context", err)
		}
	})

	t.Run("fatal broker error stops the loop and is returned", func(t *testing.T) {
		fatal := kafka.NewError(kafka.ErrAllBrokersDown, "all brokers down", true)
		fake := &fakeKafkaConsumer{events: []kafka.Event{fatal}}
		c := &Consumer{consumer: fake, log: zerolog.Nop()}

		err := c.Run(context.Background(), func(context.Context, domain.TradeEvent) error { return nil })

		if err == nil {
			t.Fatal("Run() = nil, want error on a fatal broker error")
		}
	})

	t.Run("non-fatal broker error is logged and the loop continues", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		transient := kafka.NewError(kafka.ErrTransport, "transient blip", false)
		fake := &fakeKafkaConsumer{events: []kafka.Event{transient}, afterAll: cancel}
		c := &Consumer{consumer: fake, log: zerolog.Nop()}

		if err := c.Run(ctx, func(context.Context, domain.TradeEvent) error { return nil }); err != nil {
			t.Errorf("Run() = %v, want nil after a non-fatal error followed by cancellation", err)
		}
	})

	t.Run("a message is dispatched to the handler and its offset stored", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		trade := domain.TradeEvent{Symbol: "ETHUSDT", Price: 3000, Quantity: 2, TradeID: 7}
		msg := tradeMessage(t, trade)
		fake := &fakeKafkaConsumer{events: []kafka.Event{msg}, afterAll: cancel}
		c := &Consumer{consumer: fake, log: zerolog.Nop()}

		var got domain.TradeEvent
		err := c.Run(ctx, func(_ context.Context, event domain.TradeEvent) error {
			got = event
			return nil
		})

		if err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
		if got != trade {
			t.Errorf("handler received %+v, want %+v", got, trade)
		}
		if fake.calls != 1 {
			t.Errorf("StoreMessage calls = %d, want 1", fake.calls)
		}
	})
}

func TestConsumerCheck(t *testing.T) {
	t.Run("metadata reachable reports healthy", func(t *testing.T) {
		c := &Consumer{consumer: &fakeKafkaConsumer{}, log: zerolog.Nop()}

		if err := c.Check(context.Background()); err != nil {
			t.Errorf("Check() = %v, want nil", err)
		}
	})

	t.Run("metadata failure is reported", func(t *testing.T) {
		c := &Consumer{consumer: &fakeKafkaConsumer{metadataErr: errors.New("broker unreachable")}, log: zerolog.Nop()}

		if err := c.Check(context.Background()); err == nil {
			t.Error("Check() = nil, want error when GetMetadata fails")
		}
	})

	t.Run("expired context is reported without calling the broker", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()
		<-ctx.Done()

		c := &Consumer{consumer: &fakeKafkaConsumer{}, log: zerolog.Nop()}

		if err := c.Check(ctx); err == nil {
			t.Error("Check() = nil, want error when the context has no time remaining")
		}
	})
}

func TestConsumerClose(t *testing.T) {
	fake := &fakeKafkaConsumer{closeErr: errors.New("close failed")}
	c := &Consumer{consumer: fake, log: zerolog.Nop()}

	c.Close()

	if !fake.closed {
		t.Error("Close() did not call the underlying consumer's Close")
	}
}
