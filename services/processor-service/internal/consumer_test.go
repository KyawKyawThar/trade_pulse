package internal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"trade_pulse/shared/domain"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeMarker records MarkCommitRecords calls so dispatch's commit decisions
// can be asserted without a live Kafka client.
type fakeMarker struct {
	calls int
}

func (f *fakeMarker) MarkCommitRecords(rs ...*kgo.Record) {
	f.calls += len(rs)
}

func tradeRecord(t *testing.T, event domain.TradeEvent) *kgo.Record {
	t.Helper()
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal trade event: %v", err)
	}
	return &kgo.Record{Topic: domain.TopicTradesRaw, Key: []byte(event.Symbol), Value: payload}
}

// recordFetch wraps a single record into the Fetches shape Run consumes,
// mirroring what franz-go itself would hand back from PollFetches.
func recordFetch(rec *kgo.Record) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      rec.Topic,
		Partitions: []kgo.FetchPartition{{Records: []*kgo.Record{rec}}},
	}}}}
}

// fakeKafkaConsumer implements kafkaConsumer so Run/Check/Close can be
// exercised without a live broker. PollFetches replays fetches in order;
// once exhausted it invokes afterAll (typically cancelling the test's
// context) so Run's loop can terminate deterministically instead of
// spinning forever.
type fakeKafkaConsumer struct {
	fakeMarker

	fetches        []kgo.Fetches
	idx            int
	afterAll       func()
	calledAfterAll bool

	pingErr error
	closed  bool
}

func (f *fakeKafkaConsumer) PollFetches(ctx context.Context) kgo.Fetches {
	if f.idx < len(f.fetches) {
		fs := f.fetches[f.idx]
		f.idx++
		return fs
	}
	if !f.calledAfterAll && f.afterAll != nil {
		f.calledAfterAll = true
		f.afterAll()
	}
	return kgo.Fetches{}
}

func (f *fakeKafkaConsumer) Ping(ctx context.Context) error {
	return f.pingErr
}

func (f *fakeKafkaConsumer) Close() {
	f.closed = true
}

func TestDispatch(t *testing.T) {
	trade := domain.TradeEvent{Symbol: "BTCUSDT", Price: 65000, Quantity: 0.1, TradeID: 42}

	t.Run("malformed payload marks the record without calling handler", func(t *testing.T) {
		marker := &fakeMarker{}
		rec := &kgo.Record{Key: []byte("btcusdt"), Value: []byte("not json")}
		handlerCalled := false

		dispatch(context.Background(), marker, zerolog.Nop(), rec, func(context.Context, domain.TradeEvent) error {
			handlerCalled = true
			return nil
		})

		if handlerCalled {
			t.Error("handler should not be called for a malformed payload")
		}
		if marker.calls != 1 {
			t.Errorf("MarkCommitRecords calls = %d, want 1 (advance past a message that can never decode)", marker.calls)
		}
	})

	t.Run("handler error leaves the record unmarked for redelivery", func(t *testing.T) {
		marker := &fakeMarker{}
		rec := tradeRecord(t, trade)

		dispatch(context.Background(), marker, zerolog.Nop(), rec, func(context.Context, domain.TradeEvent) error {
			return errors.New("downstream unavailable")
		})

		if marker.calls != 0 {
			t.Errorf("MarkCommitRecords calls = %d, want 0 (record must stay unmarked on handler failure)", marker.calls)
		}
	})

	t.Run("successful handling marks the record", func(t *testing.T) {
		marker := &fakeMarker{}
		rec := tradeRecord(t, trade)
		var got domain.TradeEvent

		dispatch(context.Background(), marker, zerolog.Nop(), rec, func(_ context.Context, event domain.TradeEvent) error {
			got = event
			return nil
		})

		if marker.calls != 1 {
			t.Errorf("MarkCommitRecords calls = %d, want 1 after successful handling", marker.calls)
		}
		if got != trade {
			t.Errorf("handler received %+v, want %+v", got, trade)
		}
	})
}

func TestConsumerRun(t *testing.T) {
	t.Run("context cancellation stops the loop cleanly", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		fake := &fakeKafkaConsumer{}
		c := &Consumer{client: fake, log: zerolog.Nop()}

		if err := c.Run(ctx, func(context.Context, domain.TradeEvent) error { return nil }); err != nil {
			t.Errorf("Run() = %v, want nil on a cancelled context", err)
		}
	})

	t.Run("a fetch error is logged and the loop continues until cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fake := &fakeKafkaConsumer{
			fetches:  []kgo.Fetches{kgo.NewErrFetch(errors.New("transient blip"))},
			afterAll: cancel,
		}
		c := &Consumer{client: fake, log: zerolog.Nop()}

		if err := c.Run(ctx, func(context.Context, domain.TradeEvent) error { return nil }); err != nil {
			t.Errorf("Run() = %v, want nil after a fetch error followed by cancellation", err)
		}
	})

	t.Run("a record is dispatched to the handler and marked", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		trade := domain.TradeEvent{Symbol: "ETHUSDT", Price: 3000, Quantity: 2, TradeID: 7}
		rec := tradeRecord(t, trade)
		fake := &fakeKafkaConsumer{fetches: []kgo.Fetches{recordFetch(rec)}, afterAll: cancel}
		c := &Consumer{client: fake, log: zerolog.Nop()}

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
			t.Errorf("MarkCommitRecords calls = %d, want 1", fake.calls)
		}
	})
}

func TestConsumerCheck(t *testing.T) {
	t.Run("broker reachable reports healthy", func(t *testing.T) {
		c := &Consumer{client: &fakeKafkaConsumer{}, log: zerolog.Nop()}

		if err := c.Check(context.Background()); err != nil {
			t.Errorf("Check() = %v, want nil", err)
		}
	})

	t.Run("ping failure is reported", func(t *testing.T) {
		c := &Consumer{client: &fakeKafkaConsumer{pingErr: errors.New("broker unreachable")}, log: zerolog.Nop()}

		if err := c.Check(context.Background()); err == nil {
			t.Error("Check() = nil, want error when Ping fails")
		}
	})

	t.Run("expired context is reported without calling the broker", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()
		<-ctx.Done()

		c := &Consumer{client: &fakeKafkaConsumer{}, log: zerolog.Nop()}

		if err := c.Check(ctx); err == nil {
			t.Error("Check() = nil, want error when the context has no time remaining")
		}
	})
}

func TestConsumerClose(t *testing.T) {
	fake := &fakeKafkaConsumer{}
	c := &Consumer{client: fake, log: zerolog.Nop()}

	c.Close()

	if !fake.closed {
		t.Error("Close() did not call the underlying client's Close")
	}
}
