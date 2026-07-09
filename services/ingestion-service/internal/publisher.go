package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"trade_pulse/shared/config"
	"trade_pulse/shared/domain"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

// flushTimeout bounds how long Close waits for in-flight messages to be
// acked before giving up, so a wedged broker connection can't hang shutdown
// forever.
const flushTimeout = 5 * time.Second

type Publisher struct {
	client *kgo.Client
	log    zerolog.Logger
}

func NewPublisher(cfg config.KafkaConfig, log zerolog.Logger) (*Publisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: no broker configured")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),

		// Batching: hold messages up to 5ms or 64KB, whichever comes first,
		// so a burst of trades across symbols ships as one request instead
		// of one round trip per message. 5ms is well inside the <50ms p99
		// Kafka latency target (Architecture § Performance Targets).
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchMaxBytes(64*1024),

		// lz4 trades a little compression ratio for low CPU cost, keeping
		// the producer cheap at the 50k msg/sec ingestion target.
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),

		// A trade event is not safe to drop: wait for all in-sync replicas.
		// franz-go keeps idempotent production enabled with AllISRAcks, so a
		// retried batch can never turn into a duplicate trade downstream.
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer: %w", err)
	}

	return &Publisher{client: client, log: log}, nil
}

// Publish enqueues event for async delivery to trades.raw, keyed by symbol.
// It returns once the message is buffered locally, not once the broker acks —
// callers that need delivery confirmation should watch the /health producer
// checker (Sprint 1 task 6), not this call. Delivery failures surface via the
// promise callback below (logged, not returned), since Produce is async.
func (p *Publisher) Publish(event domain.TradeEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal trade event: %w", err)
	}

	rec := &kgo.Record{
		Topic:     domain.TopicTradesRaw,
		Key:       []byte(event.Symbol),
		Value:     payload,
		Timestamp: event.EventTime,
	}

	p.client.Produce(context.Background(), rec, p.onDelivery)
	return nil
}

// onDelivery logs any record that failed delivery after the client exhausted
// its retries. Successful deliveries are not logged — at 50k msg/sec that
// would dominate the log stream for no benefit.
func (p *Publisher) onDelivery(rec *kgo.Record, err error) {
	if err != nil {
		p.log.Error().Err(err).
			Str("topic", rec.Topic).
			Int32("partition", rec.Partition).
			Str("symbol", string(rec.Key)).Msg("kafka delivery failed")
	}
}

// Close flushes buffered messages within flushTimeout and releases the
// client. Call once, after every worker publishing through it has stopped.
func (p *Publisher) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()

	if err := p.client.Flush(ctx); err != nil {
		p.log.Warn().Err(err).Msg("kafka producer closed with undelivered messages")
	}
	p.client.Close()
}
