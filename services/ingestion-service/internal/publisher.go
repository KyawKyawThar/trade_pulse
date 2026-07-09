package internal

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"trade_pulse/shared/config"
	"trade_pulse/shared/domain"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog"
)

// flushTimeout bounds how long Close waits for in-flight messages to be
// acked before giving up, so a wedged broker connection can't hang shutdown
// forever.

const flushTimeout = 5 * time.Second

type Publisher struct {
	producer *kafka.Producer
	log      zerolog.Logger
}

func NewPublisher(cfg config.KafkaConfig, log zerolog.Logger) (*Publisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: no broker configured")
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Brokers, ","),

		// Batching: hold messages up to 5ms or 64KB, whichever comes first,
		// so a burst of trades across symbols ships as one request instead
		// of one round trip per message. 5ms is well inside the <50ms p99
		// Kafka latency target (Architecture § Performance Targets).
		"linger.ms":  5,
		"batch.size": 64 * 1024,

		// lz4 trades a little compression ratio for low CPU cost, keeping
		// the producer cheap at the 50k msg/sec ingestion target.
		"compression.type": "lz4",

		// A trade event is not safe to drop: wait for all in-sync replicas,
		// and let idempotence dedupe broker-side if a batch is retried so a
		// retry can never turn into a duplicate trade downstream.
		"acks":               "all",
		"enable.idempotence": true,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer: %w", err)
	}

	pub := &Publisher{producer: producer, log: log}
	go pub.handleDeliveryReports()

	return pub, nil
}

// handleDeliveryReports logs any message that failed delivery after
// librdkafka exhausted its retries. Successful deliveries are not logged —
// at 50k msg/sec that would dominate the log stream for no benefit.
func (p *Publisher) handleDeliveryReports() {

	for e := range p.producer.Events() {

		msg, ok := e.(*kafka.Message)

		if !ok {
			continue
		}

		if err := msg.TopicPartition.Error; err != nil {
			p.log.Error().Err(err).
				Str("topic", *msg.TopicPartition.Topic).
				Int32("partition", msg.TopicPartition.Partition).
				Str("symbol", string(msg.Key)).Msg("kafka delivery failed")
		}
	}
}

// Publish enqueues event for async delivery to trades.raw, keyed by symbol.
// It returns once the message is queued locally, not once the broker acks —
// callers that need delivery confirmation should watch the /health producer
// checker (Sprint 1 task 6), not this call.

func (p *Publisher) Publish(event domain.TradeEvent) error {
	payload, err := json.Marshal(event)

	if err != nil {
		return fmt.Errorf("marshal trade event: %w", err)
	}

	topic := domain.TopicTradesRaw
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:       []byte(event.Symbol),
		Value:     payload,
		Timestamp: event.EventTime,
	}, nil)

	return nil
}

// Close flushes queued messages within flushTimeout and releases the
// producer. Call once, after every worker publishing through it has
// stopped.

func (p *Publisher) Close() {

	if remaining := p.producer.Flush(int(flushTimeout.Microseconds())); remaining > 0 {
		p.log.Warn().Int("undelivered", remaining).Msg("kafka producer closed with undelivered messages")
	}
	p.producer.Close()
}
