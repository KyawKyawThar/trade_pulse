package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"trade_pulse/shared/config"
	"trade_pulse/shared/domain"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
)

// pollTimeout bounds a single Poll call. The loop wakes at least this often to
// notice context cancellation, so shutdown latency is capped at ~one poll.
const pollTimeout = 100 * time.Millisecond

// metadataTimeout bounds Check's broker round trip when the caller's context
// carries no deadline of its own (mirrors publisher.go).
const metadataTimeout = 2 * time.Second

// TradeHandler processes one decoded trade. Returning an error means the trade
// was not handled: its offset is left uncommitted so it is redelivered after a
// restart (at-least-once). ctx is the consumer's run context.
type TradeHandler func(ctx context.Context, event domain.TradeEvent) error

// messageStorer is what dispatch needs to mark a message's offset eligible
// for commit. Defined on the consumer side (accept interfaces, return
// structs) so dispatch's decode/handle/commit decisions can be tested with a
// fake instead of a live Kafka client; *kafka.Consumer is the production
// implementation.
type messageStorer interface {
	StoreMessage(m *kafka.Message) ([]kafka.TopicPartition, error)
}

// kafkaConsumer is the subset of *kafka.Consumer that Consumer depends on.
// Defined on the consumer side so Run, Check, and Close can be exercised in
// tests with a fake instead of a live broker connection — the same seam
// ingestion-service uses for its Kafka producer, and the boundary that would
// absorb a future client-library swap (ingestion already swapped its
// producer to franz-go over cgo/distroless build friction; see
// SPRINT_PLAN.md risk #4 — this interface means processor-service isn't
// locked to confluent-kafka-go either).
type kafkaConsumer interface {
	messageStorer
	Subscribe(topic string, cb kafka.RebalanceCb) error
	Poll(timeoutMs int) kafka.Event
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
	Close() error
}

// Consumer reads trade events from trades.raw for the processor consumer group
// and dispatches each to a TradeHandler.
type Consumer struct {
	consumer kafkaConsumer
	log      zerolog.Logger
}

// NewConsumer builds a Consumer subscribed (in Run) to trades.raw under the
// processor consumer group.
//
// Offset discipline is at-least-once: auto-commit is left on for cheap batched
// commits, but auto-offset-store is turned off so an offset only becomes
// eligible for commit once its message has been handled (see Run). A trade is
// not safe to drop, so a crash mid-batch replays the un-stored tail rather than
// skipping it.

func NewConsumer(cfg config.KafkaConfig, log zerolog.Logger) (*Consumer, error) {

	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: no broker configured")
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Brokers, ","),
		"group.id":          domain.ConsumerGroupProcessor,
		// A fresh group with no committed offset starts from the oldest
		// retained trade rather than skipping history — losing trades is worse
		// than reprocessing them (the downstream Redis writes are idempotent).
		"auto.offset.reset": "earliest",

		// At-least-once: keep periodic auto-commit, but gate which offsets it
		// may commit on StoreMessage (called only after a successful handle).
		"enable.auto.commit":       true,
		"enable.auto.offset.store": false,
	})

	if err != nil {
		return nil, fmt.Errorf("kafka:new consumer: %w", err)
	}

	return &Consumer{
		consumer: consumer,
		log:      log,
	}, nil
}

// Run subscribes to trades.raw and dispatches each decoded trade to handle
// until ctx is cancelled, then returns nil. It returns a non-nil error only if
// it cannot subscribe or the broker reports a fatal (unrecoverable) error;
// transient errors and malformed messages are logged and the loop continues so
// one bad message can't stop the pipeline.
func (c *Consumer) Run(ctx context.Context, handle TradeHandler) error {
	if err := c.consumer.Subscribe(domain.TopicTradesRaw, nil); err != nil {
		return fmt.Errorf("subscribe %s: %w", domain.TopicTradesRaw, err)
	}

	c.log.Info().Str("topic", domain.TopicTradesRaw).Str("group", domain.ConsumerGroupProcessor).Msg("kafka consumer started")

	for {
		select {
		case <-ctx.Done():
			c.log.Info().Msg("kafka consumer stopping")
			return nil
		default:
		}

		switch ev := c.consumer.Poll(int(pollTimeout.Milliseconds())).(type) {
		case *kafka.Message:
			dispatch(ctx, c.consumer, c.log, ev, handle)
		case kafka.Error:
			// librdkafka surfaces most errors as informational events on the
			// poll stream; only a fatal one means the client can't recover.
			if ev.IsFatal() {
				return fmt.Errorf("kafka fatal error: %w", ev)
			}
			c.log.Warn().Err(ev).Msg("kafka consumer error")
		}
	}

}

// dispatch decodes one message and runs the handler, then decides whether its
// offset may be committed. A malformed payload is a dead end (it will never
// decode), so its offset is stored to advance past it; a handler error leaves
// the offset un-stored so the trade is retried after a restart.
func dispatch(ctx context.Context, storer messageStorer, log zerolog.Logger, msg *kafka.Message, handle TradeHandler) {
	var event domain.TradeEvent

	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Warn().Err(err).Str("symbol", string(msg.Key)).Msg("skipping malformed trade event")
		storeOffset(storer, log, msg)
		return
	}

	if err := handle(ctx, event); err != nil {
		// Leave the offset uncommitted for redelivery. A permanently failing
		// (poison) trade would replay forever; a dead-letter path for that is
		// deferred until real downstream processing lands in later tasks.
		log.Error().Err(err).Str("symbol", event.Symbol).Int64("trade_id", event.TradeID).Msg("handling trade failed")
		return
	}

	storeOffset(storer, log, msg)
}

// storeOffset marks msg's offset eligible for the next auto-commit.
func storeOffset(storer messageStorer, log zerolog.Logger, msg *kafka.Message) {
	if _, err := storer.StoreMessage(msg); err != nil {
		log.Error().Err(err).Msg("kafka store offset failed")
	}
}

func (c *Consumer) Name() string { return "kafka_consumer" }

// Check confirms the consumer can still reach the broker cluster via a cheap
// broker-only metadata request (mirrors publisher.go). Consumer-lag reporting
// is layered on here
func (c *Consumer) Check(ctx context.Context) error {

	timeout := metadataTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}

	if timeout <= 0 {
		return fmt.Errorf("kafka metadata: no time remaining on context")
	}

	if _, err := c.consumer.GetMetadata(nil, false, int(timeout.Microseconds())); err != nil {
		return fmt.Errorf("kafka metadata: %w", err)
	}

	return nil
}

func (c *Consumer) Close() {
	if err := c.consumer.Close(); err != nil {
		c.log.Warn().Err(err).Msg("kafka consuemr clsoe")
	}
}
