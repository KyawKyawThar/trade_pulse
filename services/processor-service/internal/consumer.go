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

// metadataTimeout bounds Check's broker round trip when the caller's context
// carries no deadline of its own (mirrors publisher.go).
const metadataTimeout = 2 * time.Second

// TradeHandler processes one decoded trade. Returning an error means the trade
// was not handled: its offset is left uncommitted so it is redelivered after a
// restart (at-least-once). ctx is the consumer's run context.
type TradeHandler func(ctx context.Context, event domain.TradeEvent) error

// recordMarker is what dispatch needs to mark a record's offset eligible for
// the next auto-commit. Defined on the consumer side (accept interfaces,
// return structs) so dispatch's decode/handle/commit decisions can be tested
// with a fake instead of a live Kafka client; *kgo.Client is the production
// implementation.
type recordMarker interface {
	MarkCommitRecords(rs ...*kgo.Record)
}

// kafkaConsumer is the subset of *kgo.Client that Consumer depends on.
// Defined on the consumer side so Run and Check can be exercised in tests
// with a fake instead of a live broker connection.
type kafkaConsumer interface {
	recordMarker
	PollFetches(ctx context.Context) kgo.Fetches
	Ping(ctx context.Context) error
	Close()
}

// Consumer reads trade events from trades.raw for the processor consumer group
// and dispatches each to a TradeHandler.
type Consumer struct {
	client kafkaConsumer
	log    zerolog.Logger
}

// NewConsumer builds a Consumer subscribed to trades.raw under the processor
// consumer group, using franz-go (pure Go — no cgo/librdkafka) so the
// CGO_ENABLED=0/distroless build every service uses keeps working; ingestion-
// service's producer made the same choice for the same reason (see
// SPRINT_PLAN.md risk #4).
//
// Offset discipline is at-least-once: auto-commit is left on for cheap batched
// commits, but AutoCommitMarks restricts it to records explicitly marked via
// MarkCommitRecords, which dispatch only does once a message has been handled
// (see dispatch). A trade is not safe to drop, so a crash mid-batch replays
// the un-marked tail rather than skipping it.
func NewConsumer(cfg config.KafkaConfig, log zerolog.Logger) (*Consumer, error) {

	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: no broker configured")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(domain.ConsumerGroupProcessor),
		kgo.ConsumeTopics(domain.TopicTradesRaw),

		// A fresh group with no committed offset starts from the oldest
		// retained trade rather than skipping history — losing trades is worse
		// than reprocessing them (the downstream Redis writes are idempotent).
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),

		// At-least-once: keep periodic auto-commit, but restrict it to
		// records marked via MarkCommitRecords, called only after a
		// successful handle.
		kgo.AutoCommitMarks(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer: %w", err)
	}

	return &Consumer{
		client: client,
		log:    log,
	}, nil
}

// Run polls trades.raw and dispatches each decoded trade to handle until ctx
// is cancelled, then returns nil. franz-go retries broker connectivity
// internally, so there is no separate fatal-error path here — connectivity
// problems surface through Check (the /health readiness report), not by
// killing the loop.
func (c *Consumer) Run(ctx context.Context, handle TradeHandler) error {
	c.log.Info().Str("topic", domain.TopicTradesRaw).Str("group", domain.ConsumerGroupProcessor).Msg("kafka consumer started")

	for {
		fetches := c.client.PollFetches(ctx)

		if err := ctx.Err(); err != nil {
			c.log.Info().Msg("kafka consumer stopping")
			return nil
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Warn().Err(err).Str("topic", topic).Int32("partition", partition).Msg("kafka fetch error")
		})

		fetches.EachRecord(func(rec *kgo.Record) {
			dispatch(ctx, c.client, c.log, rec, handle)
		})
	}
}

// dispatch decodes one record and runs the handler, then decides whether its
// offset may be committed. A malformed payload is a dead end (it will never
// decode), so it is marked to advance past it; a handler error leaves it
// unmarked so the trade is retried after a restart.
func dispatch(ctx context.Context, marker recordMarker, log zerolog.Logger, rec *kgo.Record, handle TradeHandler) {
	var event domain.TradeEvent

	if err := json.Unmarshal(rec.Value, &event); err != nil {
		log.Warn().Err(err).Str("symbol", string(rec.Key)).Msg("skipping malformed trade event")
		marker.MarkCommitRecords(rec)
		return
	}

	if err := handle(ctx, event); err != nil {
		// Leave the record unmarked for redelivery. A permanently failing
		// (poison) trade would replay forever; a dead-letter path for that is
		// deferred until real downstream processing lands in later tasks.
		log.Error().Err(err).Str("symbol", event.Symbol).Int64("trade_id", event.TradeID).Msg("handling trade failed")
		return
	}

	marker.MarkCommitRecords(rec)
}

func (c *Consumer) Name() string { return "kafka_consumer" }

// Check confirms the consumer can still reach the broker cluster via a cheap
// broker-only request (mirrors publisher.go).
func (c *Consumer) Check(ctx context.Context) error {

	timeout := metadataTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}

	if timeout <= 0 {
		return fmt.Errorf("kafka ping: no time remaining on context")
	}

	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := c.client.Ping(pingCtx); err != nil {
		return fmt.Errorf("kafka ping: %w", err)
	}

	return nil
}

func (c *Consumer) Close() {
	c.client.Close()
}
