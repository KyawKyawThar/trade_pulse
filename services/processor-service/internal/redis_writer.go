package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"trade_pulse/shared/config"
	"trade_pulse/shared/domain"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

type RedisClient interface {
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd
	Ping(ctx context.Context) *redis.StatusCmd
	Close() error
}

type RedisWriter struct {
	client RedisClient
	books  *OrderBookStore
	log    zerolog.Logger
}

func NewRedisWriterWithClient(client RedisClient, books *OrderBookStore, log zerolog.Logger) *RedisWriter {

	return &RedisWriter{
		client: client,
		books:  books,
		log:    log,
	}
}

func NewRedisWriter(cfg config.RedisConfig, books *OrderBookStore, log zerolog.Logger) (*RedisWriter, error) {

	if cfg.Addr == "" {
		return nil, fmt.Errorf("redis: addr is required")
	}

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	return NewRedisWriterWithClient(client, books, log), nil
}

func (w *RedisWriter) Run(ctx context.Context, update <-chan domain.TradeEvent) {
	for {
		select {
		case event := <-update:
			if err := w.Write(ctx, event); err != nil {
				w.log.Error().Err(err).Str("symbol", event.Symbol).Int64("trade_id", event.TradeID).Msg("redis write failed")
			}

		case <-ctx.Done():
			return
		}
	}
}

func (w *RedisWriter) Write(ctx context.Context, event domain.TradeEvent) error {

	tradeJson, err := json.Marshal(event)

	if err != nil {
		return fmt.Errorf("marshal trade: %w", err)
	}

	if err := w.client.Set(ctx, domain.KeyLatestTrade(event.Symbol), tradeJson, 0).Err(); err != nil {

		return fmt.Errorf("set latest trade: %w", err)
	}

	if err := w.client.Set(ctx, domain.KeyPrice(event.Symbol), event.Price, 0).Err(); err != nil {
		return fmt.Errorf("set price: %w", err)
	}

	if w.books == nil {
		return nil
	}

	book, ok := w.books.Get(event.Symbol)

	if !ok {
		return nil
	}

	bookJSON, err := json.Marshal(book)

	if err != nil {
		return fmt.Errorf("marshal order book: %w", err)
	}

	if err := w.client.Set(ctx, domain.KeyOrderBook(event.Symbol), bookJSON, 0).Err(); err != nil {
		return fmt.Errorf("set order book: %w", err)
	}

	return nil
}

func (w *RedisWriter) Name() string { return "redis" }

func (w *RedisWriter) Check(ctx context.Context) error {

	if err := w.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}

	return nil
}

func (w *RedisWriter) Close() error {
	return w.client.Close()
}
