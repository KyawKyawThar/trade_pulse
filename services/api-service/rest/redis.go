package rest

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Ping(ctx context.Context) *redis.StatusCmd
	Close() error
}

type RedisReader struct {
	client RedisClient
}

func NewRedisReader(addr, password string, db int) (*RedisReader, error) {

	if addr == "" {
		return nil, fmt.Errorf("redis: addr is required")
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return NewRedisReaderWithClient(client), nil

}

// NewRedisReaderWithClient builds a reader around an existing client, primarily
// for tests.
func NewRedisReaderWithClient(client RedisClient) *RedisReader {
	return &RedisReader{client: client}
}
func (r *RedisReader) Name() string { return "redis" }

// Check confirms Redis is reachable.
func (r *RedisReader) Check(ctx context.Context) error {
	if err := r.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

func (r *RedisReader) Close() error { return r.client.Close() }
