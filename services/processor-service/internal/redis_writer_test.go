package internal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
	"trade_pulse/shared/domain"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

type redisSetCall struct {
	key        string
	value      any
	expiration time.Duration
}

type recordingRedisClient struct {
	sets     []redisSetCall
	setErr   error
	pingErr  error
	closeErr error
}

func (c *recordingRedisClient) Set(_ context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	c.sets = append(c.sets, redisSetCall{key: key, value: value, expiration: expiration})

	return redis.NewStatusResult("OK", c.setErr)
}

func (c *recordingRedisClient) Ping(_ context.Context) *redis.StatusCmd {
	return redis.NewStatusResult("PONG", c.pingErr)
}

func (c *recordingRedisClient) Close() error { return c.closeErr }

func TestRedisWriterWriteStoresLatestTradePriceAndBook(t *testing.T) {

	client := &recordingRedisClient{}

	books := NewOrderBookStore()

	books.Update("BTCUSDT", domain.OrderBook{
		Symbol:     "BTCUSDT",
		Bids:       []domain.PriceLevel{{Price: 65000, Quantity: 1}},
		Asks:       []domain.PriceLevel{{Price: 65001, Quantity: 2}},
		LastUpdate: time.Unix(10, 0).UTC(),
	})
	writer := NewRedisWriterWithClient(client, books, zerolog.Nop())

	event := domain.TradeEvent{
		Symbol:    "BTCUSDT",
		Price:     65000.50,
		Quantity:  0.5,
		Side:      domain.SideBuy,
		TradeID:   42,
		Notional:  32500.25,
		EventTime: time.Unix(10, 0).UTC(),
	}

	if err := writer.Write(context.Background(), event); err != nil {
		t.Fatalf("Write() error = %v want nil", err)
	}

	if len(client.sets) != 3 {
		t.Fatalf("SET calls = %d,want 3", len(client.sets))
	}

	if client.sets[0].key != domain.KeyLatestTrade("BTCUSDT") {
		t.Errorf("latest trade key = %q, want %q", client.sets[0].key, domain.KeyLatestTrade("BTCUSDT"))
	}

	if client.sets[1].key != domain.KeyPrice("BTCUSDT") {
		t.Errorf("price key = %q, want %q", client.sets[1].key, domain.KeyPrice("BTCUSDT"))
	}

	if client.sets[1].value != event.Price {
		t.Errorf("price value = %v, want %v", client.sets[1].value, event.Price)
	}

	if client.sets[2].key != domain.KeyOrderBook("BTCUSDT") {
		t.Errorf("order book key = %q, want %q", client.sets[2].key, domain.KeyOrderBook("BTCUSDT"))
	}

	for i, call := range client.sets {
		if call.expiration != 0 {
			t.Errorf("SET call %d expiration = %v, want 0", i, call.expiration)
		}
	}

	var gotTrade domain.TradeEvent

	if err := json.Unmarshal(client.sets[0].value.([]byte), &gotTrade); err != nil {
		t.Fatalf("latest trade JSON: %v", err)
	}

	if gotTrade != event {
		t.Errorf("latest trade = %+v, want %+v", gotTrade, event)
	}

	var gotBook domain.OrderBook
	if err := json.Unmarshal(client.sets[2].value.([]byte), &gotBook); err != nil {
		t.Fatalf("order book JSON: %v", err)
	}
	if gotBook.Symbol != "BTCUSDT" || len(gotBook.Bids) != 1 || len(gotBook.Asks) != 1 {
		t.Errorf("order book snapshot = %+v, want populated BTCUSDT book", gotBook)
	}
}

func TestRedisWriterWriteSkipsMissingBook(t *testing.T) {
	client := &recordingRedisClient{}

	writer := NewRedisWriterWithClient(client, NewOrderBookStore(), zerolog.Nop())

	if err := writer.Write(context.Background(), domain.TradeEvent{Symbol: "ETHUSDT", Price: 3000}); err != nil {
		t.Fatalf("Write() error = %v, want nil", err)
	}
	if len(client.sets) != 2 {
		t.Fatalf("SET calls = %d, want 2 without an order-book snapshot", len(client.sets))
	}
}

func TestRedisWriteReturnsSetError(t *testing.T) {
	wantErr := errors.New("redis down")
	client := &recordingRedisClient{setErr: wantErr}
	writer := NewRedisWriterWithClient(client, nil, zerolog.Nop())

	if err := writer.Write(context.Background(), domain.TradeEvent{Symbol: "SOLUSDT"}); !errors.Is(err, wantErr) {
		t.Fatalf("Write() error =%v, want %v", err, wantErr)
	}
}

func TestRedisWriterCheckPingsRedis(t *testing.T) {
	wantErr := errors.New("timeout")
	writer := NewRedisWriterWithClient(&recordingRedisClient{pingErr: wantErr}, nil, zerolog.Nop())

	if err := writer.Check(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Check() error = %v, want %v", err, wantErr)
	}
}
