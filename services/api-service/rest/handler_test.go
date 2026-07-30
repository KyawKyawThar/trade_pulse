package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"trade_pulse/shared/domain"

	"github.com/go-chi/chi"
	"github.com/redis/go-redis/v9"
)

type fakeRedisClient struct {
	values  map[string]string
	getKeys []string
	getErr  error
	pingErr error
}

func (f *fakeRedisClient) Get(_ context.Context, key string) *redis.StringCmd {
	f.getKeys = append(f.getKeys, key)

	if f.getErr != nil {
		return redis.NewStringResult("", f.getErr)
	}

	value, ok := f.values[key]

	if !ok {
		return redis.NewStringResult("", redis.Nil)
	}
	return redis.NewStringResult(value, nil)
}
func (c *fakeRedisClient) Ping(_ context.Context) *redis.StatusCmd {
	return redis.NewStatusResult("PONG", c.pingErr)
}

func (c *fakeRedisClient) Close() error { return nil }

func requestWithSymbol(method, path, symbol string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("symbol", symbol)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))
}

func TestLatestTradeReadsRedisLatestTradeKey(t *testing.T) {
	trade := domain.TradeEvent{
		Symbol:    "BTCUSDT",
		Price:     65000.50,
		Quantity:  0.5,
		Side:      domain.SideBuy,
		TradeID:   42,
		Notional:  32500.35,
		EventTime: time.Unix(20, 0).UTC(),
	}

	raw, err := json.Marshal(trade)

	if err != nil {
		t.Fatalf("marshal trade: %v", err)
	}

	client := &fakeRedisClient{values: map[string]string{
		domain.KeyLatestTrade("BTCUSDT"): string(raw),
	}}

	reader := NewRedisReaderWithClient(client)

	rec := httptest.NewRecorder()

	reader.LatestTrade(rec, requestWithSymbol(http.MethodGet, "/api/v1/trades/btcusdt", "btcusdt"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	if len(client.getKeys) != 1 || client.getKeys[0] != domain.KeyLatestTrade("BTCUSDT") {
		t.Fatalf("GET keys = %v, want [%s]", client.getKeys, domain.KeyLatestTrade("BTCUSDT"))
	}

	var got tradeResponse

	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if got.Symbol != "BTCUSDT" || got.Source != "redis" || len(got.Trades) != 1 || got.Trades[0] != trade {
		t.Fatalf("response = %+v, want one Redis trade for BTCUSDT", got)
	}
}

func TestOrderBookReadsRedisOrderBookKey(t *testing.T) {

	book := domain.OrderBook{
		Symbol:     "ETHUSDT",
		Bids:       []domain.PriceLevel{{Price: 3000, Quantity: 2}},
		Asks:       []domain.PriceLevel{{Price: 3001, Quantity: 1}},
		LastUpdate: time.Unix(30, 0).UTC(),
	}

	raw, err := json.Marshal(book)

	if err != nil {
		t.Fatalf("marshal book: %v", err)
	}

	client := &fakeRedisClient{values: map[string]string{
		domain.KeyOrderBook("ETHUSDT"): string(raw),
	}}

	reader := NewRedisReaderWithClient(client)
	rec := httptest.NewRecorder()

	reader.OrderBook(rec, requestWithSymbol(http.MethodGet, "/api/v1/orderbook/ethusdt", "ethusdt"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	if len(client.getKeys) != 1 || client.getKeys[0] != domain.KeyOrderBook("ETHUSDT") {
		t.Fatalf("GET keys = %v, want [%s]", client.getKeys, domain.KeyOrderBook("ETHUSDT"))
	}

	var got orderBookResponse

	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if got.Symbol != "ETHUSDT" || got.Source != "redis" || got.BestAsk == nil || got.BestBid == nil {
		t.Fatalf("response = %+v, want best bid/ask from Redis book", got)
	}
}

func TestRESTHandlersReturnNotFoundOnMissingRedisKey(t *testing.T) {
	reader := NewRedisReaderWithClient(&fakeRedisClient{
		values: map[string]string{},
	})

	for name, handler := range map[string]http.HandlerFunc{
		"trade":      reader.LatestTrade,
		"order_book": reader.OrderBook,
	} {
		rec := httptest.NewRecorder()
		handler(rec, requestWithSymbol(http.MethodGet, "/", "SOLUSDT"))

		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want %d", name, rec.Code, http.StatusNotFound)
		}
	}
}

func TestRedisReaderCheckPingsRedis(t *testing.T) {
	wantErr := errors.New("redis timeout")
	reader := NewRedisReaderWithClient(&fakeRedisClient{pingErr: wantErr})

	if err := reader.Check(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Check() error = %v, want %v", err, wantErr)
	}
}

func TestHealthReturnsDegradedWhenRedisPingFails(t *testing.T) {
	reader := NewRedisReaderWithClient(&fakeRedisClient{pingErr: errors.New("redis down")})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)

	reader.Health(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}

	var got healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if got.Status != "degraded" || got.Checks["redis"] == "ok" {
		t.Fatalf("health response = %+v, want degraded Redis check", got)
	}
}

func TestHealthReturnsRedisPingAndKafkaLagOwnership(t *testing.T) {

	reader := NewRedisReaderWithClient(&fakeRedisClient{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)

	reader.Health(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var got healthResponse

	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if got.Status != "ok" || got.Checks["redis"] != "ok" {
		t.Fatalf("health response = %+v, want ok Redis check", got)
	}

	if got.Checks["kafka_consumer_lag"] != "not_applicable" {
		t.Fatalf("kafka_consumer_lag check = %q, want not_applicable", got.Checks["kafka_consumer_lag"])
	}
}
