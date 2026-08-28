package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"trade_pulse/shared/kafka"

	"github.com/redis/go-redis/v9"
)

// ctxAwareRedis behaves like the real go-redis client: it honours the context
// deadline instead of ignoring it. The fake in handler_test.go does not, which
// is precisely why the shared-deadline cascade went unnoticed.
type ctxAwareRedis struct{ pingErr error }

func (c ctxAwareRedis) Get(ctx context.Context, _ string) *redis.StringCmd {
	return redis.NewStringResult("", ctx.Err())
}

func (c ctxAwareRedis) Ping(ctx context.Context) *redis.StatusCmd {
	if err := ctx.Err(); err != nil {
		return redis.NewStatusResult("", err)
	}
	return redis.NewStatusResult("PONG", c.pingErr)
}

func (c ctxAwareRedis) Close() error { return nil }

// stubLag serves a fixed snapshot, standing in for *kafka.LagCache.
type stubLag struct {
	snapshot kafka.LagSnapshot
	err      error
	blockFor time.Duration
}

func (s stubLag) Snapshot() kafka.LagSnapshot { return s.snapshot }

func (s stubLag) Check(ctx context.Context) error {
	if s.blockFor > 0 {
		select {
		case <-time.After(s.blockFor):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.err
}

func getHealth(t *testing.T, h *HealthHandler) (*httptest.ResponseRecorder, healthResponse) {
	t.Helper()

	rec := httptest.NewRecorder()
	h.Health(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))

	var got healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	return rec, got
}

func TestHealthReportsRedisPingAndConsumerLag(t *testing.T) {
	lag := stubLag{snapshot: kafka.LagSnapshot{Lag: kafka.ConsumerLag{Group: "processor-service", Total: 42, Members: 1}}}

	rec, got := getHealth(t, NewHealthHandler(NewRedisReaderWithClient(ctxAwareRedis{}), lag))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	if got.Status != "ok" || got.Checks["redis"] != "ok" {
		t.Fatalf("health = %+v, want ok with a healthy Redis check", got)
	}

	if got.Checks["kafka_consumer_lag"] != "42" {
		t.Fatalf("kafka_consumer_lag = %q, want \"42\"", got.Checks["kafka_consumer_lag"])
	}
}

// Internal topology — group names, member counts, partition offsets — must not
// reach an endpoint anyone can curl. It lives on the ops server instead.
func TestPublicHealthDoesNotLeakInternalTopology(t *testing.T) {
	lag := stubLag{snapshot: kafka.LagSnapshot{Lag: kafka.ConsumerLag{
		Group:      "processor-service",
		Topic:      "trades.raw",
		Members:    3,
		Total:      42,
		Partitions: []kafka.PartitionLag{{Partition: 0, Lag: 42}},
	}}}

	rec, _ := getHealth(t, NewHealthHandler(NewRedisReaderWithClient(ctxAwareRedis{}), lag))

	for _, leaked := range []string{"processor-service", "trades.raw", "partition", "members"} {
		if body := rec.Body.String(); contains(body, leaked) {
			t.Fatalf("public health body leaks %q: %s", leaked, body)
		}
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// A lagging or unreachable pipeline must not take a serving api-service
// instance out of rotation: degraded, but still 200.
func TestHealthDegradesWithoutFailingWhenLagUnhealthy(t *testing.T) {
	lag := stubLag{err: errors.New("lag reading stale (45s old)")}

	rec, got := getHealth(t, NewHealthHandler(NewRedisReaderWithClient(ctxAwareRedis{}), lag))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (lag is not this service's own dependency)", rec.Code, http.StatusOK)
	}

	if got.Status != "degraded" || got.Checks["redis"] != "ok" {
		t.Fatalf("health = %+v, want degraded overall with Redis still ok", got)
	}
}

// Redis is api-service's own hard dependency, so its failure is a 503.
func TestHealthReturns503WhenRedisPingFails(t *testing.T) {
	handler := NewHealthHandler(NewRedisReaderWithClient(ctxAwareRedis{pingErr: errors.New("redis down")}), stubLag{})

	rec, got := getHealth(t, handler)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}

	if got.Status != "degraded" || got.Checks["redis"] == "ok" {
		t.Fatalf("health = %+v, want degraded Redis check", got)
	}
}

// The regression this design exists to prevent: a wedged broker must not
// consume Redis's budget and get a healthy instance pulled from rotation.
// The lag read is served from cache, so it cannot block at all.
func TestSlowLagCheckNeverFailsTheRedisCheck(t *testing.T) {
	handler := NewHealthHandler(
		NewRedisReaderWithClient(ctxAwareRedis{}),
		stubLag{blockFor: 5 * time.Second, err: errors.New("broker unreachable")},
	)

	start := time.Now()
	rec, got := getHealth(t, handler)
	elapsed := time.Since(start)

	if rec.Code != http.StatusServiceUnavailable && got.Checks["redis"] != "ok" {
		t.Fatalf("redis = %v, want ok: a slow lag check must not poison it", got.Checks["redis"])
	}

	if got.Checks["redis"] != "ok" {
		t.Fatalf("health = %+v, want Redis reported healthy", got)
	}

	if elapsed > redisCheckTimeout {
		t.Fatalf("elapsed = %v, want under %v: the lag path must not block the request", elapsed, redisCheckTimeout)
	}
}

func TestHealthReportsNotConfiguredWithoutABroker(t *testing.T) {
	rec, got := getHealth(t, NewHealthHandler(NewRedisReaderWithClient(ctxAwareRedis{}), nil))

	if rec.Code != http.StatusOK || got.Status != "ok" {
		t.Fatalf("status = %d/%q, want 200/ok", rec.Code, got.Status)
	}

	if got.Checks["kafka_consumer_lag"] != "not_configured" {
		t.Fatalf("kafka_consumer_lag = %q, want not_configured", got.Checks["kafka_consumer_lag"])
	}
}

// A nil *kafka.LagCache boxed into the interface is not a nil interface; the
// handler must still degrade rather than panic.
func TestHealthSurvivesATypedNilLagReporter(t *testing.T) {
	var cache *kafka.LagCache

	rec, got := getHealth(t, NewHealthHandler(NewRedisReaderWithClient(ctxAwareRedis{}), cache))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	if got.Checks["kafka_consumer_lag"] == "" {
		t.Fatalf("health = %+v, want the lag check reported, not a panic", got)
	}
}
