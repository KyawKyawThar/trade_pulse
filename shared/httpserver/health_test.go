package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// slowChecker models a wedged dependency: it burns whatever budget it is
// given.
type slowChecker struct{ name string }

func (s slowChecker) Name() string { return s.name }
func (s slowChecker) Check(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// ctxChecker reports whatever its context says, the way a real client library
// does when handed an already-expired context.
type ctxChecker struct{ name string }

func (c ctxChecker) Name() string                    { return c.name }
func (c ctxChecker) Check(ctx context.Context) error { return ctx.Err() }

type detailChecker struct{ name string }

func (d detailChecker) Name() string                { return d.name }
func (d detailChecker) Check(context.Context) error { return nil }
func (d detailChecker) Details() any                { return map[string]any{"total": 42} }

func getOpsHealth(t *testing.T, checkers ...Checker) (*httptest.ResponseRecorder, map[string]any, time.Duration) {
	t.Helper()

	s := New(":0", zerolog.New(io.Discard))

	for _, c := range checkers {
		s.RegisterChecker(c)
	}

	rec := httptest.NewRecorder()
	start := time.Now()
	s.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	elapsed := time.Since(start)

	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	return rec, body, elapsed
}

// The regression that motivated per-check deadlines: with one shared context
// the first slow checker spent the whole budget, so every later check failed
// with a deadline error and one sick dependency was reported as a total
// outage.
func TestOneSlowCheckerDoesNotPoisonTheOthers(t *testing.T) {
	_, body, _ := getOpsHealth(t, slowChecker{name: "kafka"}, ctxChecker{name: "redis"})

	checks, ok := body["checks"].(map[string]any)

	if !ok {
		t.Fatalf("body = %+v, want a checks map", body)
	}

	if checks["redis"] != "ok" {
		t.Fatalf("redis = %v, want ok: a healthy dependency must not inherit a slow one's deadline", checks["redis"])
	}

	if checks["kafka"] == "ok" {
		t.Fatal("kafka = ok, want the slow dependency reported as unhealthy")
	}
}

// Serial checks make the endpoint's worst case the sum of its dependencies'
// timeouts, which is how a health probe starts timing out and gets the pod
// killed.
func TestChecksRunConcurrently(t *testing.T) {
	_, _, elapsed := getOpsHealth(t,
		slowChecker{name: "a"}, slowChecker{name: "b"}, slowChecker{name: "c"})

	if elapsed >= 2*checkTimeout {
		t.Fatalf("elapsed = %v with 3 wedged checkers; want ~%v (concurrent), not the sum", elapsed, checkTimeout)
	}
}

func TestUnhealthyCheckerReturns503(t *testing.T) {
	failing := CheckerFunc("redis", func(context.Context) error { return errors.New("redis down") })

	rec, body, _ := getOpsHealth(t, failing)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	if body["status"] != "degraded" {
		t.Fatalf("status = %v, want degraded", body["status"])
	}
}

func TestHealthyServiceReturns200(t *testing.T) {
	rec, body, _ := getOpsHealth(t, CheckerFunc("redis", func(context.Context) error { return nil }))

	if rec.Code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("status = %d/%v, want 200/ok", rec.Code, body["status"])
	}
}

// Internal topology belongs on the ops endpoint, which is not publicly
// exposed.
func TestDetailerAttachesStructuredDetail(t *testing.T) {
	_, body, _ := getOpsHealth(t, detailChecker{name: "kafka_consumer_lag"})

	details, ok := body["details"].(map[string]any)

	if !ok {
		t.Fatalf("body = %+v, want a details map", body)
	}

	lag, ok := details["kafka_consumer_lag"].(map[string]any)

	if !ok || lag["total"] != float64(42) {
		t.Fatalf("details = %+v, want the checker's structured detail", details)
	}
}

// Checkers without detail must not leave an empty object in the response.
func TestDetailsOmittedWhenNoCheckerProvidesAny(t *testing.T) {
	_, body, _ := getOpsHealth(t, CheckerFunc("redis", func(context.Context) error { return nil }))

	if _, present := body["details"]; present {
		t.Fatalf("body = %+v, want no details key", body)
	}
}

// Liveness must never consult a dependency: restarting a pod because a broker
// is down throws away reconnect state and hammers the upstream.
func TestLivenessIgnoresDependencies(t *testing.T) {
	s := New(":0", zerolog.New(io.Discard))
	s.RegisterChecker(CheckerFunc("redis", func(context.Context) error { return errors.New("down") }))

	rec := httptest.NewRecorder()
	s.handleLive(rec, httptest.NewRequest(http.MethodGet, "/health/live", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d even with a failing dependency", rec.Code, http.StatusOK)
	}
}
