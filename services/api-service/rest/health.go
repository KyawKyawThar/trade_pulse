package rest

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"
	"trade_pulse/shared/kafka"
	"trade_pulse/shared/version"
)

// Each dependency gets its own budget. A shared deadline lets the first slow
// check spend the whole thing, so every later check fails with a deadline
// error and one sick backend is reported as a total outage.
const (
	redisCheckTimeout = 2 * time.Second

	// lagCheckTimeout guards the lag path. The production implementation
	// serves a background-refreshed snapshot and returns instantly, but the
	// handler holds an interface: bounding it here means a future
	// implementation that reads through to the broker degrades this endpoint
	// rather than hanging it.
	lagCheckTimeout = 500 * time.Millisecond
)

// LagReporter is the /health view of consumer-group lag. *kafka.LagCache is
// the production implementation — it serves a background-refreshed snapshot,
// so this call never touches the broker.
//
// It is exported so wiring code can hold one as an interface: returning a nil
// *kafka.LagCache into an interface-typed variable produces a non-nil
// interface holding a nil pointer, which would silently defeat the
// not-configured path below.
type LagReporter interface {
	Snapshot() kafka.LagSnapshot
	Check(ctx context.Context) error
}

// pinger is the dependency whose reachability gates readiness. *RedisReader is
// the production implementation.
type pinger interface {
	Check(ctx context.Context) error
}

// ErrLagNotConfigured distinguishes "no broker configured" from "broker
// configured but unhealthy". The first is a deployment choice and stays ok;
// the second is degraded.
var ErrLagNotConfigured = errors.New("not_configured")

// notConfiguredLag is the null object used when no broker is configured, so
// HealthHandler never nil-checks its collaborator.
type notConfiguredLag struct{}

func (notConfiguredLag) Snapshot() kafka.LagSnapshot { return kafka.LagSnapshot{} }
func (notConfiguredLag) Check(context.Context) error { return ErrLagNotConfigured }

type healthResponse struct {
	Status  string            `json:"status"`
	Service string            `json:"service"`
	Build   version.Info      `json:"build"`
	Checks  map[string]string `json:"checks"`
}

// HealthHandler serves the public GET /api/v1/health: a summary of whether
// this service can serve and whether the pipeline behind it is keeping up
// (SPRINT_PLAN.md Sprint 2 task 8, Architecture § API Design).
//
// It is deliberately a summary. The per-partition breakdown, group name and
// member count live on the ops server's /health, which is not publicly
// exposed — internal topology does not belong on an endpoint anyone can curl.
type HealthHandler struct {
	redis pinger
	lag   LagReporter
}

// NewHealthHandler wires the report. A nil lag reporter is replaced with the
// null object, so the handler never nil-checks.
func NewHealthHandler(redis pinger, lag LagReporter) *HealthHandler {
	if lag == nil {
		lag = notConfiguredLag{}
	}

	return &HealthHandler{redis: redis, lag: lag}
}

// Health reports each dependency and maps them to one status.
//
// Redis is api-service's own hard dependency: without it no endpoint can serve
// a response, so a failed ping is 503 and takes the instance out of rotation.
// Consumer lag is different — it describes the pipeline upstream, not this
// process. A lagging or dead processor leaves api-service perfectly able to
// serve (slightly stale) Redis data, so it reports "degraded" with 200 rather
// than removing a healthy instance from the load balancer because another
// service is behind. Alerting on a lag *threshold* belongs to the Prometheus
// work in Sprint 4; this endpoint reports the number.
func (h *HealthHandler) Health(w http.ResponseWriter, req *http.Request) {
	resp := healthResponse{
		Status:  "ok",
		Service: "api-service",
		Build:   version.GetInfo(),
		Checks:  map[string]string{},
	}

	lagCtx, cancelLag := context.WithTimeout(req.Context(), lagCheckTimeout)
	defer cancelLag()

	if h.lagStatus(lagCtx, &resp) {
		resp.Status = "degraded"
	}

	ctx, cancel := context.WithTimeout(req.Context(), redisCheckTimeout)
	defer cancel()

	if err := h.redis.Check(ctx); err != nil {
		resp.Status = "degraded"
		resp.Checks["redis"] = err.Error()
		writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}

	resp.Checks["redis"] = "ok"
	writeJSON(w, http.StatusOK, resp)
}

// lagStatus fills in the lag check and reports whether it is degraded. The
// read is O(1) against the cached snapshot — no broker call on the request
// path, so an unreachable broker cannot slow this endpoint down.
func (h *HealthHandler) lagStatus(ctx context.Context, resp *healthResponse) bool {
	err := h.lag.Check(ctx)

	switch {
	case errors.Is(err, ErrLagNotConfigured):
		resp.Checks["kafka_consumer_lag"] = "not_configured"
		return false

	case err != nil:
		resp.Checks["kafka_consumer_lag"] = err.Error()
		return true

	default:
		resp.Checks["kafka_consumer_lag"] = strconv.FormatInt(h.lag.Snapshot().Lag.Total, 10)
		return false
	}
}
