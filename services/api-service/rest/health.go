package rest

import (
	"context"
	"net/http"
	"time"
	"trade_pulse/shared/version"
)

type healthResponse struct {
	Status  string             `json:"status"`
	Service string             `json:"service"`
	Build   version.Info       `json:"build"`
	Checks  map[string]string  `json:"checks"`
	Details healthDetailFields `json:"details"`
}
type healthDetailFields struct {
	KafkaConsumerLag string `json:"kafka_consumer_lag"`
}

func contextWithTimeout(req *http.Request, timeout time.Duration) (context.Context, context.CancelFunc) {

	return context.WithTimeout(req.Context(), timeout)
}

func (r *RedisReader) Health(w http.ResponseWriter, req *http.Request) {

	ctx, cancel := contextWithTimeout(req, 2*time.Second)
	defer cancel()

	resp := healthResponse{
		Status:  "ok",
		Service: "api-service",
		Build:   version.GetInfo(),
		Checks: map[string]string{
			"kafka_consumer_lag": "not_applicable",
		},
		Details: healthDetailFields{
			KafkaConsumerLag: "owned_by_processor_service",
		},
	}

	if err := r.Check(ctx); err != nil {
		resp.Status = "degraded"
		resp.Checks["redis"] = err.Error()
		writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}

	resp.Checks["redis"] = "ok"
	writeJSON(w, http.StatusOK, resp)

}
