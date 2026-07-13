package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
	"trade_pulse/shared/version"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

type Server struct {
	log    zerolog.Logger
	srv    *http.Server
	mux    *http.ServeMux
	health *healthRegistry
}

func New(addr string, log zerolog.Logger) *Server {

	s := &Server{
		log:    log,
		mux:    http.NewServeMux(),
		health: &healthRegistry{},
	}

	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/health/live", s.handleLive)
	s.mux.Handle("/metrics", promhttp.Handler())

	s.srv = &http.Server{
		Addr:              addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func (s *Server) RegisterChecker(c Checker) { s.health.add(c) }

func (s *Server) Mount(pattern string, h http.Handler) { s.mux.Handle(pattern, h) }

func (s *Server) Start(ctx context.Context) error {

	errCh := make(chan error, 1)

	go func() {
		s.log.Info().Str("addr", s.srv.Addr).Msg("ops http server listening")

		err := s.srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.log.Info().Msg("ops http server shutting down")
		return s.srv.Shutdown(shutdownCtx)

	}
}

type healthResponse struct {
	Status  string            `json:"status"` // "ok" | "degraded"
	Service string            `json:"service,omitempty"`
	Build   version.Info      `json:"build"`
	Checks  map[string]string `json:"checks"`
}

// handleLive is the liveness probe: it answers 200 whenever the process is up
// and serving HTTP, with no dependency checks. Point orchestrator liveness
// probes (e.g. Kubernetes livenessProbe) here — a restart only helps when the
// process itself is wedged. Dependency trouble (broker down, WS reconnecting
// with backoff) belongs to /health; restarting the pod for it would throw away
// the reconnect/backoff state and hammer the upstream instead of helping.
func (s *Server) handleLive(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(struct {
		Status string       `json:"status"`
		Build  version.Info `json:"build"`
	}{Status: "ok", Build: version.GetInfo()})
}

// handleHealth is the readiness/dependency report: it runs every registered
// checker and returns 503 with per-check detail if any dependency is unhealthy.
// Use it for readiness probes and monitoring — never for liveness (see
// handleLive).
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)

	defer cancel()

	resp := healthResponse{
		Status: "ok",
		Build:  version.GetInfo(),
		Checks: map[string]string{},
	}
	for _, c := range s.health.snapshot() {

		if err := c.Check(ctx); err != nil {
			resp.Status = "degraded"
			resp.Checks[c.Name()] = err.Error()
		} else {
			resp.Checks[c.Name()] = "ok"
		}
	}

	code := http.StatusOK

	if resp.Status != "ok" {
		code = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}
