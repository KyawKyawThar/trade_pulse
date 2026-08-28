package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"
	"trade_pulse/shared/version"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// checkTimeout is each individual dependency check's budget. Checks run
// concurrently, so it is also the endpoint's overall worst case.
const checkTimeout = 2 * time.Second

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
	Details map[string]any    `json:"details,omitempty"` // from checkers implementing Detailer
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
	checkers := s.health.snapshot()

	resp := healthResponse{
		Status:  "ok",
		Build:   version.GetInfo(),
		Checks:  make(map[string]string, len(checkers)),
		Details: map[string]any{},
	}

	for _, result := range runChecks(r.Context(), checkers) {
		if result.err != nil {
			resp.Status = "degraded"
			resp.Checks[result.name] = result.err.Error()
		} else {
			resp.Checks[result.name] = "ok"
		}

		if result.detail != nil {
			resp.Details[result.name] = result.detail
		}
	}

	if len(resp.Details) == 0 {
		resp.Details = nil
	}

	code := http.StatusOK

	if resp.Status != "ok" {
		code = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

type checkResult struct {
	name   string
	err    error
	detail any
}

// runChecks runs every checker concurrently, each under its own deadline.
//
// Both properties matter under failure. Sharing one deadline across sequential
// checks lets the first slow dependency spend the whole budget, so every
// checker after it fails with a deadline error and a single sick backend is
// reported as a total outage; running them in series also makes the endpoint's
// worst case the *sum* of its dependencies' timeouts, which is how a health
// probe starts timing out and getting the pod killed. Independent budgets and
// concurrent execution bound the response at one checkTimeout regardless of
// how many dependencies a service grows.
func runChecks(ctx context.Context, checkers []Checker) []checkResult {
	results := make([]checkResult, len(checkers))

	var wg sync.WaitGroup

	for i, c := range checkers {
		wg.Add(1)

		go func(i int, c Checker) {
			defer wg.Done()

			checkCtx, cancel := context.WithTimeout(ctx, checkTimeout)
			defer cancel()

			result := checkResult{name: c.Name(), err: c.Check(checkCtx)}

			if d, ok := c.(Detailer); ok {
				result.detail = d.Details()
			}

			results[i] = result
		}(i, c)
	}

	wg.Wait()

	return results
}
