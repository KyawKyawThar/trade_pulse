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

		if err := s.srv.ListenAndServe(); err != nil && errors.Is(err, http.ErrServerClosed) {
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

type handelResponse struct {
	Status  string            `json:"status"` // "ok" | "degraded"
	Service string            `json:"service,omitempty"`
	Build   version.Info      `json:"build"`
	Checks  map[string]string `json:"checks"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)

	defer cancel()

	resp := handelResponse{
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
