package internal

import (
	"context"
	"errors"
	"net/http"
	"time"
	"trade_pulse/services/api-service/rest"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/go-chi/chi"
	"github.com/rs/zerolog"
)

type PublicServer struct {
	log zerolog.Logger
	srv *http.Server
}

func NewPublicServer(addr string, reader *rest.RedisReader, log zerolog.Logger) *PublicServer {

	router := NewRouter(reader)

	return &PublicServer{
		log: log,
		srv: &http.Server{
			Addr:              addr,
			Handler:           router,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

func NewRouter(reader *rest.RedisReader) http.Handler {

	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(middleware.ClientIPFromHeader("CF-Connecting-IP"))
	router.Use(middleware.Recoverer)

	router.Route("/api/v1", func(r chi.Router) {
		r.Get("/trades/{symbol}", reader.LatestTrade)
		r.Get("/orderbook/{symbol}", reader.OrderBook)
	})

	return router
}

func (s *PublicServer) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		s.log.Info().Str("addr", s.srv.Addr).Msg("public api server listening")
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
		s.log.Info().Msg("public api server shutting down")
		return s.srv.Shutdown(shutdownCtx)
	}
}
