package internal

import (
	"context"
	"trade_pulse/shared/config"

	"github.com/rs/zerolog"
)

type Service struct {
	cfg config.Config
	log zerolog.Logger
}

func New(cfg config.Config, log zerolog.Logger) *Service {
	return &Service{cfg: cfg, log: log}
}

func (s *Service) Run(ctx context.Context) error {
	s.log.Info().
		Str("provider", s.cfg.FX.Provider).
		Dur("poll_interval", s.cfg.FX.PollInterval).
		Msg("fx-rate-service started (skeleton — poller not wired yet)")
	<-ctx.Done()
	s.log.Info().Msg("fx-rate-service stopping")
	return nil
}
