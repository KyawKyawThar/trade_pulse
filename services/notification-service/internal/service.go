package internal

import (
	"context"
	"trade_pulse/shared/config"

	"github.com/rs/zerolog"
)

// Service is notification-service's root component.
type Service struct {
	cfg config.Config
	log zerolog.Logger
}

// New constructs the service from loaded config.
func New(cfg config.Config, log zerolog.Logger) *Service {
	return &Service{cfg: cfg, log: log}
}

func (s *Service) Run(ctx context.Context) error {
	s.log.Info().Msg("notification-service started (skeleton — no consumers wired yet)")
	<-ctx.Done()
	s.log.Info().Msg("notification-service stopping")
	return nil
}
