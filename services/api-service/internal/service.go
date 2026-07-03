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

	s.log.Info().Msg("api-service started (skeleton — REST/WS not wired yet)")
	<-ctx.Done()
	s.log.Info().Msg("api-service stopping")
	return nil
}
