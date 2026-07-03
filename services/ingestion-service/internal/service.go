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

// Run blocks until ctx is cancelled. In Sprint 1 this becomes an errgroup that
// starts one worker goroutine per configured symbol (BTC/ETH/SOL) and a Kafka
// publisher, all cancelled together via ctx (Architecture § Pattern 1).
func (s *Service) Run(ctx context.Context) error {
	s.log.Info().Msg("ingestion-service started (skeleton — no symbols wired yet)")
	<-ctx.Done()
	s.log.Info().Msg("ingestion-service stopping")
	return nil
}
