package internal

import (
	"context"
	"trade_pulse/shared/config"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

var symbols = []string{"btcusdt", "ethusdt", "solusdt"}

type Service struct {
	cfg config.Config
	log zerolog.Logger
}

func New(cfg config.Config, log zerolog.Logger) *Service {
	return &Service{cfg: cfg, log: log}
}

// Run starts one goroutine per symbol under an errgroup derived from ctx. The
// first worker to return an error cancels the group's context, which stops
// every other worker; Run blocks until all of them have exited and returns
// that first error (nil on a clean shutdown)
func (s *Service) Run(ctx context.Context) error {

	s.log.Info().Strs("symbols", symbols).Msg("ingestion-service starting workers")

	g, ctx := errgroup.WithContext(ctx)

	for _, symbol := range symbols {
		g.Go(func() error {
			return s.runSymbol(ctx, symbol)
		})
	}

	err := g.Wait()
	s.log.Info().Msg("ingestion-service stopping")
	return err
}
