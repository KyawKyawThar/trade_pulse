package internal

import (
	"context"
	"fmt"
	"time"

	"trade_pulse/shared/config"
	"trade_pulse/shared/retry"

	"github.com/rs/zerolog"
)

// rateFetcher performs one poll against the FX provider. Kept as a func so the
// poll loop can be driven with a fake in tests instead of a live provider HTTP
// call; fetchRates is the production implementation.
type rateFetcher func(ctx context.Context) error

type Service struct {
	cfg   config.Config
	log   zerolog.Logger
	fetch rateFetcher
}

func New(cfg config.Config, log zerolog.Logger) *Service {
	s := &Service{cfg: cfg, log: log}
	s.fetch = s.fetchRates
	return s
}

// Run polls the FX provider on FX.PollInterval for as long as ctx is live,
// reconnecting through the shared jittered-backoff loop: pollSession returns on
// the first fetch failure, and retry.WithBackoff waits an escalating,
// jittered delay before starting a fresh session — the same reconnect policy
// ingestion-service uses for its Binance WebSocket. A session that polls
// cleanly past retry.DefaultResetAfter is treated as recovered, so the backoff
// restarts from Base after a provider outage clears.
func (s *Service) Run(ctx context.Context) error {
	s.log.Info().
		Str("provider", s.cfg.FX.Provider).
		Dur("poll_interval", s.cfg.FX.PollInterval).
		Msg("fx-rate-service started")

	err := retry.WithBackoff(ctx, s.log, s.pollSession)

	s.log.Info().Msg("fx-rate-service stopping")
	return err
}

// pollSession fetches rates every FX.PollInterval until ctx is cancelled
// (returns nil) or a fetch fails (returns the error so the caller can back off
// before reconnecting).
func (s *Service) pollSession(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.FX.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.fetch(ctx); err != nil {
				return fmt.Errorf("fx poll: %w", err)
			}
		}
	}
}

// fetchRates pulls the latest rates from the configured provider. The provider
// HTTP integration (openexchangerates | exchangerate.host | ecb) is the next
// task; the reconnect/backoff plumbing around it is already live, so wiring the
// real request here is all that remains. Returning an error from this call is
// what trips the backoff loop.
func (s *Service) fetchRates(_ context.Context) error {
	s.log.Debug().Str("provider", s.cfg.FX.Provider).Msg("fx rate fetch (provider request not wired yet)")
	return nil
}
