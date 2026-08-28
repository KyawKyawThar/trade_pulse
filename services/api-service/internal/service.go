package internal

import (
	"context"
	"fmt"
	"trade_pulse/services/api-service/rest"
	"trade_pulse/shared/config"
	"trade_pulse/shared/domain"
	"trade_pulse/shared/httpserver"
	"trade_pulse/shared/kafka"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

type Service struct {
	cfg config.Config
	log zerolog.Logger
	ops *httpserver.Server
}

func New(cfg config.Config, log zerolog.Logger, ops *httpserver.Server) *Service {
	return &Service{cfg: cfg, log: log, ops: ops}
}

func (s *Service) Run(ctx context.Context) error {

	s.log.Info().Msg("api-service starting")

	reader, err := rest.NewRedisReader(s.cfg.Redis.Addr, s.cfg.Redis.Password, s.cfg.Redis.DB)

	if err != nil {
		return fmt.Errorf("redis reader: %w", err)
	}

	defer func() {
		if err := reader.Close(); err != nil {

			s.log.Warn().Err(err).Msg("redis reader close")
		}
	}()

	s.ops.RegisterChecker(reader)

	eg, ctx := errgroup.WithContext(ctx)

	lag := s.startLagCache(ctx, eg)

	public := NewPublicServer(s.cfg.API.PublicAddr, reader, rest.NewHealthHandler(reader, lag), s.log)

	eg.Go(func() error { return public.Run(ctx) })

	err = eg.Wait()
	s.log.Info().Msg("api-service stopping")

	return err
}

// startLagCache brings up the background consumer-lag refresher and returns
// the handle the health endpoints read from, or nil when no broker is
// configured.
//
// api-service observes processor-service's group through the broker admin API
// and never joins it: a second member would trigger a rebalance and take
// partitions from the real consumer (see shared/kafka). The reading is
// refreshed on a ticker and served from cache, so neither probe traffic nor an
// unreachable broker can reach the request path.
//
// A broker that cannot be dialled at startup is not fatal. api-service's job
// is serving Redis-backed reads; losing the lag *reading* degrades one field
// of one endpoint and must not stop the service from coming up.
func (s *Service) startLagCache(ctx context.Context, eg *errgroup.Group) rest.LagReporter {
	reader, err := kafka.NewLagReader(s.cfg.Kafka.Brokers, domain.ConsumerGroupProcessor, domain.TopicTradesRaw)

	if err != nil {
		s.log.Warn().Err(err).Msg("kafka lag reader unavailable; /health will report consumer lag as not_configured")
		return nil
	}

	cache := kafka.NewLagCache(reader, s.cfg.Kafka.LagRefreshInterval, s.log)

	s.ops.RegisterChecker(cache)

	eg.Go(func() error {
		defer reader.Close()
		return cache.Run(ctx)
	})

	return cache
}
