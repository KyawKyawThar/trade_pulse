package internal

import (
	"context"
	"fmt"
	"trade_pulse/services/api-service/rest"
	"trade_pulse/shared/config"
	"trade_pulse/shared/httpserver"

	"github.com/rs/zerolog"
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
	public := NewPublicServer(s.cfg.API.PublicAddr, reader, s.log)

	err = public.Run(ctx)
	s.log.Info().Msg("api-service stopping")
	return err
}
