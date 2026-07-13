package internal

import (
	"context"
	"fmt"
	"strings"
	"trade_pulse/shared/config"
	"trade_pulse/shared/httpserver"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

type Service struct {
	cfg     config.Config
	log     zerolog.Logger
	ops     *httpserver.Server
	ws      *wsHealth
	symbols []string
}

func New(cfg config.Config, log zerolog.Logger, ops *httpserver.Server) *Service {
	symbols := normalizeSymbols(cfg.Ingestion.Symbols)

	wsh := newWSHealth(symbols)
	ops.RegisterChecker(wsh)
	return &Service{cfg: cfg, log: log, ops: ops, ws: wsh, symbols: symbols}
}

// normalizeSymbols lowercases, trims, and dedupes the configured symbols,
// preserving first-seen order. Binance stream names are lowercase, and a
// mixed-case symbol from config/env would dial a stream that never delivers
// a trade — a silent failure — so normalization happens once here rather
// than trusting every config source to get it right.
func normalizeSymbols(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))

	for _, s := range in {
		sym := strings.ToLower(strings.TrimSpace(s))
		if sym == "" {
			continue
		}
		if _, dup := seen[sym]; dup {
			continue
		}
		seen[sym] = struct{}{}
		out = append(out, sym)
	}
	return out
}

// Run starts one goroutine per symbol under an errgroup derived from ctx. The
// first worker to return an error cancels the group's context, which stops
// every other worker; Run blocks until all of them have exited and returns
// that first error (nil on a clean shutdown)
func (s *Service) Run(ctx context.Context) error {
	symbols := s.symbols
	if len(symbols) == 0 {
		return fmt.Errorf("ingestion: no symbols configured")
	}

	s.log.Info().Strs("symbols", symbols).Msg("ingestion-service starting workers")

	pub, err := NewPublisher(s.cfg.Kafka, s.log)
	if err != nil {
		return fmt.Errorf("kafka publisher: %w", err)
	}
	defer pub.Close()
	s.ops.RegisterChecker(pub)
	g, ctx := errgroup.WithContext(ctx)

	for _, symbol := range symbols {
		g.Go(func() error {
			return s.runSymbolWithReconnect(ctx, symbol, pub)
		})
	}

	err = g.Wait()
	s.log.Info().Msg("ingestion-service stopping")
	return err
}
