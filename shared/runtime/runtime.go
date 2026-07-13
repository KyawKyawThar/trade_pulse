package runtime

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// Component is any long-lived unit of work that runs until its context is
// cancelled and then returns. The ops HTTP server and each service's core loop
// both satisfy this.
type Component func(context.Context) error

// SignalContext returns a context that is cancelled the first time the process
// receives SIGINT or SIGTERM. A second signal restores default behavior (hard
// kill), so an operator can always force-quit a wedged drain.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

func Run(ctx context.Context, log zerolog.Logger, components ...Component) error {

	g, gctx := errgroup.WithContext(ctx)

	for _, c := range components {
		g.Go(func() error { return c(gctx) })
	}

	err := g.Wait()

	// ctx (not gctx) tells us whether the outer caller asked for shutdown
	// (SIGINT/SIGTERM). gctx is always cancelled by the time we get here
	// whenever any component returned an error, so checking it would mask
	// every real failure as a clean shutdown.
	if err != nil && ctx.Err() == nil {
		log.Error().Err(err).Msg("component failed; shutting down")

		return err
	}
	log.Info().Msg("shutdown complete")
	return nil
}
