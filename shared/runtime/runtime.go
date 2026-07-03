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

	g, ctx := errgroup.WithContext(ctx)

	for _, c := range components {
		c := c
		g.Go(func() error { return c(ctx) })
	}

	err := g.Wait()

	if err != nil && ctx.Err() == nil {
		log.Error().Err(err).Msg("component failed; shutting down")

		return err
	}
	log.Info().Msg("shutdown complete")
	return nil
}
