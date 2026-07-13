package internal

import (
	"context"
	"math/rand/v2"
	"time"
)

const (
	// backoffBase is the ceiling after the first failed connection attempt;
	// each subsequent consecutive failure doubles it, up to backoffMax.
	backoffBase = 1 * time.Second
	// backoffMax caps the delay so even a long outage keeps retrying about
	// twice a minute rather than drifting into multi-minute gaps
	backoffMax = 30 * time.Second
	// backoffResetAfter is how long a connection must stay up before its
	// next drop is treated as a fresh incident. A link that survives this
	// long has clearly recovered, so the backoff sequence restarts from
	// backoffBase instead of continuing to escalate from where the previous
	// outage left off
	backoffResetAfter = 2 * time.Minute
)

// Backoff produces jittered exponential retry delays: each consecutive
// failure doubles the ceiling from Base up to Max, and Observe restarts the
// sequence once a connection has stayed up for ResetAfter. Not safe for
// concurrent use; each worker owns its own instance.
type Backoff struct {
	Base, Max, ResetAfter time.Duration

	// rng returns a uniform random int64 in [0, n), defaulting to
	// rand.Int64N; tests inject it to make delays deterministic.
	rng func(n int64) int64

	attempt int
}

func newBackoff() *Backoff {
	return &Backoff{Base: backoffBase, Max: backoffMax, ResetAfter: backoffResetAfter}
}

// Next returns the jittered delay for the current failure and advances the
// sequence.
func (b *Backoff) Next() time.Duration {
	d := b.ceiling()
	b.attempt++
	return b.jitter(d)
}

// Observe feeds back how long the last connection stayed up; at ResetAfter
// or beyond the link is considered recovered and the sequence restarts.
func (b *Backoff) Observe(uptime time.Duration) {
	if uptime >= b.ResetAfter {
		b.attempt = 0
	}
}

// ceiling is the un-jittered delay for the current attempt: Base doubled per
// consecutive failure, saturating at Max.
func (b *Backoff) ceiling() time.Duration {
	d := b.Base
	for i := 0; i < b.attempt && d < b.Max; i++ {
		d *= 2
	}
	return min(d, b.Max)
}

// jitter returns a uniform random duration in [0, d] (full jitter).
func (b *Backoff) jitter(d time.Duration) time.Duration {
	rng := b.rng
	if rng == nil {
		rng = rand.Int64N
	}
	return time.Duration(rng(int64(d) + 1))
}

func (s *Service) runSymbolWithReconnect(ctx context.Context, symbol string, pub tradePublisher) error {

	log := s.log.With().Str("symbol", symbol).Logger()

	backoff := newBackoff()

	for {
		start := time.Now()
		err := s.runSymbol(ctx, symbol, pub)

		if ctx.Err() != nil {
			return nil
		}

		// err may be nil here if runSymbol grows a clean-return path, but
		// returning would silently kill this symbol's worker while the
		// errgroup keeps the others running; retry regardless.

		backoff.Observe(time.Since(start))
		delay := backoff.Next()
		log.Warn().Err(err).Int("attempt", backoff.attempt).Dur("retry_in", delay).Msg("worker disconnected, reconnecting")

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}
