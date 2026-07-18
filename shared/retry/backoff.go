// Package retry provides jittered exponential backoff and a reconnect loop for
// long-lived connections that are expected to run forever and be re-established
// whenever they drop — a WebSocket read loop, a Kafka/RabbitMQ consumer, a
// Redis subscription, a database LISTEN, and so on. The work is expressed as a
// plain func(context.Context) error, so nothing here is tied to any particular
// transport.
package retry

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/rs/zerolog"
)

const (
	// DefaultBase is the ceiling after the first failed attempt; each
	// subsequent consecutive failure doubles it, up to DefaultMax.
	DefaultBase = 1 * time.Second
	// DefaultMax caps the delay so even a long outage keeps retrying about
	// twice a minute rather than drifting into multi-minute gaps.
	DefaultMax = 30 * time.Second
	// DefaultResetAfter is how long a connection must stay up before its next
	// drop is treated as a fresh incident. A link that survives this long has
	// clearly recovered, so the backoff sequence restarts from Base instead of
	// continuing to escalate from where the previous outage left off.
	DefaultResetAfter = 2 * time.Minute
)

// Backoff produces jittered exponential retry delays: each consecutive failure
// doubles the ceiling from Base up to Max, and Observe restarts the sequence
// once a connection has stayed up for ResetAfter. Not safe for concurrent use;
// each worker owns its own instance.
type Backoff struct {
	Base, Max, ResetAfter time.Duration

	// rng returns a uniform random int64 in [0, n), defaulting to
	// rand.Int64N; tests inject it to make delays deterministic.
	rng func(n int64) int64

	attempt int
}

// NewBackoff returns a Backoff seeded with the package defaults.
func NewBackoff() *Backoff {
	return &Backoff{Base: DefaultBase, Max: DefaultMax, ResetAfter: DefaultResetAfter}
}

// Next returns the jittered delay for the current failure and advances the
// sequence.
func (b *Backoff) Next() time.Duration {
	d := b.ceiling()
	b.attempt++
	return b.jitter(d)
}

// Observe feeds back how long the last connection stayed up; at ResetAfter or
// beyond the link is considered recovered and the sequence restarts.
func (b *Backoff) Observe(uptime time.Duration) {
	if uptime >= b.ResetAfter {
		b.attempt = 0
	}
}

// Attempt reports how many consecutive failures the sequence has seen so far.
func (b *Backoff) Attempt() int { return b.attempt }

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

// WithBackoff runs work and, whenever it returns without ctx being cancelled,
// waits a jittered backoff delay before running it again — i.e. it reconnects
// forever. A nil return from work is retried too: for an always-on connection a
// clean exit still means "the link is gone", and returning would silently kill
// this worker. WithBackoff itself returns nil once ctx is cancelled.
//
// work is anything that owns a connection for as long as it stays up: a WS read
// loop, a consumer poll loop, a Redis subscribe, etc. It must respect ctx and
// return when cancelled.
func WithBackoff(ctx context.Context, log zerolog.Logger, work func(context.Context) error) error {
	backoff := NewBackoff()

	for {
		start := time.Now()
		err := work(ctx)

		if ctx.Err() != nil {
			return nil
		}

		backoff.Observe(time.Since(start))
		delay := backoff.Next()
		log.Warn().Err(err).Int("attempt", backoff.attempt).Dur("retry_in", delay).Msg("disconnected, reconnecting")

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}
