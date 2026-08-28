package kafka

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"trade_pulse/shared/retry"

	"github.com/rs/zerolog"
)

const (
	// DefaultRefreshInterval is how often the coordinator is polled. Lag is a
	// trend, not a tick-rate signal; 10s is frequent enough to catch a stalled
	// consumer well inside any alerting window.
	DefaultRefreshInterval = 10 * time.Second

	// DefaultStaleMultiple sets StaleAfter as a multiple of the refresh
	// interval. Three missed refreshes is a real outage rather than one slow
	// round trip, so a single blip never flaps the health report.
	DefaultStaleMultiple = 3

	// refreshTimeout bounds one coordinator round trip so a wedged broker
	// cannot stall the refresh loop indefinitely.
	refreshTimeout = 5 * time.Second

	// jitterFraction desynchronises replicas: without it every pod polls the
	// same coordinator on the same cadence, turning a fleet into a synchronised
	// thundering herd.
	jitterFraction = 0.2
)

// lagSource is what LagCache needs from a reader; *LagReader implements it.
type lagSource interface {
	ReadLag(ctx context.Context) (ConsumerLag, error)
}

// LagSnapshot is the cached reading served to callers.
type LagSnapshot struct {
	// Lag is the last successfully read lag. It is retained when a later
	// refresh fails (serve-stale), so a broker blip degrades the reading's
	// freshness rather than erasing it.
	Lag ConsumerLag
	// AsOf is when Lag was read. The zero value means no successful read yet.
	AsOf time.Time
	// Err is the most recent refresh error, or nil if the last refresh
	// succeeded.
	Err error
	// Failures counts consecutive failed refreshes.
	Failures int
}

// LagCache keeps a consumer group's lag fresh in the background and serves it
// to callers in O(1), without touching the broker on the request path.
//
// This is the same shape as fx-rate-service's poller/cache: an external system
// is polled on a ticker and readers are served the last-good value. It matters
// more here than it looks, because /health is an unauthenticated endpoint —
// reading through to the coordinator per request would let probe traffic (or
// anyone with curl) amplify one cheap HTTP call into broker admin RPCs, and
// would put a 2-5s broker timeout on a request path that must stay fast.
//
// Safe for concurrent use.
type LagCache struct {
	source     lagSource
	interval   time.Duration
	staleAfter time.Duration
	log        zerolog.Logger

	snapshot atomic.Pointer[LagSnapshot]

	// now and jitter are injected by tests to keep timing deterministic.
	now    func() time.Time
	jitter func(time.Duration) time.Duration
}

// NewLagCache builds a cache around source. A non-positive interval falls back
// to DefaultRefreshInterval.
func NewLagCache(source lagSource, interval time.Duration, log zerolog.Logger) *LagCache {
	if interval <= 0 {
		interval = DefaultRefreshInterval
	}

	c := &LagCache{
		source:     source,
		interval:   interval,
		staleAfter: interval * DefaultStaleMultiple,
		log:        log,
		now:        time.Now,
	}

	c.jitter = c.defaultJitter

	return c
}

// defaultJitter spreads each tick over ±jitterFraction of the interval.
func (c *LagCache) defaultJitter(d time.Duration) time.Duration {
	spread := float64(d) * jitterFraction
	return d - time.Duration(spread) + time.Duration(rand.Float64()*2*spread)
}

// Run refreshes the cache until ctx is cancelled, then returns nil. It is
// designed to sit in the service errgroup alongside the servers.
//
// The first refresh happens immediately so /health has a reading as soon as
// the service accepts traffic. Failures back off with jitter (shared/retry) so
// a broker outage does not turn every replica into a retry storm against a
// coordinator that is already struggling; a success resets the sequence.
func (c *LagCache) Run(ctx context.Context) error {
	backoff := retry.NewBackoff()

	for {
		delay := c.jitter(c.interval)

		if err := c.refresh(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			delay = backoff.Next()
			c.log.Warn().Err(err).Dur("retry_in", delay).Msg("kafka lag refresh failed; serving last known reading")
		} else {
			backoff.Reset()
		}

		timer := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

// refresh performs one read and publishes the result.
func (c *LagCache) refresh(ctx context.Context) error {
	readCtx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	lag, err := c.source.ReadLag(readCtx)

	previous := c.Snapshot()

	if err != nil {
		// Serve-stale: keep the last good reading and its timestamp so callers
		// can see both the value and how old it now is.
		c.snapshot.Store(&LagSnapshot{
			Lag:      previous.Lag,
			AsOf:     previous.AsOf,
			Err:      err,
			Failures: previous.Failures + 1,
		})
		return err
	}

	c.snapshot.Store(&LagSnapshot{Lag: lag, AsOf: c.now()})

	return nil
}

// Snapshot returns the current reading. The zero value means nothing has been
// read yet.
//
// The nil-receiver guards on these accessors are deliberate: a nil *LagCache
// boxed into an interface is not a nil interface, so wiring code that "checks
// for nil" can still land one here. Degrading to an empty reading beats
// panicking inside a health handler.
func (c *LagCache) Snapshot() LagSnapshot {
	if c == nil {
		return LagSnapshot{}
	}

	if s := c.snapshot.Load(); s != nil {
		return *s
	}
	return LagSnapshot{}
}

// Age reports how old the last successful reading is, and whether there has
// been one at all.
func (c *LagCache) Age() (time.Duration, bool) {
	if c == nil {
		return 0, false
	}

	snapshot := c.Snapshot()

	if snapshot.AsOf.IsZero() {
		return 0, false
	}

	return c.now().Sub(snapshot.AsOf), true
}

func (c *LagCache) Name() string { return "kafka_consumer_lag" }

// Check satisfies httpserver.Checker.
//
// It deliberately does not fail on a single failed refresh: within StaleAfter
// the last-good reading is still meaningful, so a broker blip is absorbed
// rather than flapping the health report. It fails when there has never been a
// reading, when the reading has gone stale (the refresher or the broker is
// genuinely gone), or when the group has lag but no members — a stalled
// pipeline.
func (c *LagCache) Check(context.Context) error {
	if c == nil {
		return fmt.Errorf("lag cache not configured")
	}

	snapshot := c.Snapshot()

	if snapshot.AsOf.IsZero() {
		if snapshot.Err != nil {
			return fmt.Errorf("no lag reading yet: %w", snapshot.Err)
		}
		return fmt.Errorf("no lag reading yet")
	}

	if age := c.now().Sub(snapshot.AsOf); age > c.staleAfter {
		if snapshot.Err != nil {
			return fmt.Errorf("lag reading stale (%s old, %d consecutive failures): %w",
				age.Round(time.Second), snapshot.Failures, snapshot.Err)
		}
		return fmt.Errorf("lag reading stale (%s old)", age.Round(time.Second))
	}

	if snapshot.Lag.Stalled() {
		return fmt.Errorf("consumer group %q has lag %d but no members",
			snapshot.Lag.Group, snapshot.Lag.Total)
	}

	return nil
}

// Details satisfies httpserver.Detailer, attaching the full per-partition
// breakdown to the ops health report. It is deliberately not exposed on the
// public API: group names, member counts and partition offsets are internal
// topology.
func (c *LagCache) Details() any {
	if c == nil {
		return nil
	}

	snapshot := c.Snapshot()

	detail := map[string]any{
		"group":    snapshot.Lag.Group,
		"topic":    snapshot.Lag.Topic,
		"state":    snapshot.Lag.State,
		"members":  snapshot.Lag.Members,
		"total":    snapshot.Lag.Total,
		"stalled":  snapshot.Lag.Stalled(),
		"failures": snapshot.Failures,
	}

	if len(snapshot.Lag.Partitions) > 0 {
		detail["partitions"] = snapshot.Lag.Partitions
	}

	if !snapshot.AsOf.IsZero() {
		detail["as_of"] = snapshot.AsOf.UTC()
		detail["age_seconds"] = int64(c.now().Sub(snapshot.AsOf).Seconds())
	}

	if snapshot.Err != nil {
		detail["last_error"] = snapshot.Err.Error()
	}

	return detail
}
