package kafka

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// stubSource returns a scripted sequence of readings, one per call, repeating
// the last entry once exhausted.
type stubSource struct {
	mu      sync.Mutex
	results []result
	calls   int
	gotCtx  context.Context
}

type result struct {
	lag ConsumerLag
	err error
}

func (s *stubSource) ReadLag(ctx context.Context) (ConsumerLag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.gotCtx = ctx
	i := min(s.calls, len(s.results)-1)
	s.calls++

	return s.results[i].lag, s.results[i].err
}

func (s *stubSource) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// newTestCache builds a cache with a controllable clock and no jitter.
func newTestCache(source lagSource, interval time.Duration, clock *time.Time) *LagCache {
	c := NewLagCache(source, interval, zerolog.New(io.Discard))
	c.now = func() time.Time { return *clock }
	c.jitter = func(d time.Duration) time.Duration { return d }
	return c
}

func TestRefreshPublishesSnapshot(t *testing.T) {
	clock := time.Unix(1000, 0)
	source := &stubSource{results: []result{{lag: ConsumerLag{Group: "g", Total: 42, Members: 1}}}}
	cache := newTestCache(source, time.Second, &clock)

	if err := cache.refresh(context.Background()); err != nil {
		t.Fatalf("refresh() error = %v", err)
	}

	snapshot := cache.Snapshot()

	if snapshot.Lag.Total != 42 || !snapshot.AsOf.Equal(clock) || snapshot.Err != nil {
		t.Fatalf("snapshot = %+v, want lag 42 stamped at the current time", snapshot)
	}
}

// A failed refresh must keep the last good reading rather than erase it: a
// stale number plus its age is far more useful than no number.
func TestRefreshFailureServesStaleReading(t *testing.T) {
	clock := time.Unix(1000, 0)
	source := &stubSource{results: []result{
		{lag: ConsumerLag{Total: 42, Members: 1}},
		{err: errors.New("broker unreachable")},
	}}
	cache := newTestCache(source, time.Second, &clock)

	if err := cache.refresh(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}

	firstAsOf := cache.Snapshot().AsOf

	clock = clock.Add(30 * time.Second)

	if err := cache.refresh(context.Background()); err == nil {
		t.Fatal("second refresh error = nil, want the source error")
	}

	snapshot := cache.Snapshot()

	if snapshot.Lag.Total != 42 {
		t.Fatalf("lag = %d, want the last good reading 42 retained", snapshot.Lag.Total)
	}

	if !snapshot.AsOf.Equal(firstAsOf) {
		t.Fatalf("AsOf = %v, want it pinned to the last *successful* read %v", snapshot.AsOf, firstAsOf)
	}

	if snapshot.Err == nil || snapshot.Failures != 1 {
		t.Fatalf("snapshot = %+v, want the error and one consecutive failure recorded", snapshot)
	}
}

func TestRefreshBoundsTheBrokerCall(t *testing.T) {
	clock := time.Unix(1000, 0)
	source := &stubSource{results: []result{{}}}
	cache := newTestCache(source, time.Second, &clock)

	if err := cache.refresh(context.Background()); err != nil {
		t.Fatalf("refresh() error = %v", err)
	}

	if _, ok := source.gotCtx.Deadline(); !ok {
		t.Fatal("ReadLag got a context with no deadline; a wedged broker would stall the refresher")
	}
}

func TestCheckFailsBeforeTheFirstReading(t *testing.T) {
	clock := time.Unix(1000, 0)
	cache := newTestCache(&stubSource{results: []result{{}}}, time.Second, &clock)

	if err := cache.Check(context.Background()); err == nil {
		t.Fatal("Check() = nil, want an error before any reading exists")
	}
}

// One failed refresh inside the staleness window is a blip, not an outage.
func TestCheckAbsorbsATransientFailure(t *testing.T) {
	clock := time.Unix(1000, 0)
	source := &stubSource{results: []result{
		{lag: ConsumerLag{Total: 5, Members: 1}},
		{err: errors.New("blip")},
	}}
	cache := newTestCache(source, 10*time.Second, &clock)

	_ = cache.refresh(context.Background())
	clock = clock.Add(11 * time.Second) // inside staleAfter (3 x 10s)
	_ = cache.refresh(context.Background())

	if err := cache.Check(context.Background()); err != nil {
		t.Fatalf("Check() = %v, want nil: one failure inside the staleness window must not flap health", err)
	}
}

func TestCheckFailsOnceTheReadingGoesStale(t *testing.T) {
	clock := time.Unix(1000, 0)
	source := &stubSource{results: []result{
		{lag: ConsumerLag{Total: 5, Members: 1}},
		{err: errors.New("broker unreachable")},
	}}
	cache := newTestCache(source, 10*time.Second, &clock)

	_ = cache.refresh(context.Background())
	clock = clock.Add(31 * time.Second) // past staleAfter (3 x 10s)
	_ = cache.refresh(context.Background())

	err := cache.Check(context.Background())

	if err == nil {
		t.Fatal("Check() = nil, want a stale-reading error")
	}

	if !errors.Is(err, source.results[1].err) {
		t.Fatalf("Check() = %v, want it to wrap the underlying refresh error", err)
	}
}

// Lag with nobody consuming it is the failure worth reporting: a large lag on
// a healthy group may just be a cold start catching up.
func TestCheckFailsWhenTheGroupIsStalled(t *testing.T) {
	clock := time.Unix(1000, 0)
	cache := newTestCache(&stubSource{results: []result{
		{lag: ConsumerLag{Group: "processor-service", Total: 9001, Members: 0}},
	}}, 10*time.Second, &clock)

	_ = cache.refresh(context.Background())

	if err := cache.Check(context.Background()); err == nil {
		t.Fatal("Check() = nil, want an error when a group has lag but no members")
	}
}

func TestCheckPassesForAHealthyBacklog(t *testing.T) {
	clock := time.Unix(1000, 0)
	cache := newTestCache(&stubSource{results: []result{
		{lag: ConsumerLag{Group: "g", Total: 100000, Members: 3, State: "Stable"}},
	}}, 10*time.Second, &clock)

	_ = cache.refresh(context.Background())

	if err := cache.Check(context.Background()); err != nil {
		t.Fatalf("Check() = %v, want nil: a big backlog with live members is catching up, not broken", err)
	}
}

func TestDetailsCarriesTheBreakdownAndAge(t *testing.T) {
	clock := time.Unix(1000, 0)
	cache := newTestCache(&stubSource{results: []result{{lag: ConsumerLag{
		Group:      "processor-service",
		Topic:      "trades.raw",
		State:      "Stable",
		Members:    2,
		Total:      42,
		Partitions: []PartitionLag{{Partition: 0, Lag: 42}},
	}}}}, 10*time.Second, &clock)

	_ = cache.refresh(context.Background())
	clock = clock.Add(4 * time.Second)

	detail, ok := cache.Details().(map[string]any)

	if !ok {
		t.Fatalf("Details() = %T, want map[string]any", cache.Details())
	}

	if detail["total"] != int64(42) || detail["members"] != 2 || detail["group"] != "processor-service" {
		t.Fatalf("details = %+v, want the group breakdown", detail)
	}

	if detail["age_seconds"] != int64(4) {
		t.Fatalf("age_seconds = %v, want 4", detail["age_seconds"])
	}

	if _, present := detail["partitions"]; !present {
		t.Fatalf("details = %+v, want the per-partition breakdown", detail)
	}
}

func TestAgeReportsWhetherAReadingExists(t *testing.T) {
	clock := time.Unix(1000, 0)
	cache := newTestCache(&stubSource{results: []result{{}}}, time.Second, &clock)

	if _, ok := cache.Age(); ok {
		t.Fatal("Age() reported a reading before one was taken")
	}

	_ = cache.refresh(context.Background())
	clock = clock.Add(7 * time.Second)

	age, ok := cache.Age()

	if !ok || age != 7*time.Second {
		t.Fatalf("Age() = %v, %v; want 7s, true", age, ok)
	}
}

// Run must populate the cache immediately rather than waiting a full interval,
// so /health is meaningful as soon as the service accepts traffic.
func TestRunRefreshesImmediatelyAndStopsOnCancel(t *testing.T) {
	clock := time.Unix(1000, 0)
	source := &stubSource{results: []result{{lag: ConsumerLag{Total: 3, Members: 1}}}}
	cache := newTestCache(source, time.Hour, &clock) // long interval: only the eager first read can land

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- cache.Run(ctx) }()

	deadline := time.After(2 * time.Second)

	for source.callCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("Run did not refresh immediately")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	if got := cache.Snapshot().Lag.Total; got != 3 {
		t.Fatalf("lag = %d, want 3 after the eager first refresh", got)
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil on cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestNewLagCacheFallsBackToTheDefaultInterval(t *testing.T) {
	cache := NewLagCache(&stubSource{results: []result{{}}}, 0, zerolog.New(io.Discard))

	if cache.interval != DefaultRefreshInterval {
		t.Fatalf("interval = %v, want %v", cache.interval, DefaultRefreshInterval)
	}

	if cache.staleAfter != DefaultRefreshInterval*DefaultStaleMultiple {
		t.Fatalf("staleAfter = %v, want %v", cache.staleAfter, DefaultRefreshInterval*DefaultStaleMultiple)
	}
}

// Jitter must stay within bounds: too wide and refreshes drift unpredictably,
// zero and every replica polls the coordinator in lockstep.
func TestDefaultJitterStaysWithinBounds(t *testing.T) {
	cache := NewLagCache(&stubSource{results: []result{{}}}, 10*time.Second, zerolog.New(io.Discard))

	low := time.Duration(float64(10*time.Second) * (1 - jitterFraction))
	high := time.Duration(float64(10*time.Second) * (1 + jitterFraction))

	for range 1000 {
		d := cache.defaultJitter(10 * time.Second)

		if d < low || d > high {
			t.Fatalf("jitter = %v, want within [%v, %v]", d, low, high)
		}
	}
}
