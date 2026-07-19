package internal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"trade_pulse/shared/domain"

	"github.com/rs/zerolog"
)

func TestWorkerProcessesAllJobs(t *testing.T) {
	numJobs := 500

	var processed int64

	pool := NewWorkerPool(8, func(_ context.Context, event domain.TradeEvent) error {
		atomic.AddInt64(&processed, 1)
		return nil
	}, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Go(func() {
		_ = pool.Start(ctx)
	})

	for i := range numJobs {
		if err := pool.Submit(ctx, domain.TradeEvent{TradeID: int64(i)}); err != nil {
			t.Fatalf("Submit (%d): %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)

	for atomic.LoadInt64(&processed) < int64(numJobs) && time.Now().Before(deadline) {
		time.Sleep(time.Microsecond)
	}

	if got := atomic.LoadInt64(&processed); got != int64(numJobs) {
		t.Fatalf("processed=%d, want %d", got, numJobs)
	}

	cancel()
	wg.Wait()
}

// TestWorkerPoolRespectsSize checks that no more than numWorkers jobs run
// concurrently, by having each job block until released and counting the
// concurrent high-water mark.
func TestWorkerPoolRespectsSize(t *testing.T) {
	const size = 4
	const numJobs = size * 3

	var (
		mu          sync.Mutex
		concurrent  int
		highWater   int
		release     = make(chan struct{})
		startedJobs = make(chan struct{}, numJobs)
	)

	pool := NewWorkerPool(size, func(_ context.Context, _ domain.TradeEvent) error {
		mu.Lock()
		concurrent++

		if concurrent > highWater {
			highWater = concurrent
		}
		mu.Unlock()

		startedJobs <- struct{}{}
		<-release

		mu.Lock()
		concurrent--
		mu.Unlock()
		return nil
	}, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Go(func() {
		_ = pool.Start(ctx)
	})

	// Submit concurrently: the queue only holds size (buffer) + size
	// (in-flight, blocked on release) jobs at once, fewer than numJobs, so a
	// sequential blocking Submit loop would deadlock before any job is
	// released. Each submission runs in its own goroutine instead.

	var submitWG sync.WaitGroup

	for i := range numJobs {
		submitWG.Add(1)
		go func(id int64) {
			defer submitWG.Done()
			if err := pool.Submit(ctx, domain.TradeEvent{TradeID: id}); err != nil {
				t.Errorf("Submit(%d): %v", id, err)
			}
		}(int64(i))
	}

	// Wait until exactly `size` jobs are in flight (the pool saturated).

	for range size {
		select {
		case <-startedJobs:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for workers to saturate")
		}
	}

	mu.Lock()
	got := highWater
	mu.Unlock()

	close(release)
	submitWG.Wait()
	cancel()
	wg.Wait()

	if got > size {
		t.Fatalf("high-water concurrency = %d, want <= %d", got, size)
	}
}

// TestWorkerPoolSubmitBlocksThenRespectsContext checks Submit blocks once the
// buffer is full and every worker is busy, and returns ctx.Err() rather than
// hanging forever once ctx is cancelled.
func TestWorkerPoolSubmitBlocksThenRespectsContext(t *testing.T) {
	block := make(chan struct{})

	pool := NewWorkerPool(1, func(_ context.Context, _ domain.TradeEvent) error {

		<-block //nevver returns unil the testt release it
		return nil

	}, zerolog.Nop())

	poolCtx, poolCancel := context.WithCancel(context.Background())
	defer poolCancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Start(poolCtx)
	}()
	// First Submit occupies the sole worker; second fills the size-1 buffer.
	if err := pool.Submit(poolCtx, domain.TradeEvent{TradeID: 1}); err != nil {
		t.Fatalf("Submit(1): %v", err)
	}
	if err := pool.Submit(poolCtx, domain.TradeEvent{TradeID: 2}); err != nil {
		t.Fatalf("Submit(2): %v", err)
	}
	// A third Submit has nowhere to go and must block until its own ctx is
	// cancelled.
	submitCtx, submitCancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- pool.Submit(submitCtx, domain.TradeEvent{TradeID: 3}) }()

	select {
	case err := <-errCh:
		t.Fatalf("Submit returned early with err=%v, want it to block", err)
	case <-time.After(50 * time.Millisecond):
	}
	submitCancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Submit error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Submit did not return after its context was cancelled")
	}

	close(block)
	poolCancel()
	wg.Wait()
}

// TestWorkerPoolStartStopsOnContextCancel checks Start returns once ctx is
// cancelled, without requiring every queued job to drain first.
func TestWorkerPoolStartStopsOnContextCancel(t *testing.T) {
	pool := NewWorkerPool(2, func(ctx context.Context, event domain.TradeEvent) error {
		<-ctx.Done()
		return nil
	}, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- pool.Start(ctx) }()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil on clean shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}

// TestWorkerPoolHandleErrorDoesNotStopPool checks that one job's handle error
// is logged and swallowed rather than killing the pool or other workers.
func TestWorkerPoolHandleErrorDoesNotStopPool(t *testing.T) {
	var processed int64

	pool := NewWorkerPool(2, func(_ context.Context, event domain.TradeEvent) error {
		atomic.AddInt64(&processed, 1)
		if event.TradeID == 1 {
			return errors.New("boom")
		}
		return nil
	}, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		_ = pool.Start(ctx)
	}()

	for i := int64(1); i <= 3; i++ {
		if err := pool.Submit(ctx, domain.TradeEvent{TradeID: 1}); err != nil {
			t.Fatalf("Submit(%d): %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&processed) < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Microsecond)
	}

	if got := atomic.LoadInt64(&processed); got != 3 {
		t.Fatalf("processed = %d, want 3 (a handle error must not stop the pool)", got)
	}

	cancel()
	wg.Wait()

}

// TestNewWorkerPoolDefaultsSize checks size <= 0 falls back to
// defaultPoolSize rather than producing a pool with zero workers.
func TestNewWorkerPoolDefaultsSize(t *testing.T) {
	pool := NewWorkerPool(0, func(_ context.Context, _ domain.TradeEvent) error { return nil }, zerolog.Nop())

	if pool.numWorkers != defaultPoolSize {
		t.Errorf("numWorkers = %d, want %d", pool.numWorkers, defaultPoolSize)
	}

	pool = NewWorkerPool(-5, func(_ context.Context, _ domain.TradeEvent) error { return nil }, zerolog.Nop())

	if pool.numWorkers != defaultPoolSize {
		t.Errorf("numWorkers = %d, want %d", pool.numWorkers, defaultPoolSize)
	}
}
