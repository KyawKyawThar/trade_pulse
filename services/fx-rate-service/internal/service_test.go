package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	"trade_pulse/shared/config"

	"github.com/rs/zerolog"
)

func newTestService(fetch rateFetcher) *Service {
	cfg := config.Config{}
	cfg.FX.PollInterval = time.Millisecond
	s := &Service{cfg: cfg, log: zerolog.Nop(), fetch: fetch}
	return s
}

func TestPollSessionStopsOnContextCancel(t *testing.T) {
	polled := make(chan struct{}, 1)
	s := newTestService(func(ctx context.Context) error {
		select {
		case polled <- struct{}{}:
		default:
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- s.pollSession(ctx) }()

	// Wait until at least one poll happened, then cancel.
	<-polled
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("pollSession() = %v, want nil on ctx cancel", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pollSession did not return after ctx cancel")
	}
}

func TestPollSessionReturnsFetchError(t *testing.T) {
	wantErr := errors.New("provider unreachable")
	s := newTestService(func(ctx context.Context) error { return wantErr })

	err := s.pollSession(context.Background())
	if err == nil {
		t.Fatal("pollSession() = nil, want the fetch error surfaced for backoff")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("pollSession() = %v, want it to wrap %v", err, wantErr)
	}
}
