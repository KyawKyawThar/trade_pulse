package internal

import (
	"context"
	"errors"
	"testing"
	"trade_pulse/shared/domain"

	"github.com/rs/zerolog"
)

func TestChain(t *testing.T) {
	t.Run("runs outer-to-inner then unwinds, and returns the base's error", func(t *testing.T) {
		var order []string
		wantErr := errors.New("base failed")

		base := func(context.Context, domain.TradeEvent) error {
			order = append(order, "base")
			return wantErr
		}
		record := func(name string) TradeMiddleware {
			return func(next TradeHandler) TradeHandler {
				return func(ctx context.Context, event domain.TradeEvent) error {
					order = append(order, name+":before")
					err := next(ctx, event)
					order = append(order, name+":after")
					return err
				}
			}
		}

		handler := Chain(base, record("outer"), record("inner"))
		err := handler(context.Background(), domain.TradeEvent{})

		wantOrder := []string{"outer:before", "inner:before", "base", "inner:after", "outer:after"}
		if len(order) != len(wantOrder) {
			t.Fatalf("call order = %v, want %v", order, wantOrder)
		}
		for i := range wantOrder {
			if order[i] != wantOrder[i] {
				t.Fatalf("call order = %v, want %v", order, wantOrder)
			}
		}
		if !errors.Is(err, wantErr) {
			t.Errorf("Chain() error = %v, want %v", err, wantErr)
		}
	})

	t.Run("no middlewares returns base unchanged", func(t *testing.T) {
		called := false
		base := func(context.Context, domain.TradeEvent) error {
			called = true
			return nil
		}

		if err := Chain(base)(context.Background(), domain.TradeEvent{}); err != nil {
			t.Errorf("Chain(base)() = %v, want nil", err)
		}
		if !called {
			t.Error("base handler was not called")
		}
	})
}

func TestWithLogging(t *testing.T) {
	trade := domain.TradeEvent{Symbol: "BTCUSDT", Price: 1, Quantity: 1, TradeID: 1}
	var gotCtx context.Context
	var gotEvent domain.TradeEvent
	wantErr := errors.New("downstream failed")

	next := func(ctx context.Context, event domain.TradeEvent) error {
		gotCtx = ctx
		gotEvent = event
		return wantErr
	}

	handler := withLogging(zerolog.Nop())(next)
	ctx := context.Background()
	err := handler(ctx, trade)

	if gotEvent != trade {
		t.Errorf("next received %+v, want %+v", gotEvent, trade)
	}
	if gotCtx != ctx {
		t.Error("withLogging must pass through the original context unchanged")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("withLogging() error = %v, want %v (must not alter next's error)", err, wantErr)
	}
}
