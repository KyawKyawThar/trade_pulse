package internal

import (
	"context"
	"errors"
	"testing"
	"trade_pulse/shared/domain"
)

// btcusdtMetadata is the fixture NewStaticMetadataProvider is seeded with
// across these tests — one known symbol so tests can assert both the hit and
// the miss path.
var btcusdtMetadata = map[string]domain.MarketMetadata{
	"BTCUSDT": {BaseAsset: "BTC", QuoteAsset: "USDT", Exchange: "Binance"},
}

// TestEnricherHandleSetsNotional checks Handle computes price*quantity and
// forwards the enriched event to next, leaving every other field untouched.
func TestEnrichHandleSetsNotional(t *testing.T) {

	in := domain.TradeEvent{Symbol: "BTCUSDT", Price: 65000.50, Quantity: 0.5, Side: domain.SideBuy, TradeID: 42}

	var got domain.TradeEvent

	e := NewEnricher(func(_ context.Context, event domain.TradeEvent) error {

		got = event
		return nil
	}, NewStaticMetadataProvider(btcusdtMetadata))

	if err := e.Handle(context.Background(), in); err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}

	const wantNotional = 32500.25

	if got.Notional != wantNotional {
		t.Errorf("Notional = %v, want %v", got.Notional, wantNotional)
	}

	want := in
	want.Notional = wantNotional
	want.Market = domain.MarketMetadata{BaseAsset: "BTC", QuoteAsset: "USDT", Exchange: "Binance"}
	if got != want {
		t.Errorf("Handle() forwarded %+v, want %+v", got, want)
	}
}

// TestEnricherHandleSetsMarketMetadataOnUnknownSymbol checks a symbol missing
// from the metadata table fails open: Handle still forwards the event (with
// notional set) rather than erroring, since dropping a trade over unmapped
// reference data is worse than forwarding one with zero-value Market.
func TestEnricherHandleSetsMarketMetadataOnUnknownSymbol(t *testing.T) {
	in := domain.TradeEvent{Symbol: "DOGEUSDT", Price: 0.5, Quantity: 100}

	var got domain.TradeEvent
	e := NewEnricher(func(_ context.Context, event domain.TradeEvent) error {
		got = event
		return nil
	}, NewStaticMetadataProvider(btcusdtMetadata))

	if err := e.Handle(context.Background(), in); err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}

	if got.Market != (domain.MarketMetadata{}) {
		t.Errorf("Market = %+v, want zero value for unmapped symbol", got.Market)
	}
	if got.Notional != 50 {
		t.Errorf("Notional = %v, want 50", got.Notional)
	}
}

// TestEnricherHandleDoesNotMutateCaller checks the caller's event is passed
// by value, so a pool.go job dequeued elsewhere can't observe the notional
// Handle computed for a different call.
func TestEnricherHandleDoesNotMutateCaller(t *testing.T) {
	in := domain.TradeEvent{Symbol: "ETHUSDT", Price: 3000, Quantity: 2}

	e := NewEnricher(func(_ context.Context, _ domain.TradeEvent) error { return nil }, NewStaticMetadataProvider(nil))

	if err := e.Handle(context.Background(), in); err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
	if in.Notional != 0 {
		t.Errorf("caller's event mutated: Notional = %v, want 0", in.Notional)
	}
	if in.Market != (domain.MarketMetadata{}) {
		t.Errorf("caller's event mutated: Market = %+v, want zero value", in.Market)
	}
}

// TestEnricherHandlePropagatesNextError checks a next-handler error passes
// through unchanged, so pool.go's error logging still sees it.
func TestEnricherHandlePropagatesNextError(t *testing.T) {
	wantErr := errors.New("sink unavailable")

	e := NewEnricher(func(_ context.Context, _ domain.TradeEvent) error {
		return wantErr
	}, NewStaticMetadataProvider(nil))

	if err := e.Handle(context.Background(), domain.TradeEvent{}); !errors.Is(err, wantErr) {

		t.Errorf("Handle() error = %v, want %v", err, wantErr)
	}
}
