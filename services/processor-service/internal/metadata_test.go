package internal

import (
	"testing"
	"trade_pulse/shared/domain"
)

func TestStaticMetadataProviderLookup(t *testing.T) {
	p := NewStaticMetadataProvider(map[string]domain.MarketMetadata{
		"BTCUSDT": {BaseAsset: "BTC", QuoteAsset: "USDT", Exchange: "Binance"},
	})

	got, ok := p.Lookup("BTCUSDT")
	if !ok {
		t.Fatalf("Lookup(BTCUSDT) ok = false, want true")
	}
	want := domain.MarketMetadata{BaseAsset: "BTC", QuoteAsset: "USDT", Exchange: "Binance"}
	if got != want {
		t.Errorf("Lookup(BTCUSDT) = %+v, want %+v", got, want)
	}

	if _, ok := p.Lookup("DOGEUSDT"); ok {
		t.Errorf("Lookup(DOGEUSDT) ok = true, want false for a symbol not in the table")
	}
}

// TestStaticMetadataProviderLookupNilTable checks a provider built with a nil
// table (as tests elsewhere use for symbols whose metadata doesn't matter)
// behaves like an always-miss table rather than panicking.
func TestStaticMetadataProviderLookupNilTable(t *testing.T) {
	p := NewStaticMetadataProvider(nil)

	if _, ok := p.Lookup("BTCUSDT"); ok {
		t.Errorf("Lookup(BTCUSDT) ok = true, want false for a nil table")
	}
}

func TestNewDefaultMetadataProviderCoversConfiguredSymbols(t *testing.T) {
	p := NewDefaultMetadataProvider()

	for _, symbol := range []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"} {
		meta, ok := p.Lookup(symbol)
		if !ok {
			t.Errorf("Lookup(%s) ok = false, want true — should match shared/config's default ingestion.symbols", symbol)
			continue
		}
		if meta.Exchange != "Binance" || meta.QuoteAsset != "USDT" || meta.BaseAsset == "" {
			t.Errorf("Lookup(%s) = %+v, want Binance/USDT with a non-empty base asset", symbol, meta)
		}
	}
}
