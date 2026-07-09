package internal

import (
	"testing"
	"trade_pulse/shared/domain"
)

func TestNormalizeTrade(t *testing.T) {

	t.Run("valid buy", func(t *testing.T) {
		raw := []byte(`{"e":"trade","E":123456789,"s":"BTCUSDT","t":12345,"p":"65000.50","q":"0.5","b":88,"a":50,"T":1719158400000,"m":false,"M":true}`)

		got, err := normalizeTrade(raw)

		if err != nil {
			t.Fatalf("normalizeTrade() error = %v", err)
		}

		if got.Symbol != "BTCUSDT" {
			t.Errorf("Symbol = %q, want BTCUSDT", got.Symbol)
		}

		if got.Price != 65000.50 {
			t.Errorf("Price=%v, want 65000.50", got.Price)
		}
		if got.Quantity != 0.5 {
			t.Errorf("Quantity = %v, want 0.5", got.Quantity)
		}
		if got.Side != domain.SideBuy {
			t.Errorf("Side = %v, want SideBuy", got.Side)
		}
		if got.TradeID != 12345 {
			t.Errorf("TradeID = %v, want 12345", got.TradeID)
		}
		if got.EventTime.UnixMilli() != 1719158400000 {
			t.Errorf("EventTime = %v, want unix ms 1719158400000", got.EventTime)
		}
		if got.IngestTime.IsZero() {
			t.Error("IngestTime should be set")
		}

	})

	t.Run("buyer is maker means sell", func(t *testing.T) {
		raw := []byte(`{"e":"trade","s":"BTCUSDT","t":1,"p":"1","q":"1","T":1,"m":true}`)

		got, err := normalizeTrade(raw)

		if err != nil {
			t.Fatalf("normalizeTrade() error = %v", err)
		}

		if got.Side != domain.SideSell {
			t.Errorf("Side = %v, want SideSell", got.Side)
		}
	})
	tests := []struct {
		name string
		raw  string
	}{
		{"not json", `not json`},
		{"wrong event type", `{"e":"aggTrade","s":"BTCUSDT","p":"1","q":"1"}`},
		{"missing symbol", `{"e":"trade","s":"","p":"1","q":"1"}`},
		{"non-numeric price", `{"e":"trade","s":"BTCUSDT","p":"abc","q":"1"}`},
		{"zero price", `{"e":"trade","s":"BTCUSDT","p":"0","q":"1"}`},
		{"negative quantity", `{"e":"trade","s":"BTCUSDT","p":"1","q":"-1"}`},
		{"missing trade time", `{"e":"trade","s":"BTCUSDT","p":"1","q":"1"}`},
		{"zero trade time", `{"e":"trade","s":"BTCUSDT","p":"1","q":"1","T":0}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := normalizeTrade([]byte(tt.raw)); err == nil {
				t.Errorf("normalizeTrade(%s) expected error, got nil", tt.raw)
			}
		})
	}
}
