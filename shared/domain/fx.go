package domain

import (
	"strings"
	"time"
)

// FXRates is the fiat exchange-rate table maintained by fx-rate-service. It is
// fetched from an external provider once a minute and cached in Redis under
// KeyFXRates with a short TTL. api-service reads it (never the provider) to
// answer /convert in O(1) — Architecture § Service 6.
//
// Rates are expressed against Base (always "USD" here, since exchange prices are
// USD-denominated via USDT). Rates["EUR"] = 0.92 means 1 USD = 0.92 EUR.

type FXRates struct {
	Base      string             `json:"base"`
	Rates     map[string]float64 `json:"rates"`
	FetchedAt time.Time          `json:"fetched_at"`
}

var OnExchangeQuoteAssets = map[string]bool{
	"USD":  true,
	"USDT": true,
	"USDC": true,
}

// Rate returns the multiplier to convert a USD price into quote, and whether a
// rate was available. On-exchange stablecoins resolve to 1.0 without touching
// the table; everything else must be present in Rates or ok is false (the
// caller should then refuse to fabricate a number).

func (f FXRates) Rate(quote string) (rate float64, ok bool) {
	quote = strings.ToUpper(quote)
	if OnExchangeQuoteAssets[quote] {
		return 1.0, true
	}
	r, found := f.Rates[quote]
	return r, found
}
