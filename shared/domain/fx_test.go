package domain

import (
	"testing"
	"time"
)

func TestFXRates(t *testing.T) {

	rates := FXRates{
		Base:      "USD",
		Rates:     map[string]float64{"EUR": 0.92, "GBP": 0.79},
		FetchedAt: time.Now(),
	}

	test := []struct {
		quote    string
		wantRate float64
		wantOK   bool
	}{
		{"EUR", 0.92, true}, // fiat present in table
		{"eur", 0.92, true}, // case-insensitive
		{"USD", 1.0, true},  // on-exchange, no lookup
		{"USDT", 1.0, true}, // on-exchange stablecoin
		{"USDC", 1.0, true}, // on-exchange stablecoin
		{"JPY", 0, false},   // fiat absent — must not fabricate a rate
	}

	for _, tc := range test {
		gotRate, gotOK := rates.Rate(tc.quote)

		if gotRate != tc.wantRate || gotOK != tc.wantOK {
			t.Errorf("Rate(%q) = (%v, %v), want (%v, %v)",
				tc.quote, gotRate, gotOK, tc.wantRate, tc.wantOK)
		}
	}
}

func TestWhaleAlertDedupKey(t *testing.T) {
	// The dedup key must be derived only from stable trade attributes, so two
	// physical copies of the same logical alert (e.g. after a Kafka rebalance)
	// collapse to one key. Same inputs -> same key.

	at := time.Unix(1719158400, 0)

	a := WhaleAlert{Symbol: "BTCUSDT", Price: 65000.50, EventTime: at}
	b := WhaleAlert{Symbol: "BTCUSDT", Price: 65000.50, EventTime: at}

	if a.DedupKey() != b.DedupKey() {
		t.Fatalf("identical alerts produced different dedup keys: %q vs %q", a.DedupKey(), b.DedupKey())
	}

	want := "alert:whale:BTCUSDT:65000.50:1719158400"
	if got := a.DedupKey(); got != want {
		t.Errorf("DedupKey() = %q, want %q", got, want)
	}
}

// A dedup key (short for deduplication key) is a unique identifier used to detect and filter out duplicate data.

// In distributed systems, the exact same event or message can accidentally be sent or processed more than once. A dedup key acts as a "fingerprint." If the system sees a fingerprint it has already processed, it throws the duplicate away instead of handling it a second time.

// Breaking down what your specific code comment means:

// 1. The Problem: "Kafka Rebalance"
// Your code uses Kafka, a message streaming platform. Sometimes, Kafka has to re-balance which servers are talking to which workers. When this happens, a worker might crash or disconnect midway through a task.

// When a new worker takes over, it might safely replay the last few messages just to be sure nothing was missed. This means your service will receive two physical copies of the same logical alert. Without deduplication, your system might accidentally send a user two duplicate notifications or log the same trade twice.

// 2. The Solution: "Same inputs -> same key"
// To prevent this, you create a formula or hash based only on things that never change about that specific trade (stable trade attributes).

// For example, a trade's dedup key might look like a combination of:
// tradeID + timestamp + asset

// First copy arrives: The system looks at the attributes, generates the key trade_12345_1712123400, checks the database/cache, sees it's new, and processes it.

// Second copy arrives (due to Kafka rebalance): The system looks at the exact same attributes, generates the exact same key trade_12345_1712123400, sees it already exists in the cache, and safely ignores it ("collapses to one key").
