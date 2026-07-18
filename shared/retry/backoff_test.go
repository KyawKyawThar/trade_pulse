package retry

import (
	"testing"
	"time"
)

// maxRNG drives jitter to its inclusive upper bound, so Next returns the raw
// un-jittered ceiling.
func maxRNG(n int64) int64 { return n - 1 }

func newTestBackoff() *Backoff {
	return &Backoff{Base: 1 * time.Second, Max: 30 * time.Second, ResetAfter: 2 * time.Minute, rng: maxRNG}
}

func TestBackoffCeilingGrowsAndSaturates(t *testing.T) {
	b := newTestBackoff()

	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second, // 32s clamps to Max
		30 * time.Second, // stays saturated, no overflow however long the outage
	}

	for i, w := range want {
		if got := b.Next(); got != w {
			t.Fatalf("Next() call %d = %v, want %v", i+1, got, w)
		}
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	// The rng receives ceiling+1, making the range [0, ceiling] inclusive.
	var gotN int64
	b := newTestBackoff()
	b.rng = func(n int64) int64 {
		gotN = n
		return 0
	}

	if got := b.Next(); got != 0 {
		t.Errorf("Next() with zero rng = %v, want 0 (full jitter lower bound)", got)
	}
	if want := int64(1*time.Second) + 1; gotN != want {
		t.Errorf("rng received n = %d, want %d (ceiling+1 for inclusive upper bound)", gotN, want)
	}
}

func TestBackoffObserveResets(t *testing.T) {
	b := newTestBackoff()

	for range 10 {
		b.Next()
	}

	// An uptime below ResetAfter is still the same incident: stay escalated.
	b.Observe(1 * time.Minute)
	if got := b.Next(); got != b.Max {
		t.Fatalf("Next() after short uptime = %v, want %v", got, b.Max)
	}

	// At ResetAfter the link has recovered: restart from Base.
	b.Observe(2 * time.Minute)
	if got := b.Next(); got != b.Base {
		t.Fatalf("Next() after recovery = %v, want %v", got, b.Base)
	}
}

func TestBackoffDefaultRNGWithinBounds(t *testing.T) {
	// Smoke-test the real math/rand/v2 path: delays stay within [0, ceiling].
	b := NewBackoff()

	for i := range 50 {
		b.attempt = i % 10
		ceiling := b.ceiling()
		if d := b.jitter(ceiling); d < 0 || d > ceiling {
			t.Fatalf("jitter(%v) = %v, want within [0, %v]", ceiling, d, ceiling)
		}
	}
}
