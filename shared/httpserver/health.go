package httpserver

import (
	"context"
	"sync"
)

// A nil error means healthy.
type Checker interface {
	// Name identifies the dependency in the /health response.
	Name() string
	// Check reports current health; ctx carries a deadline so a hung
	// dependency cannot wedge the health endpoint.
	Check(ctx context.Context) error
}

// CheckerFunc adapts a plain (name, func) pair into a Checker, mirroring the
// http.HandlerFunc idiom so callers in other packages don't need their own
// Checker type for a one-off probe (e.g. a Kafka producer or Redis ping).
func CheckerFunc(name string, fn func(ctx context.Context) error) Checker {
	return namedChecker{name: name, fn: fn}
}

type namedChecker struct {
	name string
	fn   func(ctx context.Context) error
}

func (c namedChecker) Name() string                    { return c.name }
func (c namedChecker) Check(ctx context.Context) error { return c.fn(ctx) }

// healthRegistry holds the registered checkers. It is safe for concurrent
// registration (services may register from goroutines as components come up).

type healthRegistry struct {
	mu       sync.RWMutex
	checkers []Checker
}

func (r *healthRegistry) add(c Checker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checkers = append(r.checkers, c)
}

func (r *healthRegistry) snapshot() []Checker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Checker, len(r.checkers))
	copy(out, r.checkers)
	return out
}
