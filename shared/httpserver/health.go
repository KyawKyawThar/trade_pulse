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

type CheckerFunc struct {
	CheckerName string
	fn          func(ctx context.Context) error
}

func (c CheckerFunc) Name() string { return c.CheckerName }

func (c CheckerFunc) Check(ctx context.Context) error {
	return c.fn(ctx)
}

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
	defer r.mu.Unlock()
	out := make([]Checker, len(r.checkers))
	copy(out, r.checkers)
	return out
}
