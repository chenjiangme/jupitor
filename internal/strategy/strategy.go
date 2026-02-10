// Package strategy defines the Strategy interface for trading strategies and
// provides a Registry for managing multiple strategy implementations.
package strategy

import (
	"context"
	"sort"

	"jupitor/internal/domain"
)

// Strategy is the interface that all trading strategies must implement.
type Strategy interface {
	// Name returns the unique identifier for this strategy.
	Name() string

	// Init performs any one-time setup required before the strategy begins
	// processing market data.
	Init(ctx context.Context) error

	// OnBar is called when a new OHLCV bar is available. It returns zero or
	// more trading signals.
	OnBar(ctx context.Context, bar domain.Bar) ([]domain.Signal, error)

	// OnTrade is called when a new trade tick is available. It returns zero or
	// more trading signals.
	OnTrade(ctx context.Context, trade domain.Trade) ([]domain.Signal, error)
}

// Registry holds a named collection of strategies for lookup and enumeration.
type Registry struct {
	strategies map[string]Strategy
}

// NewRegistry creates an empty strategy Registry.
func NewRegistry() *Registry {
	return &Registry{
		strategies: make(map[string]Strategy),
	}
}

// Register adds a strategy to the registry, keyed by its Name().
func (r *Registry) Register(s Strategy) {
	r.strategies[s.Name()] = s
}

// Get retrieves a strategy by name. The second return value indicates whether
// the strategy was found.
func (r *Registry) Get(name string) (Strategy, bool) {
	s, ok := r.strategies[name]
	return s, ok
}

// List returns a sorted slice of all registered strategy names.
func (r *Registry) List() []string {
	names := make([]string, 0, len(r.strategies))
	for name := range r.strategies {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
