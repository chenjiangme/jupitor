package strategy

import (
	"context"
	"testing"

	"jupitor/internal/domain"
)

// stubStrategy is a minimal Strategy implementation used in registry tests.
type stubStrategy struct {
	name string
}

func (s *stubStrategy) Name() string                                                       { return s.name }
func (s *stubStrategy) Init(_ context.Context) error                                       { return nil }
func (s *stubStrategy) OnBar(_ context.Context, _ domain.Bar) ([]domain.Signal, error)     { return nil, nil }
func (s *stubStrategy) OnTrade(_ context.Context, _ domain.Trade) ([]domain.Signal, error) { return nil, nil }

func TestRegistryRegisterAndGet(t *testing.T) {
	r := NewRegistry()
	s := &stubStrategy{name: "test-strategy"}

	r.Register(s)

	got, ok := r.Get("test-strategy")
	if !ok {
		t.Fatal("Get returned false for registered strategy")
	}
	if got.Name() != "test-strategy" {
		t.Errorf("Get returned strategy with Name() = %q, want %q", got.Name(), "test-strategy")
	}
}

func TestRegistryGet_NotFound(t *testing.T) {
	r := NewRegistry()
	_, ok := r.Get("nonexistent")
	if ok {
		t.Error("Get returned true for unregistered strategy")
	}
}

func TestRegistryList(t *testing.T) {
	r := NewRegistry()
	r.Register(&stubStrategy{name: "alpha"})
	r.Register(&stubStrategy{name: "beta"})

	names := r.List()
	if len(names) != 2 {
		t.Fatalf("List returned %d names, want 2", len(names))
	}
	// List returns sorted names.
	if names[0] != "alpha" || names[1] != "beta" {
		t.Errorf("List returned %v, want [alpha beta]", names)
	}
}
