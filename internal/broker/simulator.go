package broker

import (
	"context"

	"jupitor/internal/domain"
)

// Compile-time interface check.
var _ Broker = (*SimulatorBroker)(nil)

// SimulatorBroker implements the Broker interface for paper trading and
// backtesting. It tracks positions and orders in memory without making
// external API calls.
type SimulatorBroker struct {
	positions map[string]*domain.Position
	orders    map[string]*domain.Order
}

// NewSimulatorBroker creates a new SimulatorBroker with empty position and
// order maps.
func NewSimulatorBroker() *SimulatorBroker {
	return &SimulatorBroker{
		positions: make(map[string]*domain.Position),
		orders:    make(map[string]*domain.Order),
	}
}

// Name returns "simulator".
func (b *SimulatorBroker) Name() string {
	return "simulator"
}

// SubmitOrder records the order in memory and simulates immediate execution.
func (b *SimulatorBroker) SubmitOrder(_ context.Context, order *domain.Order) (*domain.Order, error) {
	// TODO: simulate order fill logic — update order status, adjust positions
	b.orders[order.ID] = order
	return order, nil
}

// CancelOrder marks the specified order as cancelled in the in-memory store.
func (b *SimulatorBroker) CancelOrder(_ context.Context, orderID string) error {
	// TODO: look up order, verify it is cancellable, set status to cancelled
	if o, ok := b.orders[orderID]; ok {
		o.Status = domain.OrderStatusCancelled
	}
	return nil
}

// GetPositions returns all simulated positions.
func (b *SimulatorBroker) GetPositions(_ context.Context) ([]domain.Position, error) {
	// TODO: convert map to slice with proper deep copies
	positions := make([]domain.Position, 0, len(b.positions))
	for _, p := range b.positions {
		positions = append(positions, *p)
	}
	return positions, nil
}

// GetAccount returns simulated account information.
func (b *SimulatorBroker) GetAccount(_ context.Context) (*domain.AccountInfo, error) {
	// TODO: compute equity, cash, buying power from simulated state
	return &domain.AccountInfo{}, nil
}
