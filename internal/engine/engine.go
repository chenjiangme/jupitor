// Package engine coordinates order management, position tracking, and risk
// checking across the trading system.
package engine

import (
	"context"

	"jupitor/internal/broker"
	"jupitor/internal/domain"
	"jupitor/internal/store"
)

// Engine orchestrates the trading lifecycle by delegating to a broker for
// execution, stores for persistence, and a risk manager for pre-trade checks.
type Engine struct {
	broker      broker.Broker
	orders      store.OrderStore
	positions   store.PositionStore
	riskChecker *RiskManager
}

// NewEngine creates a new Engine wired with the given dependencies.
func NewEngine(
	b broker.Broker,
	orders store.OrderStore,
	positions store.PositionStore,
	riskChecker *RiskManager,
) *Engine {
	return &Engine{
		broker:      b,
		orders:      orders,
		positions:   positions,
		riskChecker: riskChecker,
	}
}

// SubmitOrder validates the order against risk rules and then forwards it to
// the broker for execution.
func (e *Engine) SubmitOrder(ctx context.Context, order *domain.Order) (*domain.Order, error) {
	// TODO: call riskChecker.CheckOrder, persist order via store, submit via broker
	_ = ctx
	return order, nil
}

// CancelOrder requests cancellation of an open order.
func (e *Engine) CancelOrder(ctx context.Context, orderID string) error {
	// TODO: look up order in store, delegate to broker.CancelOrder, update status
	_ = ctx
	_ = orderID
	return nil
}

// GetPositions returns all currently open positions.
func (e *Engine) GetPositions(ctx context.Context) ([]domain.Position, error) {
	// TODO: read from positions store or delegate to broker
	_ = ctx
	return nil, nil
}
