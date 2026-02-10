// Package broker defines the Broker interface and provides implementations
// for executing orders and managing accounts across different brokerages.
package broker

import (
	"context"

	"jupitor/internal/domain"
)

// Broker abstracts brokerage operations for order execution and account management.
type Broker interface {
	// Name returns the broker identifier (e.g. "alpaca", "simulator").
	Name() string

	// SubmitOrder sends an order to the brokerage for execution.
	SubmitOrder(ctx context.Context, order *domain.Order) (*domain.Order, error)

	// CancelOrder requests cancellation of an open order by its ID.
	CancelOrder(ctx context.Context, orderID string) error

	// GetPositions returns all current positions held at the brokerage.
	GetPositions(ctx context.Context) ([]domain.Position, error)

	// GetAccount returns a snapshot of the account's financial metrics.
	GetAccount(ctx context.Context) (*domain.AccountInfo, error)
}
