// Package store defines storage interfaces for persisting and retrieving
// domain objects such as bars, trades, orders, positions, and signals.
package store

import (
	"context"
	"time"

	"jupitor/internal/domain"
)

// BarStore persists and retrieves OHLCV bar data.
type BarStore interface {
	// WriteBars persists a batch of bars to storage.
	WriteBars(ctx context.Context, bars []domain.Bar) error

	// ReadBars returns bars for the given symbol and market within [start, end].
	ReadBars(ctx context.Context, symbol string, market string, start, end time.Time) ([]domain.Bar, error)

	// ListSymbols returns all distinct symbols available in the given market.
	ListSymbols(ctx context.Context, market string) ([]string, error)
}

// TradeStore persists and retrieves individual trade (tick) data.
type TradeStore interface {
	// WriteTrades persists a batch of trades to storage.
	WriteTrades(ctx context.Context, trades []domain.Trade) error

	// ReadTrades returns trades for the given symbol within [start, end].
	ReadTrades(ctx context.Context, symbol string, start, end time.Time) ([]domain.Trade, error)
}

// OrderStore persists and retrieves order records.
type OrderStore interface {
	// SaveOrder inserts a new order into storage.
	SaveOrder(ctx context.Context, order *domain.Order) error

	// GetOrder retrieves a single order by its ID.
	GetOrder(ctx context.Context, id string) (*domain.Order, error)

	// ListOrders returns all orders matching the given status.
	ListOrders(ctx context.Context, status domain.OrderStatus) ([]domain.Order, error)

	// UpdateOrder persists changes to an existing order.
	UpdateOrder(ctx context.Context, order *domain.Order) error
}

// PositionStore persists and retrieves position records.
type PositionStore interface {
	// SavePosition inserts or updates a position for a symbol.
	SavePosition(ctx context.Context, pos *domain.Position) error

	// GetPosition retrieves the current position for a symbol.
	GetPosition(ctx context.Context, symbol string) (*domain.Position, error)

	// ListPositions returns all open positions.
	ListPositions(ctx context.Context) ([]domain.Position, error)

	// DeletePosition removes the position for a symbol.
	DeletePosition(ctx context.Context, symbol string) error
}

// SignalStore persists and retrieves trading signals.
type SignalStore interface {
	// SaveSignal inserts a new signal into storage.
	SaveSignal(ctx context.Context, signal *domain.Signal) error

	// ListSignals returns the most recent signals for a strategy, up to limit.
	ListSignals(ctx context.Context, strategyID string, limit int) ([]domain.Signal, error)
}
