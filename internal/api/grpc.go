package api

import (
	"context"

	"jupitor/internal/store"
)

// MarketDataService provides gRPC endpoints for querying historical and
// real-time market data.
type MarketDataService struct {
	barStore   store.BarStore
	tradeStore store.TradeStore
}

// NewMarketDataService creates a MarketDataService backed by the given stores.
func NewMarketDataService(barStore store.BarStore, tradeStore store.TradeStore) *MarketDataService {
	return &MarketDataService{
		barStore:   barStore,
		tradeStore: tradeStore,
	}
}

// TradingService provides gRPC endpoints for order submission and position
// management.
type TradingService struct {
	orderStore    store.OrderStore
	positionStore store.PositionStore
}

// NewTradingService creates a TradingService backed by the given stores.
func NewTradingService(orderStore store.OrderStore, positionStore store.PositionStore) *TradingService {
	return &TradingService{
		orderStore:    orderStore,
		positionStore: positionStore,
	}
}

// StrategyService provides gRPC endpoints for managing and monitoring
// trading strategies.
type StrategyService struct {
	signalStore store.SignalStore
}

// NewStrategyService creates a StrategyService backed by the given store.
func NewStrategyService(signalStore store.SignalStore) *StrategyService {
	return &StrategyService{
		signalStore: signalStore,
	}
}

// Ensure context import is used (referenced by future gRPC handler signatures).
var _ context.Context
