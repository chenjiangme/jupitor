package strategy

import (
	"context"
	"time"

	"jupitor/internal/store"
)

// BacktestResult holds the summary metrics produced by a backtest run.
type BacktestResult struct {
	TotalReturn  float64
	SharpeRatio  float64
	MaxDrawdown  float64
	TotalTrades  int
	WinRate      float64
	ProfitFactor float64
}

// Backtester replays historical bar data through a strategy and computes
// performance metrics.
type Backtester struct {
	store    store.BarStore
	registry *Registry
}

// NewBacktester creates a Backtester that reads bars from the given store and
// looks up strategies in the provided registry.
func NewBacktester(barStore store.BarStore, registry *Registry) *Backtester {
	return &Backtester{
		store:    barStore,
		registry: registry,
	}
}

// Run executes a backtest for the named strategy over the specified symbols
// and date range, starting with initialCapital.
func (bt *Backtester) Run(
	_ context.Context,
	_ string,
	_ []string,
	_, _ time.Time,
	_ float64,
) (*BacktestResult, error) {
	// TODO: look up strategy by name from registry
	// TODO: iterate over date range, read bars from store
	// TODO: feed each bar to strategy.OnBar, collect signals
	// TODO: simulate order execution and track equity curve
	// TODO: compute and return BacktestResult metrics
	return &BacktestResult{}, nil
}
