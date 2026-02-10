// Package builtins provides built-in strategy implementations that ship with
// the jupitor platform.
package builtins

import (
	"context"

	"jupitor/internal/domain"
	"jupitor/internal/strategy"
)

// Compile-time interface check.
var _ strategy.Strategy = (*SMACross)(nil)

// SMACross implements a simple moving average crossover strategy. It generates
// a buy signal when the short-period SMA crosses above the long-period SMA,
// and a sell signal when it crosses below.
type SMACross struct {
	shortPeriod int
	longPeriod  int
}

// NewSMACross creates a new SMACross strategy with the specified short and
// long moving average periods.
func NewSMACross(short, long int) *SMACross {
	return &SMACross{
		shortPeriod: short,
		longPeriod:  long,
	}
}

// Name returns "sma-cross".
func (s *SMACross) Name() string {
	return "sma-cross"
}

// Init performs any setup required by the SMA crossover strategy.
func (s *SMACross) Init(_ context.Context) error {
	// TODO: pre-allocate price buffers for SMA computation
	return nil
}

// OnBar processes a new bar and returns trading signals based on SMA crossover
// logic.
func (s *SMACross) OnBar(_ context.Context, _ domain.Bar) ([]domain.Signal, error) {
	// TODO: append bar close to price history
	// TODO: compute short and long SMAs when enough data is available
	// TODO: detect crossover and generate buy/sell signal
	return nil, nil
}

// OnTrade processes a new trade tick. The SMA crossover strategy does not
// generate signals from individual trades.
func (s *SMACross) OnTrade(_ context.Context, _ domain.Trade) ([]domain.Signal, error) {
	// TODO: this strategy operates on bars; trade-level signals are not used
	return nil, nil
}
