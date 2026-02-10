package engine

import (
	"context"

	"jupitor/internal/domain"
)

// RiskManager enforces pre-trade risk rules such as position sizing limits
// and maximum daily loss constraints.
type RiskManager struct {
	maxPositionPct  float64
	maxDailyLossPct float64
}

// NewRiskManager creates a RiskManager with the specified risk thresholds.
//
//   - maxPositionPct: maximum fraction of equity allowed in a single position
//     (e.g. 0.10 for 10%).
//   - maxDailyLossPct: maximum fraction of equity that may be lost in a single
//     trading day (e.g. 0.02 for 2%).
func NewRiskManager(maxPositionPct, maxDailyLossPct float64) *RiskManager {
	return &RiskManager{
		maxPositionPct:  maxPositionPct,
		maxDailyLossPct: maxDailyLossPct,
	}
}

// CheckOrder evaluates whether the proposed order complies with the
// configured risk limits given the current account state.
func (rm *RiskManager) CheckOrder(_ context.Context, _ *domain.Order, _ *domain.AccountInfo) error {
	// TODO: check that the order's notional value does not exceed
	// maxPositionPct of account equity.
	// TODO: check that filling this order would not push daily P&L loss
	// beyond maxDailyLossPct of account equity.
	return nil
}
