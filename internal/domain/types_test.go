package domain

import (
	"testing"
	"time"
)

func TestTypesExist(t *testing.T) {
	// Verify Bar can be instantiated with zero values.
	bar := Bar{}
	if bar.Symbol != "" {
		t.Error("expected empty Symbol for zero-value Bar")
	}
	if !bar.Timestamp.IsZero() {
		t.Error("expected zero Timestamp for zero-value Bar")
	}
	if bar.Open != 0 || bar.High != 0 || bar.Low != 0 || bar.Close != 0 {
		t.Error("expected zero OHLC values for zero-value Bar")
	}
	if bar.Volume != 0 || bar.TradeCount != 0 || bar.VWAP != 0 {
		t.Error("expected zero Volume/TradeCount/VWAP for zero-value Bar")
	}

	// Verify Trade can be instantiated with zero values.
	trade := Trade{}
	if trade.Symbol != "" {
		t.Error("expected empty Symbol for zero-value Trade")
	}
	if trade.Price != 0 || trade.Size != 0 {
		t.Error("expected zero Price/Size for zero-value Trade")
	}
	if trade.Exchange != "" || trade.ID != "" {
		t.Error("expected empty Exchange/ID for zero-value Trade")
	}

	// Verify Order can be instantiated with zero values.
	order := Order{}
	if order.ID != "" {
		t.Error("expected empty ID for zero-value Order")
	}
	if order.Side != "" {
		t.Error("expected empty Side for zero-value Order")
	}
	if order.Type != "" {
		t.Error("expected empty Type for zero-value Order")
	}
	if order.Status != "" {
		t.Error("expected empty Status for zero-value Order")
	}
	if order.Qty != 0 || order.FilledQty != 0 || order.FilledAvgPrice != 0 {
		t.Error("expected zero Qty/FilledQty/FilledAvgPrice for zero-value Order")
	}
	if !order.CreatedAt.IsZero() || !order.UpdatedAt.IsZero() {
		t.Error("expected zero timestamps for zero-value Order")
	}

	// Verify enum constants are defined correctly.
	if OrderSideBuy != "buy" {
		t.Errorf("OrderSideBuy = %q, want %q", OrderSideBuy, "buy")
	}
	if MarketUS != "us" || MarketCN != "cn" {
		t.Error("Market constants have unexpected values")
	}

	// Verify structs can be constructed with real values.
	now := time.Now()
	signal := Signal{
		ID:         1,
		StrategyID: "momentum_v1",
		Symbol:     "AAPL",
		Type:       SignalTypeBuy,
		Strength:   0.85,
		Metadata:   map[string]string{"reason": "breakout"},
		CreatedAt:  now,
	}
	if signal.StrategyID != "momentum_v1" {
		t.Errorf("signal.StrategyID = %q, want %q", signal.StrategyID, "momentum_v1")
	}

	pos := Position{
		Symbol: "AAPL",
		Qty:    100,
		Side:   PositionSideLong,
	}
	if pos.Side != PositionSideLong {
		t.Errorf("pos.Side = %q, want %q", pos.Side, PositionSideLong)
	}
}
