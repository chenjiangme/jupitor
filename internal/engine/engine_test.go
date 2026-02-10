package engine

import (
	"context"
	"testing"

	"jupitor/internal/domain"
)

func TestNewEngine(t *testing.T) {
	e := NewEngine(nil, nil, nil, nil)
	if e == nil {
		t.Fatal("NewEngine returned nil")
	}
}

func TestRiskManagerCheckOrder(t *testing.T) {
	rm := NewRiskManager(0.10, 0.02)

	order := &domain.Order{
		ID:     "test-order-1",
		Symbol: "AAPL",
		Side:   domain.OrderSideBuy,
		Type:   domain.OrderTypeMarket,
		Qty:    10,
	}
	account := &domain.AccountInfo{
		Equity:      100000,
		Cash:        50000,
		BuyingPower: 200000,
	}

	err := rm.CheckOrder(context.Background(), order, account)
	if err != nil {
		t.Fatalf("CheckOrder returned unexpected error: %v", err)
	}
}
