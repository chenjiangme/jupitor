package broker

import (
	"context"

	"jupitor/internal/domain"
)

// Compile-time interface check.
var _ Broker = (*AlpacaBroker)(nil)

// AlpacaBroker implements the Broker interface using the Alpaca brokerage API.
type AlpacaBroker struct {
	apiKey    string
	apiSecret string
	baseURL   string
}

// NewAlpacaBroker creates a new AlpacaBroker configured with the given
// credentials and API endpoint.
func NewAlpacaBroker(apiKey, apiSecret, baseURL string) *AlpacaBroker {
	return &AlpacaBroker{
		apiKey:    apiKey,
		apiSecret: apiSecret,
		baseURL:   baseURL,
	}
}

// Name returns "alpaca".
func (b *AlpacaBroker) Name() string {
	return "alpaca"
}

// SubmitOrder sends an order to the Alpaca API for execution.
func (b *AlpacaBroker) SubmitOrder(_ context.Context, order *domain.Order) (*domain.Order, error) {
	// TODO: implement Alpaca REST API call to POST /v2/orders
	return order, nil
}

// CancelOrder requests cancellation of an open order via the Alpaca API.
func (b *AlpacaBroker) CancelOrder(_ context.Context, _ string) error {
	// TODO: implement Alpaca REST API call to DELETE /v2/orders/{orderID}
	return nil
}

// GetPositions returns all current positions from the Alpaca account.
func (b *AlpacaBroker) GetPositions(_ context.Context) ([]domain.Position, error) {
	// TODO: implement Alpaca REST API call to GET /v2/positions
	return nil, nil
}

// GetAccount returns the current account information from the Alpaca API.
func (b *AlpacaBroker) GetAccount(_ context.Context) (*domain.AccountInfo, error) {
	// TODO: implement Alpaca REST API call to GET /v2/account
	return &domain.AccountInfo{}, nil
}
