package jupitor

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Client provides a Go SDK for interacting with the jupitor-server API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new jupitor API client.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// GetBars retrieves daily bars for a symbol.
func (c *Client) GetBars(ctx context.Context, symbol, market string, start, end time.Time) ([]byte, error) {
	// TODO: implement GET /api/v1/bars?symbol=...&market=...&start=...&end=...
	return nil, fmt.Errorf("GetBars: not implemented")
}

// GetPositions retrieves current positions.
func (c *Client) GetPositions(ctx context.Context) ([]byte, error) {
	// TODO: implement GET /api/v1/positions
	return nil, fmt.Errorf("GetPositions: not implemented")
}

// GetAccount retrieves account information.
func (c *Client) GetAccount(ctx context.Context) ([]byte, error) {
	// TODO: implement GET /api/v1/account
	return nil, fmt.Errorf("GetAccount: not implemented")
}

// SubmitOrder submits a new order.
func (c *Client) SubmitOrder(ctx context.Context, order []byte) ([]byte, error) {
	// TODO: implement POST /api/v1/orders
	return nil, fmt.Errorf("SubmitOrder: not implemented")
}
