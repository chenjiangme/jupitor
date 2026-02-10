package cn

import (
	"context"
	"fmt"

	"jupitor/internal/domain"
	"jupitor/internal/gather"
	"jupitor/internal/store"
)

// ---------------------------------------------------------------------------
// Compile-time interface check
// ---------------------------------------------------------------------------

var _ gather.Gatherer = (*DailyBarGatherer)(nil)

// ---------------------------------------------------------------------------
// BaoStockClient — low-level TCP client for the BaoStock data service.
// ---------------------------------------------------------------------------

// BaoStockClient communicates with the BaoStock server over a custom TCP
// protocol to retrieve China A-share market data.
type BaoStockClient struct {
	host string
	port int
}

// NewBaoStockClient creates a BaoStockClient targeting the given host and
// port.
func NewBaoStockClient(host string, port int) *BaoStockClient {
	return &BaoStockClient{
		host: host,
		port: port,
	}
}

// Connect establishes a TCP connection to the BaoStock server.
func (c *BaoStockClient) Connect(ctx context.Context) error {
	// TODO: Dial tcp c.host:c.port and store the connection.
	return fmt.Errorf("BaoStockClient.Connect: not implemented")
}

// Close shuts down the TCP connection.
func (c *BaoStockClient) Close() error {
	// TODO: Close the underlying TCP connection.
	return fmt.Errorf("BaoStockClient.Close: not implemented")
}

// Login authenticates the session with the BaoStock server.
func (c *BaoStockClient) Login(ctx context.Context) error {
	// TODO: Send login request message and parse response.
	return fmt.Errorf("BaoStockClient.Login: not implemented")
}

// Logout terminates the authenticated session.
func (c *BaoStockClient) Logout(ctx context.Context) error {
	// TODO: Send logout request message and parse response.
	return fmt.Errorf("BaoStockClient.Logout: not implemented")
}

// QueryDailyBars retrieves daily OHLCV bars for the given symbol between
// start and end dates (formatted as "YYYY-MM-DD").
func (c *BaoStockClient) QueryDailyBars(ctx context.Context, symbol string, start, end string) ([]domain.Bar, error) {
	// TODO: Build query message, send over TCP, parse tabular response into
	// []domain.Bar.
	return nil, fmt.Errorf("BaoStockClient.QueryDailyBars: not implemented")
}

// ---------------------------------------------------------------------------
// DailyBarGatherer — orchestrates daily bar collection for China A-shares.
// ---------------------------------------------------------------------------

// DailyBarGatherer uses a BaoStockClient to fetch daily bars and persists
// them through a BarStore.
type DailyBarGatherer struct {
	client    *BaoStockClient
	store     store.BarStore
	startDate string
}

// NewDailyBarGatherer creates a DailyBarGatherer with the given client,
// store, and start date.
func NewDailyBarGatherer(client *BaoStockClient, store store.BarStore, startDate string) *DailyBarGatherer {
	return &DailyBarGatherer{
		client:    client,
		store:     store,
		startDate: startDate,
	}
}

// Name returns the gatherer identifier.
func (g *DailyBarGatherer) Name() string { return "cn-daily" }

// Run starts the China A-share daily bar gathering process. It blocks until
// ctx is cancelled.
func (g *DailyBarGatherer) Run(ctx context.Context) error {
	// TODO: Implement daily bar gathering via BaoStock.
	//  1. Connect and login via g.client.
	//  2. List A-share symbols (or use a pre-configured list).
	//  3. For each symbol, call QueryDailyBars from startDate to today.
	//  4. Write bars to g.store.
	//  5. Logout and close on completion or context cancellation.
	return fmt.Errorf("DailyBarGatherer.Run: not implemented")
}
