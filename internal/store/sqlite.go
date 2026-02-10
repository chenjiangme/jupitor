package store

import (
	"context"
	"database/sql"

	"jupitor/internal/domain"

	_ "modernc.org/sqlite" // Pure-Go SQLite driver.
)

// Compile-time interface checks.
var _ OrderStore = (*SQLiteStore)(nil)
var _ PositionStore = (*SQLiteStore)(nil)
var _ SignalStore = (*SQLiteStore)(nil)

// SQLiteStore implements OrderStore, PositionStore, and SignalStore backed by
// a SQLite database.
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore opens (or creates) a SQLite database at dbPath and returns
// a ready-to-use SQLiteStore.
func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	// TODO: run migrations / create tables
	return &SQLiteStore{db: db}, nil
}

// Close closes the underlying database connection.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// ---------------------------------------------------------------------------
// OrderStore implementation
// ---------------------------------------------------------------------------

// SaveOrder inserts a new order into the database.
func (s *SQLiteStore) SaveOrder(_ context.Context, _ *domain.Order) error {
	// TODO: implement INSERT INTO orders
	return nil
}

// GetOrder retrieves a single order by its ID.
func (s *SQLiteStore) GetOrder(_ context.Context, _ string) (*domain.Order, error) {
	// TODO: implement SELECT FROM orders WHERE id = ?
	return nil, nil
}

// ListOrders returns all orders matching the given status.
func (s *SQLiteStore) ListOrders(_ context.Context, _ domain.OrderStatus) ([]domain.Order, error) {
	// TODO: implement SELECT FROM orders WHERE status = ?
	return nil, nil
}

// UpdateOrder persists changes to an existing order.
func (s *SQLiteStore) UpdateOrder(_ context.Context, _ *domain.Order) error {
	// TODO: implement UPDATE orders SET ... WHERE id = ?
	return nil
}

// ---------------------------------------------------------------------------
// PositionStore implementation
// ---------------------------------------------------------------------------

// SavePosition inserts or updates a position for a symbol.
func (s *SQLiteStore) SavePosition(_ context.Context, _ *domain.Position) error {
	// TODO: implement INSERT OR REPLACE INTO positions
	return nil
}

// GetPosition retrieves the current position for a symbol.
func (s *SQLiteStore) GetPosition(_ context.Context, _ string) (*domain.Position, error) {
	// TODO: implement SELECT FROM positions WHERE symbol = ?
	return nil, nil
}

// ListPositions returns all open positions.
func (s *SQLiteStore) ListPositions(_ context.Context) ([]domain.Position, error) {
	// TODO: implement SELECT FROM positions
	return nil, nil
}

// DeletePosition removes the position for a symbol.
func (s *SQLiteStore) DeletePosition(_ context.Context, _ string) error {
	// TODO: implement DELETE FROM positions WHERE symbol = ?
	return nil
}

// ---------------------------------------------------------------------------
// SignalStore implementation
// ---------------------------------------------------------------------------

// SaveSignal inserts a new signal into the database.
func (s *SQLiteStore) SaveSignal(_ context.Context, _ *domain.Signal) error {
	// TODO: implement INSERT INTO signals
	return nil
}

// ListSignals returns the most recent signals for a strategy, up to limit.
func (s *SQLiteStore) ListSignals(_ context.Context, _ string, _ int) ([]domain.Signal, error) {
	// TODO: implement SELECT FROM signals WHERE strategy_id = ? ORDER BY created_at DESC LIMIT ?
	return nil, nil
}
