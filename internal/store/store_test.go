package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"jupitor/internal/domain"
)

func TestParquetStorePath(t *testing.T) {
	ps := NewParquetStore("/data")

	// Test barPath produces the expected layout.
	ts := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	bp := ps.barPath("AAPL", "us", ts)

	wantBarPath := filepath.Join("/data", "us", "daily", "AAPL", "2024.parquet")
	if bp != wantBarPath {
		t.Errorf("barPath mismatch:\n  got  %s\n  want %s", bp, wantBarPath)
	}
	if !strings.Contains(bp, "us") {
		t.Errorf("barPath should contain market segment 'us': %s", bp)
	}
	if !strings.Contains(bp, "AAPL") {
		t.Errorf("barPath should contain symbol 'AAPL': %s", bp)
	}
	if !strings.Contains(bp, "2024.parquet") {
		t.Errorf("barPath should contain year file '2024.parquet': %s", bp)
	}

	// Test tradePath produces the expected layout.
	tp := ps.tradePath("TSLA", ts)

	wantTradePath := filepath.Join("/data", "us", "trades", "TSLA", "2024-06-15.parquet")
	if tp != wantTradePath {
		t.Errorf("tradePath mismatch:\n  got  %s\n  want %s", tp, wantTradePath)
	}
	if !strings.Contains(tp, "trades") {
		t.Errorf("tradePath should contain 'trades': %s", tp)
	}
	if !strings.Contains(tp, "TSLA") {
		t.Errorf("tradePath should contain symbol 'TSLA': %s", tp)
	}
	if !strings.Contains(tp, "2024-06-15.parquet") {
		t.Errorf("tradePath should contain date file '2024-06-15.parquet': %s", tp)
	}
}

func TestParquetStoreWriteReadBars(t *testing.T) {
	dir := t.TempDir()
	ps := NewParquetStore(dir)
	ctx := context.Background()

	bars := []domain.Bar{
		{
			Symbol:     "AAPL",
			Timestamp:  time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			Open:       185.0,
			High:       186.5,
			Low:        184.0,
			Close:      185.5,
			Volume:     50000000,
			TradeCount: 500000,
			VWAP:       185.25,
		},
		{
			Symbol:     "AAPL",
			Timestamp:  time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			Open:       185.5,
			High:       187.0,
			Low:        185.0,
			Close:      186.0,
			Volume:     45000000,
			TradeCount: 450000,
			VWAP:       185.75,
		},
	}

	// Write bars.
	if err := ps.WriteBars(ctx, bars); err != nil {
		t.Fatalf("WriteBars: %v", err)
	}

	// Read them back.
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)
	got, err := ps.ReadBars(ctx, "AAPL", "us", start, end)
	if err != nil {
		t.Fatalf("ReadBars: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ReadBars returned %d bars, want 2", len(got))
	}
	if got[0].Close != 185.5 {
		t.Errorf("first bar Close = %v, want 185.5", got[0].Close)
	}
	if got[1].Close != 186.0 {
		t.Errorf("second bar Close = %v, want 186.0", got[1].Close)
	}
}

func TestParquetStoreMergeBars(t *testing.T) {
	dir := t.TempDir()
	ps := NewParquetStore(dir)
	ctx := context.Background()

	// Write initial bar.
	bars1 := []domain.Bar{
		{
			Symbol:    "MSFT",
			Timestamp: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			Open:      400.0, High: 405.0, Low: 399.0, Close: 403.0,
			Volume: 30000000, TradeCount: 300000, VWAP: 402.0,
		},
	}
	if err := ps.WriteBars(ctx, bars1); err != nil {
		t.Fatalf("WriteBars (first): %v", err)
	}

	// Write another bar for same symbol+year — should merge, not overwrite.
	bars2 := []domain.Bar{
		{
			Symbol:    "MSFT",
			Timestamp: time.Date(2024, 3, 4, 0, 0, 0, 0, time.UTC),
			Open:      403.0, High: 410.0, Low: 402.0, Close: 408.0,
			Volume: 35000000, TradeCount: 350000, VWAP: 406.0,
		},
	}
	if err := ps.WriteBars(ctx, bars2); err != nil {
		t.Fatalf("WriteBars (second): %v", err)
	}

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)
	got, err := ps.ReadBars(ctx, "MSFT", "us", start, end)
	if err != nil {
		t.Fatalf("ReadBars: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ReadBars returned %d bars after merge, want 2", len(got))
	}
}

func TestParquetStoreListSymbols(t *testing.T) {
	dir := t.TempDir()
	ps := NewParquetStore(dir)
	ctx := context.Background()

	// Write bars for two symbols.
	bars := []domain.Bar{
		{Symbol: "AAPL", Timestamp: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Open: 185.0, High: 186.0, Low: 184.0, Close: 185.5, Volume: 50000000},
		{Symbol: "GOOGL", Timestamp: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Open: 140.0, High: 141.0, Low: 139.0, Close: 140.5, Volume: 20000000},
	}
	if err := ps.WriteBars(ctx, bars); err != nil {
		t.Fatalf("WriteBars: %v", err)
	}

	symbols, err := ps.ListSymbols(ctx, "us")
	if err != nil {
		t.Fatalf("ListSymbols: %v", err)
	}
	if len(symbols) != 2 {
		t.Fatalf("ListSymbols returned %d symbols, want 2", len(symbols))
	}
	if symbols[0] != "AAPL" || symbols[1] != "GOOGL" {
		t.Errorf("ListSymbols = %v, want [AAPL GOOGL]", symbols)
	}
}

func TestSQLiteStoreOpen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore(%q) returned error: %v", dbPath, err)
	}
	defer func() {
		if cerr := store.Close(); cerr != nil {
			t.Errorf("Close() returned error: %v", cerr)
		}
	}()

	// Verify the store is usable by pinging the database.
	if err := store.db.Ping(); err != nil {
		t.Fatalf("db.Ping() returned error: %v", err)
	}
}
