package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"jupitor/internal/domain"
)

// Compile-time interface checks.
var _ BarStore = (*ParquetStore)(nil)
var _ TradeStore = (*ParquetStore)(nil)

// ParquetStore implements BarStore and TradeStore using Parquet files on disk.
type ParquetStore struct {
	DataDir string
}

// NewParquetStore creates a new ParquetStore rooted at the given data directory.
func NewParquetStore(dataDir string) *ParquetStore {
	return &ParquetStore{DataDir: dataDir}
}

// ---------------------------------------------------------------------------
// Parquet record types (on-disk schema)
// ---------------------------------------------------------------------------

// BarRecord is the Parquet schema for daily bar data.
type BarRecord struct {
	Symbol     string  `parquet:"symbol"`
	Timestamp  int64   `parquet:"timestamp,timestamp(millisecond)"` // Unix ms
	Open       float64 `parquet:"open"`
	High       float64 `parquet:"high"`
	Low        float64 `parquet:"low"`
	Close      float64 `parquet:"close"`
	Volume     int64   `parquet:"volume"`
	TradeCount int64   `parquet:"trade_count"`
	VWAP       float64 `parquet:"vwap"`
}

// TradeRecord is the Parquet schema for trade tick data.
type TradeRecord struct {
	Symbol    string  `parquet:"symbol"`
	Timestamp int64   `parquet:"timestamp,timestamp(millisecond)"` // Unix ms
	Price     float64 `parquet:"price"`
	Size      int64   `parquet:"size"`
	Exchange  string  `parquet:"exchange"`
	ID        string  `parquet:"id"`
}

// ---------------------------------------------------------------------------
// BarStore implementation
// ---------------------------------------------------------------------------

// WriteBars writes bar data to Parquet files organized by symbol and year.
// Each symbol+year combination produces a separate file at:
//
//	<DataDir>/<market>/daily/<SYMBOL>/<YYYY>.parquet
func (s *ParquetStore) WriteBars(_ context.Context, bars []domain.Bar) error {
	if len(bars) == 0 {
		return nil
	}

	// Group bars by (symbol, market, year) → we need market info.
	// Convention: the market is inferred from the path structure. For WriteBars,
	// callers must set a consistent market. We default to "us" and allow an
	// override via WriteBarsForMarket.
	return s.WriteBarsForMarket(bars, "us")
}

// WriteBarsForMarket writes bars to Parquet grouped by symbol and year under
// the given market directory.
func (s *ParquetStore) WriteBarsForMarket(bars []domain.Bar, market string) error {
	// Group by symbol → year.
	type key struct {
		symbol string
		year   int
	}
	groups := make(map[key][]BarRecord)
	for _, b := range bars {
		k := key{symbol: b.Symbol, year: b.Timestamp.Year()}
		groups[k] = append(groups[k], BarRecord{
			Symbol:     b.Symbol,
			Timestamp:  b.Timestamp.UnixMilli(),
			Open:       b.Open,
			High:       b.High,
			Low:        b.Low,
			Close:      b.Close,
			Volume:     b.Volume,
			TradeCount: b.TradeCount,
			VWAP:       b.VWAP,
		})
	}

	for k, records := range groups {
		path := s.barPath(k.symbol, market, time.Date(k.year, 1, 1, 0, 0, 0, 0, time.UTC))

		// Read existing records to merge.
		existing, _ := readParquetFile[BarRecord](path)
		merged := mergeBarRecords(existing, records)

		if err := writeParquetFile(path, merged); err != nil {
			return fmt.Errorf("writing bars for %s/%d: %w", k.symbol, k.year, err)
		}
	}
	return nil
}

// ReadBars reads bar data from Parquet files for the given symbol and time range.
func (s *ParquetStore) ReadBars(_ context.Context, symbol string, market string, start, end time.Time) ([]domain.Bar, error) {
	// Determine which year files to read.
	var bars []domain.Bar
	for year := start.Year(); year <= end.Year(); year++ {
		path := s.barPath(symbol, market, time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC))

		records, err := readParquetFile[BarRecord](path)
		if err != nil {
			// File doesn't exist for this year — skip.
			continue
		}

		for _, r := range records {
			ts := time.UnixMilli(r.Timestamp)
			if (ts.Equal(start) || ts.After(start)) && (ts.Equal(end) || ts.Before(end)) {
				bars = append(bars, domain.Bar{
					Symbol:     r.Symbol,
					Timestamp:  ts,
					Open:       r.Open,
					High:       r.High,
					Low:        r.Low,
					Close:      r.Close,
					Volume:     r.Volume,
					TradeCount: r.TradeCount,
					VWAP:       r.VWAP,
				})
			}
		}
	}
	return bars, nil
}

// ListSymbols lists all symbols that have bar data in the given market.
func (s *ParquetStore) ListSymbols(_ context.Context, market string) ([]string, error) {
	dir := filepath.Join(s.DataDir, market, "daily")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var symbols []string
	for _, e := range entries {
		if e.IsDir() {
			symbols = append(symbols, e.Name())
		}
	}
	sort.Strings(symbols)
	return symbols, nil
}

// ---------------------------------------------------------------------------
// TradeStore implementation
// ---------------------------------------------------------------------------

// WriteTrades writes trade data to Parquet files organized by symbol and date.
func (s *ParquetStore) WriteTrades(_ context.Context, trades []domain.Trade) error {
	if len(trades) == 0 {
		return nil
	}

	type key struct {
		symbol string
		date   string // YYYY-MM-DD
	}
	groups := make(map[key][]TradeRecord)
	for _, t := range trades {
		k := key{symbol: t.Symbol, date: t.Timestamp.Format("2006-01-02")}
		groups[k] = append(groups[k], TradeRecord{
			Symbol:    t.Symbol,
			Timestamp: t.Timestamp.UnixMilli(),
			Price:     t.Price,
			Size:      t.Size,
			Exchange:  t.Exchange,
			ID:        t.ID,
		})
	}

	for k, records := range groups {
		t, _ := time.Parse("2006-01-02", k.date)
		path := s.tradePath(k.symbol, t)

		existing, _ := readParquetFile[TradeRecord](path)
		merged := mergeTradeRecords(existing, records)

		if err := writeParquetFile(path, merged); err != nil {
			return fmt.Errorf("writing trades for %s/%s: %w", k.symbol, k.date, err)
		}
	}
	return nil
}

// ReadTrades reads trade data from Parquet files for the given symbol and time range.
func (s *ParquetStore) ReadTrades(_ context.Context, symbol string, start, end time.Time) ([]domain.Trade, error) {
	var trades []domain.Trade
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		path := s.tradePath(symbol, d)
		records, err := readParquetFile[TradeRecord](path)
		if err != nil {
			continue
		}
		for _, r := range records {
			ts := time.UnixMilli(r.Timestamp)
			if (ts.Equal(start) || ts.After(start)) && (ts.Equal(end) || ts.Before(end)) {
				trades = append(trades, domain.Trade{
					Symbol:    r.Symbol,
					Timestamp: ts,
					Price:     r.Price,
					Size:      r.Size,
					Exchange:  r.Exchange,
					ID:        r.ID,
				})
			}
		}
	}
	return trades, nil
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

// barPath returns the filesystem path for a bar Parquet file.
// Layout: <dataDir>/<market>/daily/<SYMBOL>/<YYYY>.parquet
func (s *ParquetStore) barPath(symbol, market string, t time.Time) string {
	year := fmt.Sprintf("%d", t.Year())
	return filepath.Join(s.DataDir, market, "daily", strings.ToUpper(symbol), year+".parquet")
}

// tradePath returns the filesystem path for a trade Parquet file.
// Layout: <dataDir>/us/trades/<SYMBOL>/<YYYY-MM-DD>.parquet
func (s *ParquetStore) tradePath(symbol string, t time.Time) string {
	date := t.Format("2006-01-02")
	return filepath.Join(s.DataDir, "us", "trades", strings.ToUpper(symbol), date+".parquet")
}

// ---------------------------------------------------------------------------
// Parquet file helpers
// ---------------------------------------------------------------------------

func writeParquetFile[T any](path string, records []T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return parquet.WriteFile(path, records)
}

func readParquetFile[T any](path string) ([]T, error) {
	rows, err := parquet.ReadFile[T](path)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// mergeBarRecords deduplicates bar records by (symbol, timestamp), preferring
// new records over existing ones.
func mergeBarRecords(existing, incoming []BarRecord) []BarRecord {
	type key struct {
		symbol string
		ts     int64
	}
	seen := make(map[key]BarRecord, len(existing)+len(incoming))
	for _, r := range existing {
		seen[key{r.Symbol, r.Timestamp}] = r
	}
	for _, r := range incoming {
		seen[key{r.Symbol, r.Timestamp}] = r
	}

	merged := make([]BarRecord, 0, len(seen))
	for _, r := range seen {
		merged = append(merged, r)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Timestamp < merged[j].Timestamp
	})
	return merged
}

// mergeTradeRecords deduplicates trade records by (symbol, id), preferring
// new records over existing ones. Results are sorted by timestamp.
func mergeTradeRecords(existing, incoming []TradeRecord) []TradeRecord {
	type key struct {
		symbol string
		id     string
	}
	seen := make(map[key]TradeRecord, len(existing)+len(incoming))
	for _, r := range existing {
		seen[key{r.Symbol, r.ID}] = r
	}
	for _, r := range incoming {
		seen[key{r.Symbol, r.ID}] = r
	}

	merged := make([]TradeRecord, 0, len(seen))
	for _, r := range seen {
		merged = append(merged, r)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Timestamp < merged[j].Timestamp
	})
	return merged
}
