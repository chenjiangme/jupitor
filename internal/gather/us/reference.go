package us

import (
	"encoding/csv"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ReferenceData holds ETF and stock classification loaded from reference CSVs.
type ReferenceData struct {
	ETFs   map[string]bool // symbols from us_etf_*.csv
	Stocks map[string]bool // symbols from us_stock_*.csv
}

// LoadReferenceData finds the latest date-stamped us_etf_*.csv and
// us_stock_*.csv in refDir. Falls back to us_etf.csv / us_stock.csv
// if no dated files exist.
func LoadReferenceData(refDir string) *ReferenceData {
	etfPath := findLatestRefFile(refDir, "us_etf")
	stockPath := findLatestRefFile(refDir, "us_stock")

	ref := &ReferenceData{
		ETFs:   loadSymbolSet(etfPath, "ETF"),
		Stocks: loadSymbolSet(stockPath, "stock"),
	}
	slog.Info("loaded reference data", "etfs", len(ref.ETFs), "stocks", len(ref.Stocks),
		"etf_file", filepath.Base(etfPath), "stock_file", filepath.Base(stockPath))
	return ref
}

// SymbolType returns "ETF", "STOCK", or "OTHER".
func (r *ReferenceData) SymbolType(symbol string) string {
	sym := strings.ToUpper(symbol)
	if r.ETFs[sym] {
		return "ETF"
	}
	if r.Stocks[sym] {
		return "STOCK"
	}
	return "OTHER"
}

// findLatestRefFile finds the latest date-stamped file matching
// prefix_YYYY-MM-DD.csv in dir. Falls back to prefix.csv if none found.
func findLatestRefFile(dir, prefix string) string {
	pattern := filepath.Join(dir, prefix+"_????-??-??.csv")
	matches, err := filepath.Glob(pattern)
	if err == nil && len(matches) > 0 {
		sort.Strings(matches)
		return matches[len(matches)-1]
	}
	// Fallback to undated file
	return filepath.Join(dir, prefix+".csv")
}

// loadSymbolSet reads the first column of a CSV file and returns a set of
// uppercase symbols. Returns an empty set if the file is missing.
func loadSymbolSet(path string, label string) map[string]bool {
	set := make(map[string]bool)

	f, err := os.Open(path)
	if err != nil {
		slog.Warn("reference file not found", "label", label, "path", path)
		return set
	}
	defer f.Close()

	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil {
		slog.Warn("failed to read CSV header", "label", label, "path", path, "error", err)
		return set
	}

	// Find symbol column (first column or column named "symbol")
	symbolIdx := 0
	for i, col := range header {
		if strings.EqualFold(strings.TrimSpace(col), "symbol") {
			symbolIdx = i
			break
		}
	}

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		if len(record) > symbolIdx {
			sym := strings.ToUpper(strings.TrimSpace(record[symbolIdx]))
			if sym != "" {
				set[sym] = true
			}
		}
	}

	return set
}
