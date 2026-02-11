package us

import (
	"encoding/csv"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// ReferenceData holds ETF and stock classification loaded from reference CSVs.
type ReferenceData struct {
	ETFs   map[string]bool // symbols from us_etf.csv
	Stocks map[string]bool // symbols from us_stock.csv
}

// LoadReferenceData reads us_etf.csv and us_stock.csv from refDir.
// If a file is missing, its set is empty (with a warning).
func LoadReferenceData(refDir string) *ReferenceData {
	ref := &ReferenceData{
		ETFs:   loadSymbolSet(filepath.Join(refDir, "us_etf.csv"), "ETF"),
		Stocks: loadSymbolSet(filepath.Join(refDir, "us_stock.csv"), "stock"),
	}
	slog.Info("loaded reference data", "etfs", len(ref.ETFs), "stocks", len(ref.Stocks))
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
