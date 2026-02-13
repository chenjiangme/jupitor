package dashboard

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/parquet-go/parquet-go"

	"jupitor/internal/store"
)

// ListHistoryDates returns sorted dates (YYYY-MM-DD) that have both a
// stock-trades-ex-index parquet file and a trade-universe CSV.
func ListHistoryDates(dataDir string) ([]string, error) {
	tradeDir := filepath.Join(dataDir, "us", "stock-trades-ex-index")
	universeDir := filepath.Join(dataDir, "us", "trade-universe")

	tradeEntries, err := os.ReadDir(tradeDir)
	if err != nil {
		return nil, fmt.Errorf("reading stock-trades-ex-index dir: %w", err)
	}

	// Build set of trade-universe dates.
	uniDates := make(map[string]bool)
	uniEntries, err := os.ReadDir(universeDir)
	if err != nil {
		return nil, fmt.Errorf("reading trade-universe dir: %w", err)
	}
	for _, e := range uniEntries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".csv") {
			uniDates[strings.TrimSuffix(e.Name(), ".csv")] = true
		}
	}

	var dates []string
	for _, e := range tradeEntries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".parquet") {
			continue
		}
		date := strings.TrimSuffix(e.Name(), ".parquet")
		if uniDates[date] {
			dates = append(dates, date)
		}
	}
	sort.Strings(dates)
	return dates, nil
}

// LoadHistoryTrades reads all ex-index trades for a given date from the
// consolidated parquet file at $DATA_1/us/stock-trades-ex-index/<date>.parquet.
func LoadHistoryTrades(dataDir, date string) ([]store.TradeRecord, error) {
	path := filepath.Join(dataDir, "us", "stock-trades-ex-index", date+".parquet")
	records, err := parquet.ReadFile[store.TradeRecord](path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	return records, nil
}

// LoadTierMapForDate reads the trade-universe CSV for a specific date.
func LoadTierMapForDate(dataDir, date string) (map[string]string, error) {
	path := filepath.Join(dataDir, "us", "trade-universe", date+".csv")
	return loadTierMapFromFile(path)
}

// loadTierMapFromFile reads a trade-universe CSV file and returns symbol→tier.
func loadTierMapFromFile(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	tierMap := make(map[string]string)
	for i, line := range strings.Split(string(data), "\n") {
		if i == 0 || line == "" {
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) < 5 {
			continue
		}
		tier := strings.TrimSpace(fields[4])
		if tier != "" {
			tierMap[fields[0]] = tier
		}
	}
	return tierMap, nil
}
