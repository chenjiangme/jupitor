package us

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// GenerateTradeUniverse scans all universe dates and writes trade-universe CSVs
// for dates where all symbols have trade files. Used by the standalone command.
// Returns (true, nil) if any CSVs were written.
func GenerateTradeUniverse(ctx context.Context, dataDir string, ref *ReferenceData, log *slog.Logger) (bool, error) {
	universeDir := filepath.Join(dataDir, "us", "universe")
	dates, err := ListUniverseDates(universeDir)
	if err != nil {
		return false, fmt.Errorf("listing universe dates: %w", err)
	}

	tradesDir := filepath.Join(dataDir, "us", "trades")

	wrote := 0
	for _, date := range dates {
		if ctx.Err() != nil {
			return wrote > 0, ctx.Err()
		}

		outPath := tradeUniversePath(dataDir, date)
		if _, err := os.Stat(outPath); err == nil {
			continue
		}

		symbols, err := ReadUniverseFile(filepath.Join(universeDir, date+".txt"))
		if err != nil {
			log.Error("reading universe file", "date", date, "error", err)
			continue
		}

		allComplete := true
		for _, sym := range symbols {
			tradePath := filepath.Join(tradesDir, strings.ToUpper(sym), date+".parquet")
			if _, err := os.Stat(tradePath); os.IsNotExist(err) {
				allComplete = false
				break
			}
		}

		if !allComplete {
			continue
		}

		if err := generateTradeUniverseForDate(dataDir, date, symbols, ref, log); err != nil {
			continue
		}
		wrote++
	}

	return wrote > 0, nil
}

// generateTradeUniverseForDate writes a single trade-universe CSV for the given
// date. Called from the daemon after a trade day completes, and from the
// standalone batch command.
func generateTradeUniverseForDate(dataDir, date string, symbols []string, ref *ReferenceData, log *slog.Logger) error {
	outPath := tradeUniversePath(dataDir, date)
	spxDir := filepath.Join(dataDir, "us", "index", "spx")
	ndxDir := filepath.Join(dataDir, "us", "index", "ndx")

	// Compute tiers from trailing ex-index trade data.
	tiers, err := computeTiers(dataDir, date, log)
	if err != nil {
		log.Warn("computing tiers, continuing without", "date", date, "error", err)
	}

	if err := writeTradeUniverseCSV(outPath, symbols, ref, spxDir, ndxDir, date, tiers); err != nil {
		log.Error("writing trade universe CSV", "date", date, "error", err)
		return err
	}

	log.Info("trade universe CSV written", "date", date, "symbols", len(symbols))
	return nil
}

// tradeUniversePath returns the path for a trade-universe CSV.
func tradeUniversePath(dataDir, date string) string {
	return filepath.Join(dataDir, "us", "trade-universe", date+".csv")
}

// writeTradeUniverseCSV generates a trade-universe CSV for a single date.
// tiers maps non-index stock symbols to their activity tier; may be nil.
func writeTradeUniverseCSV(path string, symbols []string, ref *ReferenceData, spxDir, ndxDir, date string, tiers map[string]string) error {
	// Load SPX/NDX members for this date.
	spxMembers := readIndexSet(filepath.Join(spxDir, date+".txt"))
	ndxMembers := readIndexSet(filepath.Join(ndxDir, date+".txt"))

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating trade-universe dir: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating CSV %s: %w", path, err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	w.WriteString("symbol,type,spx,ndx,tier\n")

	sorted := make([]string, len(symbols))
	copy(sorted, symbols)
	sort.Strings(sorted)

	for _, sym := range sorted {
		symType := "STOCK"
		if ref != nil {
			symType = ref.SymbolType(sym)
		}
		inSPX := "false"
		if spxMembers[sym] {
			inSPX = "true"
		}
		inNDX := "false"
		if ndxMembers[sym] {
			inNDX = "true"
		}

		tier := ""
		if tiers != nil && symType != "ETF" && !spxMembers[sym] && !ndxMembers[sym] {
			if t, ok := tiers[sym]; ok {
				tier = t
			} else {
				tier = "SPORADIC"
			}
		}

		fmt.Fprintf(w, "%s,%s,%s,%s,%s\n", sym, symType, inSPX, inNDX, tier)
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("flushing CSV: %w", err)
	}
	return nil
}

// computeTiers computes activity tiers for non-index stocks based on trailing
// daily summary data. Returns a map of symbol→tier (ACTIVE/MODERATE/SPORADIC).
// Returns nil if no trailing data is available.
func computeTiers(dataDir, date string, log *slog.Logger) (map[string]string, error) {
	dailyDir := filepath.Join(dataDir, "us", "stock-trades-daily")
	allDates, err := listDailyDates(dailyDir)
	if err != nil {
		return nil, err
	}

	// Filter to dates strictly before the target date, take last N.
	var trailing []string
	for _, d := range allDates {
		if d < date {
			trailing = append(trailing, d)
		}
	}
	const maxTrailing = 60
	if len(trailing) > maxTrailing {
		trailing = trailing[len(trailing)-maxTrailing:]
	}
	if len(trailing) == 0 {
		return nil, nil
	}

	// Load SPX/NDX index sets for the target date to exclude index members.
	spxDir := filepath.Join(dataDir, "us", "index", "spx")
	ndxDir := filepath.Join(dataDir, "us", "index", "ndx")
	spxMembers := readIndexSet(filepath.Join(spxDir, date+".txt"))
	ndxMembers := readIndexSet(filepath.Join(ndxDir, date+".txt"))

	// Per-symbol per-date turnover from daily summaries.
	symbolTurnover := make(map[string]map[int]float64)

	for idx, d := range trailing {
		path := filepath.Join(dailyDir, d+".parquet")
		records, err := parquet.ReadFile[DailyRecord](path)
		if err != nil {
			log.Warn("reading daily summary for tier computation", "date", d, "error", err)
			continue
		}
		for i := range records {
			r := &records[i]
			// Skip index members.
			if spxMembers[r.Symbol] || ndxMembers[r.Symbol] {
				continue
			}
			m := symbolTurnover[r.Symbol]
			if m == nil {
				m = make(map[int]float64)
				symbolTurnover[r.Symbol] = m
			}
			m[idx] += r.Turnover
		}
	}

	nDates := len(trailing)

	// For each symbol, compute median daily turnover (0 for missing dates).
	var allMedians []float64
	medians := make(map[string]float64, len(symbolTurnover))
	for sym, dayMap := range symbolTurnover {
		vals := make([]float64, nDates)
		for idx, t := range dayMap {
			vals[idx] = t
		}
		sort.Float64s(vals)
		med := medianSorted(vals)
		medians[sym] = med
		allMedians = append(allMedians, med)
	}

	if len(allMedians) == 0 {
		return nil, nil
	}

	sort.Float64s(allMedians)
	p25 := percentileSorted(allMedians, 25)
	p75 := percentileSorted(allMedians, 75)

	tiers := make(map[string]string, len(medians))
	for sym, med := range medians {
		switch {
		case med >= p75:
			tiers[sym] = "ACTIVE"
		case med >= p25:
			tiers[sym] = "MODERATE"
		default:
			tiers[sym] = "SPORADIC"
		}
	}

	log.Info("computed tiers",
		"trailing_dates", nDates,
		"symbols", len(tiers),
		"p25", fmt.Sprintf("%.0f", p25),
		"p75", fmt.Sprintf("%.0f", p75),
	)
	return tiers, nil
}

// listDailyDates returns all dates that have daily summary parquet files,
// sorted in ascending order (earliest first).
func listDailyDates(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		return nil, fmt.Errorf("globbing daily summary files: %w", err)
	}

	var dates []string
	for _, m := range matches {
		base := filepath.Base(m)
		date := strings.TrimSuffix(base, ".parquet")
		if len(date) == 10 && date[4] == '-' && date[7] == '-' {
			dates = append(dates, date)
		}
	}
	sort.Strings(dates)
	return dates, nil
}

// medianSorted returns the median of an already-sorted slice.
func medianSorted(sorted []float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

// percentileSorted returns the p-th percentile (0-100) of an already-sorted slice
// using linear interpolation.
func percentileSorted(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	rank := p / 100 * float64(n-1)
	lower := int(rank)
	frac := rank - float64(lower)
	if lower >= n-1 {
		return sorted[n-1]
	}
	return sorted[lower] + frac*(sorted[lower+1]-sorted[lower])
}

// readIndexSet reads an index constituent file and returns a set of symbols.
// Returns an empty map if the file doesn't exist.
func readIndexSet(path string) map[string]bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return make(map[string]bool)
	}

	set := make(map[string]bool)
	for _, line := range strings.Split(string(data), "\n") {
		sym := strings.TrimSpace(line)
		if sym != "" {
			set[sym] = true
		}
	}
	return set
}
