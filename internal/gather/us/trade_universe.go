package us

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

// GenerateTradeUniverse scans all universe dates and writes trade-universe CSVs
// for dates that have both universe and index files. Used by the standalone command.
// Returns (true, nil) if any CSVs were written.
func GenerateTradeUniverse(ctx context.Context, dataDir string, ref *ReferenceData, log *slog.Logger) (bool, error) {
	universeDir := filepath.Join(dataDir, "us", "universe")
	dates, err := ListUniverseDates(universeDir)
	if err != nil {
		return false, fmt.Errorf("listing universe dates: %w", err)
	}

	wrote := 0
	for _, date := range dates {
		if ctx.Err() != nil {
			return wrote > 0, ctx.Err()
		}

		outPath := tradeUniversePath(dataDir, date)
		if _, err := os.Stat(outPath); err == nil {
			continue
		}

		// Require both SPX and NDX index files for the date.
		spxPath := filepath.Join(dataDir, "us", "index", "spx", date+".txt")
		ndxPath := filepath.Join(dataDir, "us", "index", "ndx", date+".txt")
		if _, err := os.Stat(spxPath); os.IsNotExist(err) {
			continue
		}
		if _, err := os.Stat(ndxPath); os.IsNotExist(err) {
			continue
		}

		symbols, err := ReadUniverseFile(filepath.Join(universeDir, date+".txt"))
		if err != nil {
			log.Error("reading universe file", "date", date, "error", err)
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

// barTurnoverRecord is a minimal parquet schema for reading bar files when
// computing turnover (VWAP × Volume). Only the fields needed are declared.
type barTurnoverRecord struct {
	Timestamp int64   `parquet:"timestamp,timestamp(millisecond)"`
	Volume    int64   `parquet:"volume"`
	VWAP      float64 `parquet:"vwap"`
}

// computeTiers computes activity tiers for non-index stocks based on trailing
// daily bar data (turnover = VWAP × Volume). Returns a map of symbol→tier
// (ACTIVE/MODERATE/SPORADIC). Returns nil if no trailing data is available.
func computeTiers(dataDir, date string, log *slog.Logger) (map[string]string, error) {
	universeDir := filepath.Join(dataDir, "us", "universe")
	allDates, err := ListUniverseDates(universeDir)
	if err != nil {
		return nil, err
	}

	// ListUniverseDates returns descending order. Take up to 60 dates before target.
	var trailing []string
	for _, d := range allDates {
		if d < date {
			trailing = append(trailing, d)
		}
	}
	const maxTrailing = 60
	if len(trailing) > maxTrailing {
		trailing = trailing[:maxTrailing]
	}
	if len(trailing) == 0 {
		return nil, nil
	}

	// Build trailing date index (date string → position in trailing array).
	trailingIdx := make(map[string]int, len(trailing))
	for i, d := range trailing {
		trailingIdx[d] = i
	}

	// Determine year range for bar file reads.
	latestYear, _ := strconv.Atoi(trailing[0][:4])
	earliestYear, _ := strconv.Atoi(trailing[len(trailing)-1][:4])

	// Load SPX/NDX index sets for the target date to exclude index members.
	spxDir := filepath.Join(dataDir, "us", "index", "spx")
	ndxDir := filepath.Join(dataDir, "us", "index", "ndx")
	spxMembers := readIndexSet(filepath.Join(spxDir, date+".txt"))
	ndxMembers := readIndexSet(filepath.Join(ndxDir, date+".txt"))

	// Read target date's universe to get the symbol list.
	symbols, err := ReadUniverseFile(filepath.Join(universeDir, date+".txt"))
	if err != nil {
		return nil, fmt.Errorf("reading universe for %s: %w", date, err)
	}

	// Filter to non-index symbols.
	var exIndexSymbols []string
	for _, sym := range symbols {
		if !spxMembers[sym] && !ndxMembers[sym] {
			exIndexSymbols = append(exIndexSymbols, sym)
		}
	}

	if len(exIndexSymbols) == 0 {
		return nil, nil
	}

	// Read bar data for each symbol in parallel using 16 workers.
	dailyDir := filepath.Join(dataDir, "us", "daily")
	nDates := len(trailing)

	type symbolResult struct {
		symbol   string
		turnover map[int]float64
	}

	symCh := make(chan string, len(exIndexSymbols))
	for _, sym := range exIndexSymbols {
		symCh <- sym
	}
	close(symCh)

	resultCh := make(chan symbolResult, len(exIndexSymbols))

	var wg sync.WaitGroup
	workers := min(16, len(exIndexSymbols))
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sym := range symCh {
				turnover := make(map[int]float64)
				for year := earliestYear; year <= latestYear; year++ {
					path := filepath.Join(dailyDir, sym, fmt.Sprintf("%d.parquet", year))
					records, err := parquet.ReadFile[barTurnoverRecord](path)
					if err != nil {
						continue
					}
					for _, r := range records {
						dateStr := time.UnixMilli(r.Timestamp).UTC().Format("2006-01-02")
						if idx, ok := trailingIdx[dateStr]; ok {
							turnover[idx] += r.VWAP * float64(r.Volume)
						}
					}
				}
				if len(turnover) > 0 {
					resultCh <- symbolResult{symbol: sym, turnover: turnover}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results and compute medians.
	var allMedians []float64
	medians := make(map[string]float64)
	for res := range resultCh {
		vals := make([]float64, nDates)
		for idx, t := range res.turnover {
			vals[idx] = t
		}
		sort.Float64s(vals)
		med := medianSorted(vals)
		medians[res.symbol] = med
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

// latestDateFile returns the path to the latest date-stamped .txt file in dir
// (e.g. "2026-02-12.txt"). Returns "" if none found.
func latestDateFile(dir string) string {
	matches, _ := filepath.Glob(filepath.Join(dir, "*.txt"))
	if len(matches) == 0 {
		return ""
	}
	sort.Strings(matches)
	return matches[len(matches)-1]
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
