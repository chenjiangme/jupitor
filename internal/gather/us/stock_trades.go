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
	"time"

	"github.com/parquet-go/parquet-go"

	"jupitor/internal/store"
)

// DailyRecord is the Parquet schema for per-symbol daily trade aggregates.
// Combines index + ex-index stock trades into a single lightweight summary.
type DailyRecord struct {
	Symbol   string  `parquet:"symbol"`
	Trades   int64   `parquet:"trades"`
	Turnover float64 `parquet:"turnover"` // sum(price * size)
	Vwap     float64 `parquet:"vwap"`     // turnover / volume
	Open     float64 `parquet:"open"`     // first trade price
	Close    float64 `parquet:"close"`    // last trade price
	Low      float64 `parquet:"low"`
	High     float64 `parquet:"high"`
}

// allowedConds defines the set of trade condition codes that pass the filter.
var allowedConds = map[string]bool{" ": true, "@": true, "T": true, "F": true}

// GenerateStockTrades scans consecutive trade-universe date pairs (P, D)
// and builds filtered stock-trades parquet files. Skips if output exists.
// When maxDates > 0, only the latest maxDates pairs are considered.
// Returns the number of files written.
func GenerateStockTrades(ctx context.Context, dataDir string, maxDates int, skipIndex bool, log *slog.Logger) (int, error) {
	tuDir := filepath.Join(dataDir, "us", "trade-universe")
	dates, err := listTradeUniverseDates(tuDir)
	if err != nil {
		return 0, fmt.Errorf("listing trade-universe dates: %w", err)
	}

	if len(dates) < 2 {
		return 0, nil
	}

	// Trim to latest maxDates+1 entries (need P for each D).
	if maxDates > 0 && len(dates) > maxDates+1 {
		dates = dates[len(dates)-maxDates-1:]
	}

	wrote := 0
	for i := 1; i < len(dates); i++ {
		if ctx.Err() != nil {
			return wrote, ctx.Err()
		}

		prevDate := dates[i-1]
		date := dates[i]

		idxPath := filepath.Join(dataDir, "us", "stock-trades-index", date+".parquet")
		exPath := filepath.Join(dataDir, "us", "stock-trades-ex-index", date+".parquet")
		idxExists := skipIndex || fileExists(idxPath)
		exExists := fileExists(exPath)
		if idxExists && exExists {
			continue
		}

		if err := processStockTradesForDate(dataDir, prevDate, date, idxExists, exExists, log); err != nil {
			log.Error("processing stock trades", "date", date, "error", err)
			continue
		}
		wrote++
	}

	return wrote, nil
}

// processStockTradesForDate reads STOCK symbols from D's trade-universe CSV,
// reads trades from both P and D per-symbol files, filters by timestamp
// window (P 4PM ET, D 4PM ET] + exchange/condition filters, writes output.
// skipIdx/skipEx indicate which output files already exist and can be skipped.
func processStockTradesForDate(dataDir string, prevDate, date string, skipIdx, skipEx bool, log *slog.Logger) error {
	csvPath := filepath.Join(dataDir, "us", "trade-universe", date+".csv")
	symbols, indexSyms, _, err := readStockSymbols(csvPath)
	if err != nil {
		return fmt.Errorf("reading stock symbols for %s: %w", date, err)
	}

	prevClose, err := regularClose(prevDate)
	if err != nil {
		return fmt.Errorf("computing P close for %s: %w", prevDate, err)
	}
	dateClose, err := regularClose(date)
	if err != nil {
		return fmt.Errorf("computing D close for %s: %w", date, err)
	}

	tradesDir := filepath.Join(dataDir, "us", "trades")
	var indexTrades []store.TradeRecord
	var exIndexTrades []store.TradeRecord

	for _, sym := range symbols {
		symDir := filepath.Join(tradesDir, strings.ToUpper(sym))
		isIndex := indexSyms[sym]

		var symTrades []store.TradeRecord

		// Read P's trade file: filter timestamp > prevClose
		pPath := filepath.Join(symDir, prevDate+".parquet")
		if records, err := parquet.ReadFile[store.TradeRecord](pPath); err == nil {
			for _, r := range records {
				if r.Timestamp > prevClose && filterTradeRecord(r) {
					symTrades = append(symTrades, r)
				}
			}
		}

		// Read D's trade file: filter timestamp <= dateClose
		dPath := filepath.Join(symDir, date+".parquet")
		if records, err := parquet.ReadFile[store.TradeRecord](dPath); err == nil {
			for _, r := range records {
				if r.Timestamp <= dateClose && filterTradeRecord(r) {
					symTrades = append(symTrades, r)
				}
			}
		}

		if isIndex {
			indexTrades = append(indexTrades, symTrades...)
		} else {
			exIndexTrades = append(exIndexTrades, symTrades...)
		}
	}

	sortByTS := func(trades []store.TradeRecord) {
		sort.Slice(trades, func(i, j int) bool {
			return trades[i].Timestamp < trades[j].Timestamp
		})
	}

	if !skipIdx {
		sortByTS(indexTrades)
		idxPath := filepath.Join(dataDir, "us", "stock-trades-index", date+".parquet")
		if err := os.MkdirAll(filepath.Dir(idxPath), 0o755); err != nil {
			return fmt.Errorf("creating stock-trades-index dir: %w", err)
		}
		if err := parquet.WriteFile(idxPath, indexTrades); err != nil {
			return fmt.Errorf("writing index stock trades for %s: %w", date, err)
		}
		log.Info("stock trades index written",
			"date", date,
			"stocks", len(indexSyms),
			"filtered", len(indexTrades),
		)
	}

	if !skipEx {
		sortByTS(exIndexTrades)
		exPath := filepath.Join(dataDir, "us", "stock-trades-ex-index", date+".parquet")
		if err := os.MkdirAll(filepath.Dir(exPath), 0o755); err != nil {
			return fmt.Errorf("creating stock-trades-ex-index dir: %w", err)
		}
		if err := parquet.WriteFile(exPath, exIndexTrades); err != nil {
			return fmt.Errorf("writing ex-index stock trades for %s: %w", date, err)
		}
		log.Info("stock trades ex-index written",
			"date", date,
			"stocks", len(symbols)-len(indexSyms),
			"filtered", len(exIndexTrades),
		)
	}

	return nil
}

// aggregateDailyRecords groups trades by symbol and computes per-symbol daily
// aggregates. Output is sorted by symbol.
func aggregateDailyRecords(trades []store.TradeRecord) []DailyRecord {
	type accum struct {
		trades   int64
		turnover float64
		volume   int64
		open     float64
		close    float64
		low      float64
		high     float64
		firstTS  int64
		lastTS   int64
	}

	bySymbol := make(map[string]*accum)
	for i := range trades {
		r := &trades[i]
		a := bySymbol[r.Symbol]
		if a == nil {
			a = &accum{
				low:     r.Price,
				high:    r.Price,
				open:    r.Price,
				close:   r.Price,
				firstTS: r.Timestamp,
				lastTS:  r.Timestamp,
			}
			bySymbol[r.Symbol] = a
		}
		a.trades++
		a.turnover += r.Price * float64(r.Size)
		a.volume += r.Size
		if r.Price < a.low {
			a.low = r.Price
		}
		if r.Price > a.high {
			a.high = r.Price
		}
		if r.Timestamp < a.firstTS {
			a.firstTS = r.Timestamp
			a.open = r.Price
		}
		if r.Timestamp > a.lastTS {
			a.lastTS = r.Timestamp
			a.close = r.Price
		}
	}

	result := make([]DailyRecord, 0, len(bySymbol))
	for sym, a := range bySymbol {
		vwap := 0.0
		if a.volume > 0 {
			vwap = a.turnover / float64(a.volume)
		}
		result = append(result, DailyRecord{
			Symbol:   sym,
			Trades:   a.trades,
			Turnover: a.turnover,
			Vwap:     vwap,
			Open:     a.open,
			Close:    a.close,
			Low:      a.low,
			High:     a.high,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Symbol < result[j].Symbol
	})
	return result
}

// GenerateDailySummaries backfills daily summary parquets for dates that have
// index + ex-index files but no daily summary yet. Returns the number written.
func GenerateDailySummaries(ctx context.Context, dataDir string, maxDates int, log *slog.Logger) (int, error) {
	exDir := filepath.Join(dataDir, "us", "stock-trades-ex-index")
	dates, err := listExIndexDates(exDir)
	if err != nil {
		return 0, fmt.Errorf("listing ex-index dates: %w", err)
	}

	if maxDates > 0 && len(dates) > maxDates {
		dates = dates[len(dates)-maxDates:]
	}

	dailyDir := filepath.Join(dataDir, "us", "stock-trades-daily")
	wrote := 0
	for _, date := range dates {
		if ctx.Err() != nil {
			return wrote, ctx.Err()
		}

		outPath := filepath.Join(dailyDir, date+".parquet")
		if fileExists(outPath) {
			continue
		}

		// Read both index and ex-index trades.
		idxPath := filepath.Join(dataDir, "us", "stock-trades-index", date+".parquet")
		exPath := filepath.Join(exDir, date+".parquet")

		var allTrades []store.TradeRecord
		if records, err := parquet.ReadFile[store.TradeRecord](idxPath); err == nil {
			allTrades = append(allTrades, records...)
		}
		if records, err := parquet.ReadFile[store.TradeRecord](exPath); err == nil {
			allTrades = append(allTrades, records...)
		}

		if len(allTrades) == 0 {
			log.Warn("no trades for daily summary", "date", date)
			continue
		}

		daily := aggregateDailyRecords(allTrades)
		if err := os.MkdirAll(dailyDir, 0o755); err != nil {
			return wrote, fmt.Errorf("creating stock-trades-daily dir: %w", err)
		}
		if err := parquet.WriteFile(outPath, daily); err != nil {
			log.Error("writing daily summary", "date", date, "error", err)
			continue
		}
		log.Info("daily summary backfilled", "date", date, "symbols", len(daily))
		wrote++
	}

	return wrote, nil
}

// readStockSymbols parses a trade-universe CSV and returns all STOCK and OTHER
// symbols (excludes ETFs), a set of symbols that are in SPX or NDX on that date,
// and a map of symbol→tier for non-index stocks.
func readStockSymbols(csvPath string) (symbols []string, indexSyms map[string]bool, tiers map[string]string, err error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, nil, nil, err
	}
	defer f.Close()

	indexSyms = make(map[string]bool)
	tiers = make(map[string]string)
	scanner := bufio.NewScanner(f)
	first := true
	for scanner.Scan() {
		if first {
			first = false
			continue
		}
		// format: symbol,type,spx,ndx[,tier]
		line := scanner.Text()
		parts := strings.Split(line, ",")
		if len(parts) < 4 {
			continue
		}
		if parts[1] == "ETF" {
			continue
		}
		sym := parts[0]
		symbols = append(symbols, sym)
		if parts[2] == "true" || parts[3] == "true" {
			indexSyms[sym] = true
		}
		if len(parts) >= 5 && parts[4] != "" {
			tiers[sym] = parts[4]
		}
	}
	return symbols, indexSyms, tiers, scanner.Err()
}

// filterTradeRecord returns true if exchange != "D" and all conditions are allowed.
func filterTradeRecord(r store.TradeRecord) bool {
	if r.Exchange == "D" {
		return false
	}
	if r.Conditions == "" {
		return true
	}
	for _, c := range strings.Split(r.Conditions, ",") {
		if !allowedConds[c] {
			return false
		}
	}
	return true
}

// regularClose returns 4:00 PM ET on the given date as ET-shifted milliseconds
// (the ET clock reading encoded as-if-UTC).
func regularClose(dateStr string) (int64, error) {
	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return 0, err
	}
	close4pm := time.Date(t.Year(), t.Month(), t.Day(), 16, 0, 0, 0, time.UTC)
	return close4pm.UnixMilli(), nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// listTradeUniverseDates returns all dates that have trade-universe CSV files,
// sorted in ascending order (earliest first).
func listTradeUniverseDates(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.csv"))
	if err != nil {
		return nil, fmt.Errorf("globbing trade-universe files: %w", err)
	}

	var dates []string
	for _, m := range matches {
		base := filepath.Base(m)
		date := strings.TrimSuffix(base, ".csv")
		if len(date) == 10 && date[4] == '-' && date[7] == '-' {
			dates = append(dates, date)
		}
	}
	sort.Strings(dates)
	return dates, nil
}

// ---------------------------------------------------------------------------
// Rolling bars from ex-index trades: backward 5m window + forward gain
// ---------------------------------------------------------------------------

// RollingBarRecord is the Parquet schema for rolling bars with backward
// 5-minute window aggregates and a forward-looking gain metric.
type RollingBarRecord struct {
	Symbol        string  `parquet:"symbol"`
	Timestamp     int64   `parquet:"timestamp,timestamp(millisecond)"`
	Tier          string  `parquet:"tier"`              // ACTIVE, MODERATE, SPORADIC (empty for index)
	Vwap          float64 `parquet:"vwap"`              // per-bin VWAP
	Trades        int64   `parquet:"trades"`            // per-bin trade count
	Turnover      float64 `parquet:"turnover"`          // per-bin turnover
	GainPct5m     float64 `parquet:"gain_pct_5m"`       // backward 5m: (vwap - minVwap) / minVwap * 100
	Trades5m      int64   `parquet:"trades_5m"`         // backward 5m: sum of trades
	Turnover5m    float64 `parquet:"turnover_5m"`       // backward 5m: sum of turnover
	GainPctFuture float64 `parquet:"gain_pct_future"`   // forward: (maxFutureVwap - vwap) / vwap * 100
}

// binStats holds aggregated stats for a single 5-second bin.
type binStats struct {
	trades   int64
	turnover float64 // sum(price * size)
	volume   int64   // sum(size), for VWAP
	vwap     float64 // turnover / volume, computed after binning
}

// GenerateRollingBars scans ex-index parquet files and generates rolling
// 5-minute forward-looking bar files. Skips dates with existing output.
// When maxDates > 0, only the latest maxDates files are considered.
// Returns the number of files written.
func GenerateRollingBars(ctx context.Context, dataDir string, maxDates int, log *slog.Logger) (int, error) {
	exDir := filepath.Join(dataDir, "us", "stock-trades-ex-index")
	dates, err := listExIndexDates(exDir)
	if err != nil {
		return 0, fmt.Errorf("listing ex-index dates: %w", err)
	}

	if maxDates > 0 && len(dates) > maxDates {
		dates = dates[len(dates)-maxDates:]
	}

	outDir := filepath.Join(dataDir, "us", "stock-trades-ex-index-rolling")
	wrote := 0
	for _, date := range dates {
		if ctx.Err() != nil {
			return wrote, ctx.Err()
		}

		outPath := filepath.Join(outDir, date+".parquet")
		if fileExists(outPath) {
			continue
		}

		if err := processRollingBarsForDate(dataDir, date, log); err != nil {
			log.Error("processing rolling bars", "date", date, "error", err)
			continue
		}
		wrote++
	}

	return wrote, nil
}

// processRollingBarsForDate reads an ex-index parquet file, bins trades into
// 5-second intervals per symbol, computes VWAP per bin, then builds:
//   - Backward 5m window: gain_pct_5m, trades_5m, turnover_5m over past 60 bins
//   - Forward gain: gain_pct_future = (max future vwap - current vwap) / current vwap * 100
func processRollingBarsForDate(dataDir, date string, log *slog.Logger) error {
	// Read tiers from trade-universe CSV for this date.
	csvPath := tradeUniversePath(dataDir, date)
	_, _, tiers, tierErr := readStockSymbols(csvPath)
	if tierErr != nil {
		log.Warn("reading tiers for rolling bars", "date", date, "error", tierErr)
		tiers = nil
	}

	inPath := filepath.Join(dataDir, "us", "stock-trades-ex-index", date+".parquet")
	records, err := parquet.ReadFile[store.TradeRecord](inPath)
	if err != nil {
		return fmt.Errorf("reading ex-index trades for %s: %w", date, err)
	}

	const binSize int64 = 5_000 // 5 seconds in milliseconds

	// Single-pass binning: group trades into per-symbol 5-second bins.
	type symBin struct {
		sym string
		ts  int64
	}
	bins := make(map[symBin]*binStats)

	for i := range records {
		r := &records[i]
		alignedTS := (r.Timestamp / binSize) * binSize
		k := symBin{r.Symbol, alignedTS}
		b := bins[k]
		if b == nil {
			b = &binStats{}
			bins[k] = b
		}
		b.trades++
		b.turnover += r.Price * float64(r.Size)
		b.volume += r.Size
	}

	// Free raw records.
	records = nil

	// Compute VWAP per bin and group by symbol.
	type tsBin struct {
		ts    int64
		stats *binStats
	}
	symbolBins := make(map[string][]tsBin)
	for k, b := range bins {
		b.vwap = b.turnover / float64(b.volume)
		symbolBins[k.sym] = append(symbolBins[k.sym], tsBin{k.ts, b})
	}
	bins = nil

	// Compute rolling bars.
	const windowSize = 60             // 60 bins × 5s = 5 minutes
	const gapThreshold = 60 * binSize // max gap between consecutive active bins

	// Session boundaries (ET-as-UTC ms). Gaps between sessions are bridged;
	// the 5-min gap threshold only applies within the same session.
	dateT, _ := time.Parse("2006-01-02", date)
	prev := dateT.AddDate(0, 0, -1)
	postEnd := time.Date(prev.Year(), prev.Month(), prev.Day(), 20, 0, 0, 0, time.UTC).UnixMilli()
	preStart := time.Date(dateT.Year(), dateT.Month(), dateT.Day(), 4, 0, 0, 0, time.UTC).UnixMilli()
	regStart := time.Date(dateT.Year(), dateT.Month(), dateT.Day(), 9, 30, 0, 0, time.UTC).UnixMilli()
	sessionOf := func(ts int64) int {
		if ts < postEnd {
			return 0 // post-market
		}
		if ts < preStart {
			return 1 // overnight
		}
		if ts < regStart {
			return 2 // pre-market
		}
		return 3 // regular
	}

	var result []RollingBarRecord
	for sym, sbs := range symbolBins {
		// Sort bins by timestamp.
		sort.Slice(sbs, func(i, j int) bool { return sbs[i].ts < sbs[j].ts })

		n := len(sbs)

		// Build prefix sums for trades and turnover.
		prefixTrades := make([]int64, n+1)
		prefixTurnover := make([]float64, n+1)
		for i, b := range sbs {
			prefixTrades[i+1] = prefixTrades[i] + b.stats.trades
			prefixTurnover[i+1] = prefixTurnover[i] + b.stats.turnover
		}

		// Suffix-max of VWAP for forward gain computation.
		suffixMaxVwap := make([]float64, n)
		suffixMaxVwap[n-1] = sbs[n-1].stats.vwap
		for j := n - 2; j >= 0; j-- {
			suffixMaxVwap[j] = max(sbs[j].stats.vwap, suffixMaxVwap[j+1])
		}

		for i := 0; i < n; i++ {
			// Backward window: up to 60 bins ending at i (inclusive).
			// Stop expanding if consecutive bins in the same session
			// are separated by > gapThreshold.
			start := i
			for start > 0 && i-start < windowSize-1 {
				if sessionOf(sbs[start-1].ts) == sessionOf(sbs[start].ts) && sbs[start].ts-sbs[start-1].ts > gapThreshold {
					break
				}
				start--
			}

			curVwap := sbs[i].stats.vwap
			minVwap := curVwap
			for j := start; j < i; j++ {
				if sbs[j].stats.vwap < minVwap {
					minVwap = sbs[j].stats.vwap
				}
			}

			gainPct5m := 0.0
			if minVwap > 0 {
				gainPct5m = (curVwap - minVwap) / minVwap * 100
			}

			trades5m := prefixTrades[i+1] - prefixTrades[start]
			turnover5m := prefixTurnover[i+1] - prefixTurnover[start]

			// Forward gain: max future VWAP vs current.
			gainPctFuture := 0.0
			if i+1 < n && curVwap > 0 {
				gainPctFuture = (suffixMaxVwap[i+1] - curVwap) / curVwap * 100
				if gainPctFuture < 0 {
					gainPctFuture = 0
				}
			}

			tier := ""
			if tiers != nil {
				tier = tiers[sym]
			}

			result = append(result, RollingBarRecord{
				Symbol:        sym,
				Timestamp:     sbs[i].ts,
				Tier:          tier,
				Vwap:          curVwap,
				Trades:        sbs[i].stats.trades,
				Turnover:      sbs[i].stats.turnover,
				GainPct5m:     gainPct5m,
				Trades5m:      trades5m,
				Turnover5m:    turnover5m,
				GainPctFuture: gainPctFuture,
			})
		}
	}

	// Sort output by (timestamp, symbol).
	sort.Slice(result, func(i, j int) bool {
		if result[i].Timestamp != result[j].Timestamp {
			return result[i].Timestamp < result[j].Timestamp
		}
		return result[i].Symbol < result[j].Symbol
	})

	outPath := filepath.Join(dataDir, "us", "stock-trades-ex-index-rolling", date+".parquet")
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return fmt.Errorf("creating rolling bars dir: %w", err)
	}
	if err := parquet.WriteFile(outPath, result); err != nil {
		return fmt.Errorf("writing rolling bars for %s: %w", date, err)
	}

	log.Info("rolling bars written",
		"date", date,
		"symbols", len(symbolBins),
		"bars", len(result),
	)
	return nil
}

// listExIndexDates returns all dates that have ex-index parquet files,
// sorted in ascending order (earliest first).
func listExIndexDates(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		return nil, fmt.Errorf("globbing ex-index files: %w", err)
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
