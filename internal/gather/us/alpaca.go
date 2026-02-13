package us

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	alpacaapi "github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/parquet-go/parquet-go"

	"jupitor/internal/domain"
	"jupitor/internal/gather"
	"jupitor/internal/live"
	"jupitor/internal/store"
)

// ---------------------------------------------------------------------------
// Compile-time interface checks
// ---------------------------------------------------------------------------

var _ gather.Gatherer = (*DailyBarGatherer)(nil)
var _ gather.Gatherer = (*StreamGatherer)(nil)

// utcToETTime converts a UTC time to an ET-as-UTC time.Time. The returned
// time has ET clock values (hour, minute, second) but is tagged as UTC,
// making .UnixMilli() return ET-shifted milliseconds and .Format("2006-01-02")
// return the ET-calendar date.
func utcToETTime(t time.Time, loc *time.Location) time.Time {
	et := t.In(loc)
	_, offset := et.Zone()
	return time.UnixMilli(t.UnixMilli() + int64(offset)*1000).UTC()
}

// ---------------------------------------------------------------------------
// DailyBarGatherer — long-running daemon for daily bars + trade backfill.
// ---------------------------------------------------------------------------

// DailyBarGatherer is a long-running daemon that:
//   - At 8:05 PM ET on trading days, runs a three-phase daily bar update
//   - Otherwise, continuously backfills historical trade data (latest dates first)
type DailyBarGatherer struct {
	client     *marketdata.Client
	barStore   store.BarStore
	tradeStore store.TradeStore
	batchSize  int // symbols per bar API call (5000)
	maxWorkers int // concurrent goroutines for bar fetch (10)

	tradeWorkers int // concurrent goroutines for trade fetch (16)

	startDate    string
	csvPath      string
	apiKey       string
	apiSecret    string
	baseURL      string // live trading API for calendar
	refData      *ReferenceData
	exIndexOnly  bool // when true, trade backfill skips ETFs and index (SPX/NDX) stocks
	loc          *time.Location
	log          *slog.Logger
}

// NewDailyBarGatherer creates a DailyBarGatherer configured with the given
// Alpaca credentials, target stores, and batch parameters.
func NewDailyBarGatherer(
	apiKey, apiSecret, dataURL string,
	barStore store.BarStore,
	tradeStore store.TradeStore,
	batchSize, maxWorkers int,
	tradeWorkers int,
	startDate, csvPath, baseURL, refDir string,
) *DailyBarGatherer {
	opts := marketdata.ClientOpts{
		APIKey:    apiKey,
		APISecret: apiSecret,
	}
	if dataURL != "" {
		opts.BaseURL = dataURL
	}

	loc, _ := time.LoadLocation("America/New_York")

	return &DailyBarGatherer{
		client:       marketdata.NewClient(opts),
		barStore:     barStore,
		tradeStore:   tradeStore,
		batchSize:    batchSize,
		maxWorkers:   maxWorkers,
		tradeWorkers: tradeWorkers,
		startDate:    startDate,
		csvPath:      csvPath,
		apiKey:       apiKey,
		apiSecret:    apiSecret,
		baseURL:      baseURL,
		refData:      LoadReferenceData(refDir),
		loc:          loc,
		log:          slog.Default().With("daemon", "us-alpaca-data"),
	}
}

// Name returns the gatherer identifier.
func (g *DailyBarGatherer) Name() string { return "us-alpaca-data" }

// Run is the main daemon loop. It runs forever, alternating between daily bar
// updates (triggered at 8:05 PM ET) and trade backfill (latest dates first).
func (g *DailyBarGatherer) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		// Check daily bar trigger.
		if g.shouldRunDailyUpdate() {
			g.log.Info("daily update triggered")
			if err := g.runDailyUpdate(ctx); err != nil {
				g.log.Error("daily update failed", "error", err)
			}
		}

		if ctx.Err() != nil {
			return nil
		}

		// Generate missing trade-universe CSVs.
		if n, err := g.tradeUniverseStep(ctx); err != nil {
			g.log.Error("trade universe step error", "error", err)
		} else if n > 0 {
			g.log.Info("trade universe CSVs generated", "count", n)
		}

		if ctx.Err() != nil {
			return nil
		}

		// Trade backfill: pick next date, process it.
		didWork, err := g.tradeBackfillStep(ctx)
		if err != nil {
			g.log.Error("trade backfill error", "error", err)
		}

		if !didWork {
			// Nothing to do — wait and re-check.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(1 * time.Minute):
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Daily bar update trigger
// ---------------------------------------------------------------------------

// shouldRunDailyUpdate returns true if it's after 8:05 PM ET on a trading day
// and today's bars haven't been fetched yet.
func (g *DailyBarGatherer) shouldRunDailyUpdate() bool {
	et, err := time.LoadLocation("America/New_York")
	if err != nil {
		g.log.Error("loading ET timezone", "error", err)
		return false
	}

	now := time.Now().In(et)
	cutoff := time.Date(now.Year(), now.Month(), now.Day(), 20, 5, 0, 0, et)
	if now.Before(cutoff) {
		return false
	}

	// Check if latest trading day's bars are already done.
	endDate, err := LatestFinishedTradingDay(g.apiKey, g.apiSecret, g.baseURL)
	if err != nil {
		g.log.Error("checking trading calendar", "error", err)
		return false
	}

	dailyDir := filepath.Join(g.dataDir(), "us", "daily")
	data, err := os.ReadFile(filepath.Join(dailyDir, ".last-completed"))
	if err != nil {
		return true // no .last-completed → need to run
	}
	return strings.TrimSpace(string(data)) != endDate.Format("2006-01-02")
}

// ---------------------------------------------------------------------------
// Daily bar update (phases 1-3)
// ---------------------------------------------------------------------------

// runDailyUpdate executes the three-phase daily bar update:
//  1. Update known symbols with only missing days
//  2. Discover new symbols via brute-force
//  3. Backfill full history for newly discovered symbols
func (g *DailyBarGatherer) runDailyUpdate(ctx context.Context) error {
	startDate, err := time.Parse("2006-01-02", g.startDate)
	if err != nil {
		return fmt.Errorf("parsing start date %q: %w", g.startDate, err)
	}

	// 1. Determine end date from trading calendar.
	endDate, err := LatestFinishedTradingDay(g.apiKey, g.apiSecret, g.baseURL)
	if err != nil {
		return fmt.Errorf("determining end date: %w", err)
	}
	endDateStr := endDate.Format("2006-01-02")

	// 2. Set up progress tracker.
	dailyDir := filepath.Join(g.dataDir(), "us", "daily")
	tracker, err := newProgressTracker(dailyDir)
	if err != nil {
		return fmt.Errorf("creating progress tracker: %w", err)
	}
	defer tracker.Close()

	// 3. Check idempotency.
	if tracker.IsCompleted(endDateStr) {
		g.log.Info("daily update already completed", "endDate", endDateStr)
		return nil
	}

	// 4. New day vs resume.
	lastCompleted := tracker.LastCompleted()
	if lastCompleted != "" && lastCompleted != endDateStr {
		if err := tracker.Reset(); err != nil {
			return fmt.Errorf("resetting tracker: %w", err)
		}
	}

	// 5. Compute fetchStart: only fetch what's missing.
	var fetchStart time.Time
	if lastCompleted != "" {
		lc, err := time.Parse("2006-01-02", lastCompleted)
		if err != nil {
			return fmt.Errorf("parsing last completed date %q: %w", lastCompleted, err)
		}
		fetchStart = lc.AddDate(0, 0, 1)
	} else {
		fetchStart = startDate // first run: full history
	}

	// 6. Set up universe writer.
	universeDir := filepath.Join(g.dataDir(), "us", "universe")
	universe := newUniverseWriter(universeDir)

	runStart := time.Now()

	// --- Phase 1: Update known symbols with missing days only ---
	known, err := g.barStore.ListSymbols(ctx, "us")
	if err != nil {
		return fmt.Errorf("listing known symbols: %w", err)
	}

	knownSet := make(map[string]struct{}, len(known))
	for _, sym := range known {
		knownSet[sym] = struct{}{}
	}

	if len(known) > 0 {
		totalBatches := (len(known) + g.batchSize - 1) / max(g.batchSize, 1)
		g.log.Info("phase=update",
			"known", len(known),
			"batches", totalBatches,
			"fetchStart", fetchStart.Format("2006-01-02"),
			"fetchEnd", endDateStr,
		)

		updateHits, err := g.processBatches(ctx, known, fetchStart, endDate,
			tracker, universe, false, "update", runStart)
		if err != nil {
			return fmt.Errorf("phase update: %w", err)
		}

		g.log.Info("phase=update complete",
			"hits", len(updateHits),
			"elapsed", time.Since(runStart).Round(time.Second),
		)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// --- Phase 2: Discover new symbols via brute-force ---
	allSymbols, err := AllBruteSymbols(g.csvPath)
	if err != nil {
		return fmt.Errorf("generating symbols: %w", err)
	}

	var remaining []string
	for _, sym := range allSymbols {
		if _, isKnown := knownSet[sym]; isKnown {
			continue
		}
		if tracker.IsTriedEmpty(sym) {
			continue
		}
		remaining = append(remaining, sym)
	}

	var newDiscoveries []string

	if len(remaining) > 0 {
		totalBatches := (len(remaining) + g.batchSize - 1) / max(g.batchSize, 1)
		g.log.Info("phase=discover",
			"remaining", len(remaining),
			"batches", totalBatches,
			"fetchStart", fetchStart.Format("2006-01-02"),
			"fetchEnd", endDateStr,
		)

		newDiscoveries, err = g.processBatches(ctx, remaining, fetchStart, endDate,
			tracker, universe, true, "discover", runStart)
		if err != nil {
			return fmt.Errorf("phase discover: %w", err)
		}

		g.log.Info("phase=discover complete",
			"newSymbols", len(newDiscoveries),
			"elapsed", time.Since(runStart).Round(time.Second),
		)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// --- Phase 3: Backfill full history for newly discovered symbols ---
	if len(newDiscoveries) > 0 {
		g.log.Info("phase=backfill",
			"symbols", len(newDiscoveries),
			"fetchStart", startDate.Format("2006-01-02"),
			"fetchEnd", endDateStr,
		)

		backfillHits, err := g.processBatches(ctx, newDiscoveries, startDate, endDate,
			tracker, universe, false, "backfill", runStart)
		if err != nil {
			return fmt.Errorf("phase backfill: %w", err)
		}

		g.log.Info("phase=backfill complete",
			"hits", len(backfillHits),
			"elapsed", time.Since(runStart).Round(time.Second),
		)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Finalize universe files.
	if err := universe.Finalize(); err != nil {
		return fmt.Errorf("finalizing universe: %w", err)
	}

	// Mark completed.
	if err := tracker.MarkCompleted(endDateStr); err != nil {
		return fmt.Errorf("marking completed: %w", err)
	}

	g.log.Info("daily update complete",
		"endDate", endDateStr,
		"elapsed", time.Since(runStart).Round(time.Second),
	)
	return nil
}

// processBatches splits symbols into batches and processes them concurrently.
// It returns the list of symbols that had bar data (hits). If markEmpty is
// true, symbols with no data are recorded in the progress tracker.
func (g *DailyBarGatherer) processBatches(ctx context.Context, symbols []string,
	start, end time.Time, tracker *progressTracker, universe *universeWriter,
	markEmpty bool, phase string, runStart time.Time) ([]string, error) {

	var batches [][]string
	for i := 0; i < len(symbols); i += g.batchSize {
		e := min(i+g.batchSize, len(symbols))
		batches = append(batches, symbols[i:e])
	}

	totalBatches := len(batches)

	batchCh := make(chan int, len(batches))
	for i := range batches {
		batchCh <- i
	}
	close(batchCh)

	var (
		mu      sync.Mutex
		allHits []string
		wg      sync.WaitGroup
	)

	workers := min(g.maxWorkers, len(batches))
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batchIdx := range batchCh {
				if ctx.Err() != nil {
					return
				}

				batch := batches[batchIdx]
				bars, err := g.fetchMultiBars(ctx, batch, start, end)
				if err != nil {
					g.log.Error("batch fetch failed",
						"phase", phase,
						"batch", fmt.Sprintf("%d/%d", batchIdx+1, totalBatches),
						"err", err,
					)
					continue
				}

				hitSymbols := make(map[string]struct{})
				for _, b := range bars {
					hitSymbols[b.Symbol] = struct{}{}
				}

				// Write bars to store.
				if len(bars) > 0 {
					if err := g.barStore.WriteBars(ctx, bars); err != nil {
						g.log.Error("writing bars failed", "phase", phase, "err", err)
						continue
					}
					universe.AddBars(bars)
					if err := universe.Flush(); err != nil {
						g.log.Error("flushing universe failed", "phase", phase, "err", err)
					}
				}

				// Mark empty symbols.
				if markEmpty {
					var emptySymbols []string
					for _, sym := range batch {
						if _, hit := hitSymbols[sym]; !hit {
							emptySymbols = append(emptySymbols, sym)
						}
					}
					if len(emptySymbols) > 0 {
						if err := tracker.MarkEmpty(emptySymbols); err != nil {
							g.log.Error("marking empty failed", "phase", phase, "err", err)
						}
					}
				}

				// Collect hit symbol names for return.
				if len(hitSymbols) > 0 {
					mu.Lock()
					for sym := range hitSymbols {
						allHits = append(allHits, sym)
					}
					mu.Unlock()
				}

				g.log.Info("batch done",
					"phase", phase,
					"batch", fmt.Sprintf("%d/%d", batchIdx+1, totalBatches),
					"hits", len(hitSymbols),
					"empty", len(batch)-len(hitSymbols),
					"elapsed", time.Since(runStart).Round(time.Second),
				)
			}
		}()
	}

	wg.Wait()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return allHits, nil
}

// fetchMultiBars fetches daily bars for multiple symbols in a single API call.
func (g *DailyBarGatherer) fetchMultiBars(ctx context.Context, symbols []string, start, end time.Time) ([]domain.Bar, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Alpaca's End is exclusive for daily bars, so add one day to include
	// bars on the end date itself.
	multiBars, err := g.client.GetMultiBars(symbols, marketdata.GetBarsRequest{
		TimeFrame: marketdata.OneDay,
		Start:     start,
		End:       end.AddDate(0, 0, 1),
		Feed:      "sip",
	})
	if err != nil {
		return nil, fmt.Errorf("GetMultiBars: %w", err)
	}

	var bars []domain.Bar
	for symbol, alpacaBars := range multiBars {
		for _, ab := range alpacaBars {
			bars = append(bars, domain.Bar{
				Symbol:     strings.ToUpper(symbol),
				Timestamp:  ab.Timestamp,
				Open:       ab.Open,
				High:       ab.High,
				Low:        ab.Low,
				Close:      ab.Close,
				Volume:     int64(ab.Volume),
				TradeCount: int64(ab.TradeCount),
				VWAP:       ab.VWAP,
			})
		}
	}
	return bars, nil
}

// ---------------------------------------------------------------------------
// Trade backfill
// ---------------------------------------------------------------------------

// SetExIndexOnly configures the gatherer to only backfill ex-index stocks
// (no ETFs, no SPX/NDX constituents) when true.
func (g *DailyBarGatherer) SetExIndexOnly(v bool) {
	g.exIndexOnly = v
}

// tradeUniverseStep generates trade-universe CSVs for universe dates that have
// index files but no existing CSV. Returns the number of CSVs written.
func (g *DailyBarGatherer) tradeUniverseStep(ctx context.Context) (int, error) {
	universeDir := filepath.Join(g.dataDir(), "us", "universe")
	dates, err := ListUniverseDates(universeDir)
	if err != nil {
		return 0, fmt.Errorf("listing universe dates: %w", err)
	}

	wrote := 0
	for _, date := range dates {
		if ctx.Err() != nil {
			return wrote, ctx.Err()
		}

		csvPath := tradeUniversePath(g.dataDir(), date)
		if _, err := os.Stat(csvPath); err == nil {
			continue
		}

		// Require both SPX and NDX index files for the date.
		spxPath := filepath.Join(g.dataDir(), "us", "index", "spx", date+".txt")
		ndxPath := filepath.Join(g.dataDir(), "us", "index", "ndx", date+".txt")
		if _, err := os.Stat(spxPath); os.IsNotExist(err) {
			continue
		}
		if _, err := os.Stat(ndxPath); os.IsNotExist(err) {
			continue
		}

		symbols, err := ReadUniverseFile(filepath.Join(universeDir, date+".txt"))
		if err != nil {
			g.log.Error("reading universe file", "date", date, "error", err)
			continue
		}

		if err := generateTradeUniverseForDate(g.dataDir(), date, symbols, g.refData, g.log); err != nil {
			continue
		}
		wrote++
	}

	return wrote, nil
}

// tradeBackfillStep finds the latest universe date with missing trade files
// and processes it. Returns (true, nil) if work was done, (false, nil) if
// all dates are complete. Only symbols without existing trade files are
// fetched, so new symbols added to historical universe files are picked up.
func (g *DailyBarGatherer) tradeBackfillStep(ctx context.Context) (bool, error) {
	universeDir := filepath.Join(g.dataDir(), "us", "universe")
	dates, err := ListUniverseDates(universeDir)
	if err != nil {
		return false, fmt.Errorf("listing universe dates: %w", err)
	}

	// Load index sets for ex-index filtering.
	var spxSet, ndxSet map[string]bool
	if g.exIndexOnly {
		latestDate := dates[len(dates)-1]
		spxPath := filepath.Join(g.dataDir(), "us", "index", "spx", latestDate+".txt")
		ndxPath := filepath.Join(g.dataDir(), "us", "index", "ndx", latestDate+".txt")
		spxSet = readIndexSet(spxPath)
		ndxSet = readIndexSet(ndxPath)
	}

	for _, date := range dates {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		symbols, err := ReadUniverseFile(filepath.Join(universeDir, date+".txt"))
		if err != nil {
			g.log.Error("reading universe file", "date", date, "error", err)
			continue
		}

		dayTime, err := time.Parse("2006-01-02", date)
		if err != nil {
			continue
		}

		// Find symbols missing trade files for this date.
		var missing []string
		for _, sym := range symbols {
			if g.exIndexOnly && (g.refData.ETFs[sym] || spxSet[sym] || ndxSet[sym]) {
				continue
			}
			tradePath := g.tradePath(sym, dayTime)
			if _, err := os.Stat(tradePath); os.IsNotExist(err) {
				missing = append(missing, sym)
			}
		}

		if len(missing) == 0 {
			continue
		}

		g.log.Info("trade backfill",
			"date", date,
			"missing", len(missing),
			"total", len(symbols),
			"exIndexOnly", g.exIndexOnly,
		)

		count, err := g.ProcessTradeDay(ctx, dayTime, missing)
		if err != nil {
			return true, fmt.Errorf("processing trade day %s: %w", date, err)
		}

		g.log.Info("trade backfill done",
			"date", date,
			"symbols", len(missing),
			"trades", count,
		)

		return true, nil
	}

	return false, nil
}

// symbolTradeCounts reads the trade_count from bar data for each symbol on
// the given date. Returns a map of symbol → trade_count.
func (g *DailyBarGatherer) symbolTradeCounts(ctx context.Context, symbols []string, day time.Time) map[string]int64 {
	// Alpaca daily bar timestamps are at 05:00 UTC (midnight ET), not midnight
	// UTC. Use a full-day range to capture the bar regardless of exact timestamp.
	dayEnd := day.AddDate(0, 0, 1)
	counts := make(map[string]int64, len(symbols))
	for _, sym := range symbols {
		bars, err := g.barStore.ReadBars(ctx, sym, "us", day, dayEnd)
		if err == nil && len(bars) > 0 {
			counts[sym] = bars[0].TradeCount
		}
	}
	return counts
}

// buildTradeBatches groups symbols into batches targeting ~100K total trades
// each (based on trade_count from bar data). Symbols must be sorted by
// trade_count descending. Each batch has at least 1 symbol.
func buildTradeBatches(symbols []string, counts map[string]int64) [][]string {
	const targetTradesPerBatch = 500_000

	var batches [][]string
	var batch []string
	var batchCount int64

	for _, sym := range symbols {
		tc := counts[sym]
		if tc <= 0 {
			tc = 1
		}

		// Start a new batch if adding this symbol would exceed the target
		// and the current batch already has symbols.
		if len(batch) > 0 && batchCount+tc > targetTradesPerBatch {
			batches = append(batches, batch)
			batch = nil
			batchCount = 0
		}

		batch = append(batch, sym)
		batchCount += tc
	}

	if len(batch) > 0 {
		batches = append(batches, batch)
	}

	return batches
}

// ProcessTradeDay fetches trades for a single date using batched multi-symbol
// API calls. Symbols are grouped into ~500K-trade batches based on bar
// trade_count. Returns the total number of trades written.
func (g *DailyBarGatherer) ProcessTradeDay(ctx context.Context, day time.Time, symbols []string) (int, error) {
	// Read trade counts and sort symbols by trade_count descending.
	counts := g.symbolTradeCounts(ctx, symbols, day)
	sort.Slice(symbols, func(i, j int) bool {
		return counts[symbols[i]] > counts[symbols[j]]
	})

	batches := buildTradeBatches(symbols, counts)

	totalBatches := len(batches)
	batchCh := make(chan int, len(batches))
	for i := range batches {
		batchCh <- i
	}
	close(batchCh)

	var (
		mu         sync.Mutex
		totalCount int
		wg         sync.WaitGroup
	)

	workers := min(g.tradeWorkers, len(batches))
	ticker := time.NewTicker(300 * time.Millisecond) // ~200/min rate limit
	defer ticker.Stop()

	g.log.Info("trade day started",
		"date", day.Format("2006-01-02"),
		"symbols", len(symbols),
		"batches", totalBatches,
		"workers", workers,
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batchIdx := range batchCh {
				if ctx.Err() != nil {
					return
				}

				// Rate limit.
				<-ticker.C

				batch := batches[batchIdx]

				var trades []domain.Trade
				var fetchErr error
				for attempt := 1; attempt <= 3; attempt++ {
					trades, fetchErr = g.fetchMultiTrades(ctx, batch, day)
					if fetchErr == nil {
						break
					}
					g.log.Warn("trade fetch retry",
						"date", day.Format("2006-01-02"),
						"batch", fmt.Sprintf("%d/%d", batchIdx+1, totalBatches),
						"symbols", len(batch),
						"attempt", fmt.Sprintf("%d/3", attempt),
						"err", fetchErr,
					)
					select {
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Second):
					}
				}
				if fetchErr != nil {
					g.log.Error("trade fetch failed, skipping batch",
						"date", day.Format("2006-01-02"),
						"batch", fmt.Sprintf("%d/%d", batchIdx+1, totalBatches),
						"symbols", len(batch),
						"err", fetchErr,
					)
					continue
				}

				if len(trades) > 0 {
					if err := g.tradeStore.WriteTrades(ctx, trades); err != nil {
						g.log.Error("writing trades failed",
							"date", day.Format("2006-01-02"),
							"batch", fmt.Sprintf("%d/%d", batchIdx+1, totalBatches),
							"err", err,
						)
						continue
					}
				}

				// Write empty parquet files for symbols with no qualifying
				// trades so they are not retried on subsequent runs.
				tradeSymbols := make(map[string]struct{})
				for _, t := range trades {
					tradeSymbols[t.Symbol] = struct{}{}
				}
				for _, sym := range batch {
					if _, ok := tradeSymbols[sym]; !ok {
						g.writeEmptyTradeFile(sym, day)
					}
				}

				mu.Lock()
				totalCount += len(trades)
				mu.Unlock()

				g.log.Info("trade batch done",
					"date", day.Format("2006-01-02"),
					"batch", fmt.Sprintf("%d/%d", batchIdx+1, totalBatches),
					"symbols", len(batch),
					"trades", len(trades),
				)
			}
		}()
	}

	wg.Wait()

	if ctx.Err() != nil {
		return totalCount, ctx.Err()
	}

	return totalCount, nil
}

// writeEmptyTradeFile creates an empty parquet file (schema only, zero rows)
// for a symbol-date pair so backfill knows it was already processed.
func (g *DailyBarGatherer) writeEmptyTradeFile(symbol string, day time.Time) {
	path := g.tradePath(symbol, day)
	if _, err := os.Stat(path); err == nil {
		return // already exists
	}
	// WriteTrades with an empty slice is a no-op, so write directly.
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	_ = parquet.WriteFile(path, []store.TradeRecord{})
}

// fetchMultiTrades fetches trades for multiple symbols in a single API call
// for one trading day (4AM–8PM ET). Timestamps are stored as ET-shifted
// milliseconds. Only trades with size > 100 AND price * size >= 100 are returned.
func (g *DailyBarGatherer) fetchMultiTrades(ctx context.Context, symbols []string, day time.Time) ([]domain.Trade, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Query window: 4AM–8PM ET on this trading day.
	startET := time.Date(day.Year(), day.Month(), day.Day(), 4, 0, 0, 0, g.loc)
	endET := time.Date(day.Year(), day.Month(), day.Day(), 20, 0, 0, 0, g.loc)

	multiTrades, err := g.client.GetMultiTrades(symbols, marketdata.GetTradesRequest{
		Start: startET,
		End:   endET,
		Feed:  marketdata.SIP,
	})
	if err != nil {
		return nil, fmt.Errorf("GetMultiTrades: %w", err)
	}

	var trades []domain.Trade
	for symbol, sdkTrades := range multiTrades {
		for _, t := range sdkTrades {
			size := int64(t.Size)
			amount := t.Price * float64(size)
			if size > 100 && amount >= 100 {
				trades = append(trades, domain.Trade{
					Symbol:     strings.ToUpper(symbol),
					Timestamp:  utcToETTime(t.Timestamp, g.loc),
					Price:      t.Price,
					Size:       size,
					Exchange:   t.Exchange,
					ID:         strconv.FormatInt(t.ID, 10),
					Conditions: strings.Join(t.Conditions, ","),
					Update:     t.Update,
				})
			}
		}
	}
	return trades, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// dataDir returns the data directory from the bar store.
func (g *DailyBarGatherer) dataDir() string {
	return g.barStore.(*store.ParquetStore).DataDir
}

// tradePath returns the expected trade file path for a symbol and date.
func (g *DailyBarGatherer) tradePath(symbol string, day time.Time) string {
	date := day.Format("2006-01-02")
	return filepath.Join(g.dataDir(), "us", "trades", strings.ToUpper(symbol), date+".parquet")
}

// ---------------------------------------------------------------------------
// StreamGatherer — live WebSocket streaming + REST backfill of trades.
// ---------------------------------------------------------------------------

// StreamGatherer connects to the Alpaca real-time WebSocket feed, backfills
// today's trades via the REST API, and maintains a shared LiveModel.
type StreamGatherer struct {
	apiKey    string
	apiSecret string
	baseURL   string // Alpaca trading API base URL (for GetAssets)
	dataDir   string
	csvPath   string // path to symbol_5_chars.csv
	refDir    string // path to reference/us/ directory
	model     *live.LiveModel
	log       *slog.Logger
	ready     chan struct{}

	stockSyms    map[string]bool // ex-index stock symbols (fast lookup)
	loc          *time.Location
	dateMu       sync.RWMutex // protects today, prevDate, prevCloseUTC
	today        string       // "YYYY-MM-DD"
	prevDate     string       // previous trading day
	prevCloseUTC time.Time    // prevDate 4PM ET in UTC
}

// NewStreamGatherer creates a StreamGatherer that loads symbols from the
// Alpaca API, backfills per-symbol via REST, and streams via WebSocket.
func NewStreamGatherer(apiKey, apiSecret, baseURL, dataDir, csvPath, refDir string) *StreamGatherer {
	return &StreamGatherer{
		apiKey:    apiKey,
		apiSecret: apiSecret,
		baseURL:   baseURL,
		dataDir:   dataDir,
		csvPath:   csvPath,
		refDir:    refDir,
		log:       slog.Default().With("gatherer", "us-stream"),
		ready:     make(chan struct{}),
	}
}

// Name returns the gatherer identifier.
func (g *StreamGatherer) Name() string { return "us-stream" }

// Model returns the shared LiveModel (available after Run starts).
func (g *StreamGatherer) Model() *live.LiveModel { return g.model }

// Ready returns a channel that is closed once the model is created and the
// WebSocket stream is connected. Use this instead of sleeping in main.
func (g *StreamGatherer) Ready() <-chan struct{} { return g.ready }

// Run starts backfill + streaming. It blocks until ctx is cancelled.
func (g *StreamGatherer) Run(ctx context.Context) error {
	var err error
	g.loc, err = time.LoadLocation("America/New_York")
	if err != nil {
		return fmt.Errorf("loading timezone: %w", err)
	}

	// Determine today's trading day. Before 3:50 AM ET the current trading
	// session still belongs to the previous calendar date (day switch hasn't
	// fired yet), so use yesterday's date.
	now := time.Now().In(g.loc)
	cutoff350 := time.Date(now.Year(), now.Month(), now.Day(), 3, 50, 0, 0, g.loc)
	if now.Before(cutoff350) {
		g.today = now.AddDate(0, 0, -1).Format("2006-01-02")
	} else {
		g.today = now.Format("2006-01-02")
	}

	g.prevDate, err = g.findPrevTradingDay()
	if err != nil {
		return fmt.Errorf("finding previous trading day: %w", err)
	}

	g.log.Info("dates determined", "today", g.today, "prevDate", g.prevDate)

	// Load symbols dynamically from Alpaca API.
	stockSyms, err := g.loadSymbolsFromAPI()
	if err != nil {
		return fmt.Errorf("loading symbols from API: %w", err)
	}
	g.stockSyms = stockSyms

	g.log.Info("loaded symbols", "exIndexStocks", len(g.stockSyms))

	// Compute todayCutoff = D 4PM ET in the ET-shifted millisecond frame
	// (must match utcToETMilli output so the model classifies correctly).
	todayCutoff, err := g.etClose(g.today)
	if err != nil {
		return fmt.Errorf("computing today cutoff: %w", err)
	}

	prevDateT, err := time.ParseInLocation("2006-01-02", g.prevDate, g.loc)
	if err != nil {
		return fmt.Errorf("parsing prev date: %w", err)
	}
	g.prevCloseUTC = time.Date(prevDateT.Year(), prevDateT.Month(), prevDateT.Day(), 16, 0, 0, 0, g.loc)

	g.model = live.NewLiveModel(todayCutoff)

	// Load backfill cache from /tmp (if exists from earlier run today).
	g.loadBackfillCache()

	// Start WebSocket stream immediately (captures from NOW).
	streamClient := stream.NewStocksClient(
		marketdata.SIP,
		stream.WithCredentials(g.apiKey, g.apiSecret),
		stream.WithTrades(func(t stream.Trade) {
			g.handleStreamTrade(t)
		}, "*"),
	)

	if err := streamClient.Connect(ctx); err != nil {
		return fmt.Errorf("connecting WebSocket: %w", err)
	}

	g.log.Info("WebSocket stream connected")

	// Signal readiness — model is created and stream is connected.
	close(g.ready)

	// Start background goroutines.
	go g.runBackfill(ctx)
	go g.runDaySwitch(ctx)
	go g.logStatus(ctx)

	// Wait for context cancellation or stream termination.
	select {
	case <-ctx.Done():
		g.log.Info("context cancelled, shutting down")
	case err := <-streamClient.Terminated():
		if err != nil {
			g.log.Error("stream terminated", "error", err)
			return fmt.Errorf("stream terminated: %w", err)
		}
	}

	tIdx, tExIdx, nIdx, nExIdx := g.model.Counts()
	g.log.Info("final counts",
		"todayIndex", tIdx,
		"todayExIndex", tExIdx,
		"nextIndex", nIdx,
		"nextExIndex", nExIdx,
		"seen", g.model.SeenCount(),
	)

	return nil
}

// loadSymbolsFromAPI fetches active US equity assets from the Alpaca trading
// API and filters to ex-index stocks (tradable, not ETF, not SPX/NDX).
func (g *StreamGatherer) loadSymbolsFromAPI() (map[string]bool, error) {
	client := alpacaapi.NewClient(alpacaapi.ClientOpts{
		APIKey:    g.apiKey,
		APISecret: g.apiSecret,
		BaseURL:   g.baseURL,
	})

	assets, err := client.GetAssets(alpacaapi.GetAssetsRequest{
		Status:     "active",
		AssetClass: "us_equity",
	})
	if err != nil {
		return nil, fmt.Errorf("GetAssets: %w", err)
	}

	g.log.Info("fetched assets from Alpaca API", "total", len(assets))

	// Build set of allowed 5+ char symbols from CSV.
	fiveCharSet := make(map[string]bool)
	if csvSyms, err := LoadCSVSymbols(g.csvPath); err == nil {
		for _, s := range csvSyms {
			if len(s) >= 5 {
				fiveCharSet[s] = true
			}
		}
		g.log.Info("loaded 5+ char symbols from CSV", "count", len(fiveCharSet))
	} else {
		g.log.Warn("could not load CSV symbols", "path", g.csvPath, "error", err)
	}

	// Load ETF reference data.
	refData := LoadReferenceData(g.refDir)

	// Load SPX/NDX index sets for today.
	spxPath := filepath.Join(g.dataDir, "us", "index", "spx", g.today+".txt")
	ndxPath := filepath.Join(g.dataDir, "us", "index", "ndx", g.today+".txt")
	spxSet := readIndexSet(spxPath)
	ndxSet := readIndexSet(ndxPath)

	g.log.Info("loaded index constituents", "spx", len(spxSet), "ndx", len(ndxSet))

	// Filter: tradable, len<=4 or in fiveCharSet, not ETF, not index.
	stockSyms := make(map[string]bool, len(assets)/2)
	for _, a := range assets {
		sym := a.Symbol
		if !a.Tradable {
			continue
		}
		if len(sym) > 4 && !fiveCharSet[sym] {
			continue
		}
		if refData.ETFs[sym] {
			continue
		}
		if spxSet[sym] || ndxSet[sym] {
			continue
		}
		stockSyms[sym] = true
	}

	return stockSyms, nil
}

// handleStreamTrade processes a single trade from the WebSocket stream.
func (g *StreamGatherer) handleStreamTrade(t stream.Trade) {
	if !g.stockSyms[t.Symbol] {
		return
	}

	// Apply size filter: size > 100 AND price*size >= 100.
	if int64(t.Size) <= 100 || t.Price*float64(t.Size) < 100 {
		return
	}

	conditions := strings.Join(t.Conditions, ",")
	record := store.TradeRecord{
		Symbol:     t.Symbol,
		Timestamp:  g.utcToETMilli(t.Timestamp),
		Price:      t.Price,
		Size:       int64(t.Size),
		Exchange:   t.Exchange,
		ID:         strconv.FormatInt(t.ID, 10),
		Conditions: conditions,
	}

	// Apply exchange/condition filter.
	if !filterTradeRecord(record) {
		return
	}

	// Always ex-index (index stocks are excluded from stockSyms).
	g.model.Add(record, t.ID, false)
}

// runBackfill uses 4 workers to fetch trades per-symbol from prevDate 4PM ET
// → now. Each symbol gets its own cache file for incremental resume. After a
// full scan, waits 5 min and rescans (stream fills the gap).
func (g *StreamGatherer) runBackfill(ctx context.Context) {
	client := marketdata.NewClient(marketdata.ClientOpts{
		APIKey:    g.apiKey,
		APISecret: g.apiSecret,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second, // default is 10s, too short for paginated calls
		},
	})

	for {
		// Snapshot date fields for this scan.
		g.dateMu.RLock()
		today := g.today
		prevCloseUTC := g.prevCloseUTC
		g.dateMu.RUnlock()

		cacheDir := filepath.Join(os.TempDir(), "us-stream", today, "backfill")

		// Build shuffled symbol list for fair distribution.
		symbols := make([]string, 0, len(g.stockSyms))
		for sym := range g.stockSyms {
			symbols = append(symbols, sym)
		}
		rand.Shuffle(len(symbols), func(i, j int) {
			symbols[i], symbols[j] = symbols[j], symbols[i]
		})

		symCh := make(chan string, len(symbols))
		for _, s := range symbols {
			symCh <- s
		}
		close(symCh)

		var (
			wg        sync.WaitGroup
			cached    atomic.Int64 // trades written to cache files
			added     atomic.Int64 // trades new to in-memory model
			completed atomic.Int64
		)
		scanStart := time.Now()

		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for sym := range symCh {
					if ctx.Err() != nil {
						return
					}
					c, a := g.backfillSymbol(ctx, client, sym, cacheDir, prevCloseUTC)
					cached.Add(int64(c))
					added.Add(int64(a))
					done := completed.Add(1)
					if done%500 == 0 {
						g.log.Info("backfill progress",
							"done", done,
							"total", len(symbols),
							"cached", cached.Load(),
							"addedToModel", added.Load(),
							"elapsed", time.Since(scanStart).Round(time.Second),
						)
					}
				}
			}()
		}
		wg.Wait()

		g.log.Info("backfill scan complete",
			"symbols", len(symbols),
			"cached", cached.Load(),
			"addedToModel", added.Load(),
			"elapsed", time.Since(scanStart).Round(time.Second),
		)

		if ctx.Err() != nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Minute):
		}
	}
}

// backfillSymbol fetches trades for a single symbol, resuming from the latest
// cached timestamp if a per-symbol cache file exists. Returns (cached, added):
// cached = trades written to cache file, added = trades new to the in-memory model.
// cacheDir and prevCloseUTC are snapshot values from the caller (thread-safe).
func (g *StreamGatherer) backfillSymbol(ctx context.Context, client *marketdata.Client, sym string, cacheDir string, prevCloseUTC time.Time) (int, int) {
	if ctx.Err() != nil {
		return 0, 0
	}

	cachePath := filepath.Join(cacheDir, sym+".parquet")

	// Read existing cache to determine start time.
	var existing []store.TradeRecord
	start := prevCloseUTC

	if records, err := parquet.ReadFile[store.TradeRecord](cachePath); err == nil && len(records) > 0 {
		existing = records
		// Find latest timestamp (ET ms) and convert back to UTC.
		var latestET int64
		for _, r := range records {
			if r.Timestamp > latestET {
				latestET = r.Timestamp
			}
		}
		start = g.etMilliToUTC(latestET).Add(time.Millisecond)
	}

	end := time.Now().UTC()
	if !start.Before(end) {
		return 0, 0
	}

	trades, err := client.GetTrades(sym, marketdata.GetTradesRequest{
		Start: start,
		End:   end,
		Feed:  marketdata.SIP,
	})
	if err != nil {
		g.log.Error("backfill fetch failed", "symbol", sym, "error", err)
		return 0, 0
	}

	// Filter and convert.
	var newRecords []store.TradeRecord
	var newIDs []int64
	for _, t := range trades {
		if int64(t.Size) <= 100 || t.Price*float64(t.Size) < 100 {
			continue
		}

		conditions := strings.Join(t.Conditions, ",")
		record := store.TradeRecord{
			Symbol:     sym,
			Timestamp:  g.utcToETMilli(t.Timestamp),
			Price:      t.Price,
			Size:       int64(t.Size),
			Exchange:   t.Exchange,
			ID:         strconv.FormatInt(t.ID, 10),
			Conditions: conditions,
			Update:     t.Update,
		}

		if !filterTradeRecord(record) {
			continue
		}

		newRecords = append(newRecords, record)
		newIDs = append(newIDs, t.ID)
	}

	if len(newRecords) == 0 {
		return 0, 0
	}

	// Append to existing cache and write back.
	all := append(existing, newRecords...)
	g.writeSymbolCache(cachePath, all)

	// Add only new records to model (stream may already have them).
	added := g.model.AddBatch(newRecords, newIDs, false)
	return len(newRecords), added
}

// writeSymbolCache writes trade records to a per-symbol cache parquet file.
func (g *StreamGatherer) writeSymbolCache(path string, records []store.TradeRecord) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		g.log.Error("creating backfill cache dir", "error", err)
		return
	}
	if err := parquet.WriteFile(path, records); err != nil {
		g.log.Error("writing backfill cache", "path", path, "error", err)
	}
}

// logStatus periodically logs model counts.
func (g *StreamGatherer) logStatus(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tIdx, tExIdx, nIdx, nExIdx := g.model.Counts()
			g.log.Info("model status",
				"todayIndex", tIdx,
				"todayExIndex", tExIdx,
				"nextIndex", nIdx,
				"nextExIndex", nExIdx,
				"seen", g.model.SeenCount(),
			)
		}
	}
}

// findPrevTradingDay returns the most recent trading day before today by
// looking at existing trade-universe CSV files.
func (g *StreamGatherer) findPrevTradingDay() (string, error) {
	tuDir := filepath.Join(g.dataDir, "us", "trade-universe")
	dates, err := listTradeUniverseDates(tuDir)
	if err != nil {
		return "", err
	}

	for i := len(dates) - 1; i >= 0; i-- {
		if dates[i] < g.today {
			return dates[i], nil
		}
	}
	return "", fmt.Errorf("no previous trading day found before %s", g.today)
}

// etClose returns 4PM ET on the given date as ET-shifted milliseconds
// (consistent with utcToETMilli). Use this for LiveModel cutoffs.
func (g *StreamGatherer) etClose(dateStr string) (int64, error) {
	return regularClose(dateStr)
}

// utcToETMilli converts a UTC time.Time to ET Unix milliseconds.
func (g *StreamGatherer) utcToETMilli(t time.Time) int64 {
	et := t.In(g.loc)
	_, offset := et.Zone()
	return t.UnixMilli() + int64(offset)*1000
}

// etMilliToUTC converts an ET Unix millisecond timestamp back to a UTC time.
func (g *StreamGatherer) etMilliToUTC(etMs int64) time.Time {
	approx := time.UnixMilli(etMs)
	_, offset := approx.In(g.loc).Zone()
	return time.UnixMilli(etMs - int64(offset)*1000)
}

// ---------------------------------------------------------------------------
// Backfill cache: /tmp/us-stream/<YYYY-MM-DD>/backfill/<SYMBOL>.parquet
// ---------------------------------------------------------------------------

func (g *StreamGatherer) cacheDir() string {
	return filepath.Join(os.TempDir(), "us-stream", g.today)
}

// loadBackfillCache scans per-symbol cache files from a previous run today
// and populates the model's seen set so stream dedup works correctly.
func (g *StreamGatherer) loadBackfillCache() {
	dir := filepath.Join(g.cacheDir(), "backfill")
	matches, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil || len(matches) == 0 {
		return
	}

	totalRecords := 0
	totalAdded := 0
	for _, path := range matches {
		records, err := parquet.ReadFile[store.TradeRecord](path)
		if err != nil || len(records) == 0 {
			continue
		}

		ids := make([]int64, len(records))
		for i := range records {
			id, _ := strconv.ParseInt(records[i].ID, 10, 64)
			ids[i] = id
		}

		added := g.model.AddBatch(records, ids, false)
		totalRecords += len(records)
		totalAdded += added
	}

	if totalRecords > 0 {
		g.log.Info("loaded backfill cache",
			"files", len(matches),
			"records", totalRecords,
			"added", totalAdded,
		)
	}
}

// ---------------------------------------------------------------------------
// Day switching: advances the model to a new trading day at 3:50 AM ET.
// ---------------------------------------------------------------------------

// isTradingDay checks whether the given date is a trading day using the
// Alpaca Calendar API.
func (g *StreamGatherer) isTradingDay(date string) (bool, error) {
	client := alpacaapi.NewClient(alpacaapi.ClientOpts{
		APIKey:    g.apiKey,
		APISecret: g.apiSecret,
		BaseURL:   g.baseURL,
	})
	d, err := time.Parse("2006-01-02", date)
	if err != nil {
		return false, err
	}
	cal, err := client.GetCalendar(alpacaapi.GetCalendarRequest{Start: d, End: d})
	if err != nil {
		return false, err
	}
	return len(cal) > 0 && cal[0].Date == date, nil
}

// runDaySwitch fires at 3:50 AM ET each day and, on trading days, promotes
// the next-day bucket to today and resets backfill for the new window.
func (g *StreamGatherer) runDaySwitch(ctx context.Context) {
	for {
		// Sleep until next 3:50 AM ET.
		now := time.Now().In(g.loc)
		next350 := time.Date(now.Year(), now.Month(), now.Day(), 3, 50, 0, 0, g.loc)
		if !now.Before(next350) {
			next350 = next350.AddDate(0, 0, 1)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(next350)):
		}

		newDay := time.Now().In(g.loc).Format("2006-01-02")

		g.dateMu.RLock()
		oldToday := g.today
		g.dateMu.RUnlock()

		if newDay == oldToday {
			continue // shouldn't happen, but guard
		}

		isTrading, err := g.isTradingDay(newDay)
		if err != nil {
			g.log.Error("calendar check failed", "error", err)
			continue
		}
		if !isTrading {
			g.log.Info("day switch skipped (non-trading day)", "date", newDay)
			continue
		}

		// Compute new cutoff + prev close.
		newCutoff, _ := g.etClose(newDay)
		oldTodayT, _ := time.ParseInLocation("2006-01-02", oldToday, g.loc)
		newPrevCloseUTC := time.Date(oldTodayT.Year(), oldTodayT.Month(), oldTodayT.Day(), 16, 0, 0, 0, g.loc)

		// Switch model.
		g.model.SwitchDay(newCutoff)

		// Update gatherer date fields.
		g.dateMu.Lock()
		g.today = newDay
		g.prevDate = oldToday
		g.prevCloseUTC = newPrevCloseUTC
		g.dateMu.Unlock()

		// Clean old cache dir (best-effort).
		oldCacheDir := filepath.Join(os.TempDir(), "us-stream", oldToday)
		os.RemoveAll(oldCacheDir)

		g.log.Info("day switch complete",
			"newToday", newDay, "prevDate", oldToday,
			"newCutoff", time.UnixMilli(newCutoff).In(g.loc).Format("2006-01-02 15:04"),
		)
	}
}
