package us

import (
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

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"

	"jupitor/internal/domain"
	"jupitor/internal/gather"
	"jupitor/internal/store"
)

// ---------------------------------------------------------------------------
// Compile-time interface checks
// ---------------------------------------------------------------------------

var _ gather.Gatherer = (*DailyBarGatherer)(nil)
var _ gather.Gatherer = (*StreamGatherer)(nil)

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

	startDate string
	csvPath   string
	apiKey    string
	apiSecret string
	baseURL   string // live trading API for calendar
	log       *slog.Logger
}

// NewDailyBarGatherer creates a DailyBarGatherer configured with the given
// Alpaca credentials, target stores, and batch parameters.
func NewDailyBarGatherer(
	apiKey, apiSecret, dataURL string,
	barStore store.BarStore,
	tradeStore store.TradeStore,
	batchSize, maxWorkers int,
	tradeWorkers int,
	startDate, csvPath, baseURL string,
) *DailyBarGatherer {
	opts := marketdata.ClientOpts{
		APIKey:    apiKey,
		APISecret: apiSecret,
	}
	if dataURL != "" {
		opts.BaseURL = dataURL
	}

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

// tradeBackfillStep finds the latest universe date with missing trade files
// and processes it. Returns (true, nil) if work was done, (false, nil) if
// all dates are complete.
func (g *DailyBarGatherer) tradeBackfillStep(ctx context.Context) (bool, error) {
	universeDir := filepath.Join(g.dataDir(), "us", "universe")
	dates, err := ListUniverseDates(universeDir)
	if err != nil {
		return false, fmt.Errorf("listing universe dates: %w", err)
	}

	doneDir := filepath.Join(g.dataDir(), "us", "trades", ".done")

	for _, date := range dates {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		// Skip dates already fully processed.
		if _, err := os.Stat(filepath.Join(doneDir, date)); err == nil {
			continue
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

		g.log.Info("trade backfill",
			"date", date,
			"symbols", len(symbols),
		)

		count, err := g.ProcessTradeDay(ctx, dayTime, symbols)
		if err != nil {
			return true, fmt.Errorf("processing trade day %s: %w", date, err)
		}

		// Mark date as fully processed.
		if ctx.Err() == nil {
			os.MkdirAll(doneDir, 0o755)
			os.WriteFile(filepath.Join(doneDir, date), nil, 0o644)
		}

		g.log.Info("trade backfill done",
			"date", date,
			"symbols", len(symbols),
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

// ProcessTradeDay fetches trades for a single date using a worker pool.
// Symbols are sorted by trade_count descending and grouped into batches
// targeting ~100K trades each. Returns the total number of trades written.
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
				trades, err := g.fetchMultiTrades(ctx, batch, day)
				if err != nil {
					g.log.Error("trade fetch failed",
						"date", day.Format("2006-01-02"),
						"batch", fmt.Sprintf("%d/%d", batchIdx+1, totalBatches),
						"symbols", len(batch),
						"err", err,
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

// fetchMultiTrades fetches trades for multiple symbols for a single day.
// Only trades with size > 100 AND price * size >= 100 are returned.
func (g *DailyBarGatherer) fetchMultiTrades(ctx context.Context, symbols []string, day time.Time) ([]domain.Trade, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	nextDay := day.AddDate(0, 0, 1)

	multiTrades, err := g.client.GetMultiTrades(symbols, marketdata.GetTradesRequest{
		Start: day,
		End:   nextDay,
		Feed:  "sip",
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
					Timestamp:  t.Timestamp,
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
// StreamGatherer — live WebSocket streaming of trades from the Alpaca feed.
// ---------------------------------------------------------------------------

// StreamGatherer connects to the Alpaca real-time WebSocket feed and
// persists incoming trades.
type StreamGatherer struct {
	apiKey    string
	apiSecret string
	streamURL string
	store     store.TradeStore
	log       *slog.Logger
}

// NewStreamGatherer creates a StreamGatherer configured with the given
// Alpaca credentials, WebSocket URL, and target store.
func NewStreamGatherer(apiKey, apiSecret, streamURL string, s store.TradeStore) *StreamGatherer {
	return &StreamGatherer{
		apiKey:    apiKey,
		apiSecret: apiSecret,
		streamURL: streamURL,
		store:     s,
		log:       slog.Default().With("gatherer", "us-stream"),
	}
}

// Name returns the gatherer identifier.
func (g *StreamGatherer) Name() string { return "us-stream" }

// Run connects to the Alpaca WebSocket feed and streams trades into the
// store. It blocks until ctx is cancelled.
func (g *StreamGatherer) Run(ctx context.Context) error {
	// TODO: Implement WebSocket streaming from Alpaca.
	//  1. Dial streamURL and authenticate with apiKey/apiSecret.
	//  2. Subscribe to trade updates for configured symbols.
	//  3. Decode incoming messages into domain.Trade and write to g.store.
	//  4. Reconnect with backoff on transient errors.
	_ = ctx
	return fmt.Errorf("StreamGatherer.Run: not implemented")
}
