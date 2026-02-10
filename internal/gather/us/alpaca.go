package us

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
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
var _ gather.Gatherer = (*TradeGatherer)(nil)
var _ gather.Gatherer = (*StreamGatherer)(nil)

// ---------------------------------------------------------------------------
// DailyBarGatherer — brute-force daily OHLCV bars from the Alpaca API.
// ---------------------------------------------------------------------------

// DailyBarGatherer gathers daily bar data for US equities via the Alpaca
// market-data API. It tries every possible 1-4 character A-Z symbol
// combination plus 5+ char symbols from a CSV file.
type DailyBarGatherer struct {
	client     *marketdata.Client
	store      store.BarStore
	batchSize  int // symbols per API call (5000)
	maxWorkers int // concurrent goroutines (10)
	startDate  string
	csvPath    string
	apiKey     string
	apiSecret  string
	baseURL    string // live trading API for calendar
	log        *slog.Logger
}

// NewDailyBarGatherer creates a DailyBarGatherer configured with the given
// Alpaca credentials, target store, and batch parameters.
func NewDailyBarGatherer(apiKey, apiSecret, dataURL string, s store.BarStore, batchSize, maxWorkers int, startDate, csvPath, baseURL string) *DailyBarGatherer {
	opts := marketdata.ClientOpts{
		APIKey:    apiKey,
		APISecret: apiSecret,
	}
	if dataURL != "" {
		opts.BaseURL = dataURL
	}

	return &DailyBarGatherer{
		client:     marketdata.NewClient(opts),
		store:      s,
		batchSize:  batchSize,
		maxWorkers: maxWorkers,
		startDate:  startDate,
		csvPath:    csvPath,
		apiKey:     apiKey,
		apiSecret:  apiSecret,
		baseURL:    baseURL,
		log:        slog.Default().With("gatherer", "us-daily"),
	}
}

// Name returns the gatherer identifier.
func (g *DailyBarGatherer) Name() string { return "us-daily" }

// Run fetches daily bars for all US equity symbols from the Alpaca API using
// a three-phase approach: (1) update known symbols with only missing days,
// (2) discover new symbols via brute-force, (3) backfill full history for
// newly discovered symbols. This is resumable and idempotent within a day.
func (g *DailyBarGatherer) Run(ctx context.Context) error {
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
	dailyDir := filepath.Join(g.store.(*store.ParquetStore).DataDir, "us", "daily")
	tracker, err := newProgressTracker(dailyDir)
	if err != nil {
		return fmt.Errorf("creating progress tracker: %w", err)
	}
	defer tracker.Close()

	// 3. Check idempotency.
	if tracker.IsCompleted(endDateStr) {
		g.log.Info("already completed", "endDate", endDateStr)
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
	universeDir := filepath.Join(g.store.(*store.ParquetStore).DataDir, "us", "universe")
	universe := newUniverseWriter(universeDir)

	runStart := time.Now()

	// --- Phase 1: Update known symbols with missing days only ---
	known, err := g.store.ListSymbols(ctx, "us")
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

	g.log.Info("complete",
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
					if err := g.store.WriteBars(ctx, bars); err != nil {
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
// TradeGatherer — fetches historical trade (tick) data from the Alpaca API.
// ---------------------------------------------------------------------------

// TradeGatherer gathers historical trade data for US equities via the Alpaca
// market-data API.
type TradeGatherer struct {
	client          *marketdata.Client
	store           store.TradeStore
	batchSize       int
	rateLimitPerMin int
	startDate       string
	log             *slog.Logger
}

// NewTradeGatherer creates a TradeGatherer configured with the given Alpaca
// credentials, target store, and rate-limit parameters.
func NewTradeGatherer(apiKey, apiSecret, dataURL string, s store.TradeStore, batchSize, rateLimitPerMin int, startDate string) *TradeGatherer {
	opts := marketdata.ClientOpts{
		APIKey:    apiKey,
		APISecret: apiSecret,
	}
	if dataURL != "" {
		opts.BaseURL = dataURL
	}

	return &TradeGatherer{
		client:          marketdata.NewClient(opts),
		store:           s,
		batchSize:       batchSize,
		rateLimitPerMin: rateLimitPerMin,
		startDate:       startDate,
		log:             slog.Default().With("gatherer", "us-trade"),
	}
}

// Name returns the gatherer identifier.
func (g *TradeGatherer) Name() string { return "us-trade" }

// Run starts the historical trade gathering process. It blocks until ctx is
// cancelled.
func (g *TradeGatherer) Run(ctx context.Context) error {
	// TODO: Implement historical trade fetching from Alpaca API.
	//  1. List tradable symbols (or read from config/reference).
	//  2. For each symbol, paginate through GetTrades.
	//  3. Respect rateLimitPerMin and write batches to g.store.
	_ = ctx
	return fmt.Errorf("TradeGatherer.Run: not implemented")
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
