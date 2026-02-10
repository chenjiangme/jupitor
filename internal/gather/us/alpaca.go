package us

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

// Run fetches daily bars for all brute-force US equity symbols from the
// Alpaca API and writes them to the Parquet store. It is resumable and
// idempotent within a day.
func (g *DailyBarGatherer) Run(ctx context.Context) error {
	start, err := time.Parse("2006-01-02", g.startDate)
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
		// New day — stale .tried-empty, delete and start fresh.
		if err := tracker.Reset(); err != nil {
			return fmt.Errorf("resetting tracker: %w", err)
		}
	}
	// If lastCompleted is empty, this is first run or mid-day crash — keep .tried-empty as-is.

	// 5. Build skip set: tried-empty ∪ existing symbols.
	existing, err := g.store.ListSymbols(ctx, "us")
	if err != nil {
		return fmt.Errorf("listing existing symbols: %w", err)
	}
	skipSet := make(map[string]struct{}, len(existing))
	for _, sym := range existing {
		skipSet[sym] = struct{}{}
	}

	// 6. Generate all brute-force symbols, filter, shuffle.
	allSymbols, err := AllBruteSymbols(g.csvPath)
	if err != nil {
		return fmt.Errorf("generating symbols: %w", err)
	}

	var remaining []string
	for _, sym := range allSymbols {
		if _, skip := skipSet[sym]; skip {
			continue
		}
		if tracker.IsTriedEmpty(sym) {
			continue
		}
		remaining = append(remaining, sym)
	}

	totalSymbols := len(allSymbols)
	totalBatches := (len(remaining) + g.batchSize - 1) / max(g.batchSize, 1)

	g.log.Info("starting us-daily",
		"endDate", endDateStr,
		"total", totalSymbols,
		"remaining", len(remaining),
		"batches", totalBatches,
	)

	if len(remaining) == 0 {
		if err := tracker.MarkCompleted(endDateStr); err != nil {
			return fmt.Errorf("marking completed: %w", err)
		}
		g.log.Info("no remaining symbols to process")
		return nil
	}

	// 7. Split into batches.
	var batches [][]string
	for i := 0; i < len(remaining); i += g.batchSize {
		end := min(i+g.batchSize, len(remaining))
		batches = append(batches, remaining[i:end])
	}

	// 8. Set up universe writer.
	universeDir := filepath.Join(g.store.(*store.ParquetStore).DataDir, "us", "universe")
	universe := newUniverseWriter(universeDir)

	// 9. Feed batches to workers.
	batchCh := make(chan int, len(batches))
	for i := range batches {
		batchCh <- i
	}
	close(batchCh)

	var (
		wg        sync.WaitGroup
		totalHits atomic.Int64
		totalMiss atomic.Int64
		runStart  = time.Now()
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
				bars, err := g.fetchMultiBars(ctx, batch, start, endDate)
				if err != nil {
					g.log.Error("batch fetch failed",
						"batch", fmt.Sprintf("%d/%d", batchIdx+1, totalBatches),
						"err", err,
					)
					continue
				}

				// Determine hits and misses.
				hitSymbols := make(map[string]struct{})
				for _, b := range bars {
					hitSymbols[b.Symbol] = struct{}{}
				}

				var emptySymbols []string
				for _, sym := range batch {
					if _, hit := hitSymbols[sym]; !hit {
						emptySymbols = append(emptySymbols, sym)
					}
				}

				// Write bars to store.
				if len(bars) > 0 {
					if err := g.store.WriteBars(ctx, bars); err != nil {
						g.log.Error("writing bars failed", "err", err)
						continue
					}
					universe.AddBars(bars)
					if err := universe.Flush(); err != nil {
						g.log.Error("flushing universe failed", "err", err)
					}
				}

				// Mark empty symbols.
				if len(emptySymbols) > 0 {
					if err := tracker.MarkEmpty(emptySymbols); err != nil {
						g.log.Error("marking empty failed", "err", err)
					}
				}

				hits := int64(len(hitSymbols))
				misses := int64(len(emptySymbols))
				totalHits.Add(hits)
				totalMiss.Add(misses)

				g.log.Info("batch done",
					"batch", fmt.Sprintf("%d/%d", batchIdx+1, totalBatches),
					"hits", hits,
					"empty", misses,
					"elapsed", time.Since(runStart).Round(time.Second),
				)
			}
		}()
	}

	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// 10. Finalize universe files.
	if err := universe.Finalize(); err != nil {
		return fmt.Errorf("finalizing universe: %w", err)
	}

	// 11. Mark completed.
	if err := tracker.MarkCompleted(endDateStr); err != nil {
		return fmt.Errorf("marking completed: %w", err)
	}

	g.log.Info("complete",
		"hits", totalHits.Load(),
		"empty", totalMiss.Load(),
		"elapsed", time.Since(runStart).Round(time.Second),
	)
	return nil
}

// fetchMultiBars fetches daily bars for multiple symbols in a single API call.
func (g *DailyBarGatherer) fetchMultiBars(ctx context.Context, symbols []string, start, end time.Time) ([]domain.Bar, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	multiBars, err := g.client.GetMultiBars(symbols, marketdata.GetBarsRequest{
		TimeFrame: marketdata.OneDay,
		Start:     start,
		End:       end,
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
