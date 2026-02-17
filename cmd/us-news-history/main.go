// One-shot tool: build historical news archive for top-traded ex-index stocks.
//
// For each trading day with consolidated stock-trades-ex-index data, fetches
// news from Alpaca, Google News RSS, GlobeNewswire RSS, and StockTwits for
// the top 100 most-traded symbols per tier (ACTIVE, MODERATE, SPORADIC).
// Stores individual articles/posts as parquet.
//
// StockTwits uses cursor-based pagination for the top 20 MODERATE and
// SPORADIC symbols to capture full trading-day history. Other symbols get
// a single page (latest ~30 posts).
//
// Usage:
//
//	go build -o bin/us-news-history ./cmd/us-news-history/
//	bin/us-news-history [-n 5] [-recent] [-force]
package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	alpacaapi "github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/parquet-go/parquet-go"

	"jupitor/internal/config"
	"jupitor/internal/dashboard"
	"jupitor/internal/news"
)

// NewsRecord is one article row in the output parquet file.
type NewsRecord struct {
	Symbol   string `parquet:"symbol"`
	Source   string `parquet:"source"`
	Time     int64  `parquet:"time,timestamp(millisecond)"`
	Headline string `parquet:"headline"`
	Content  string `parquet:"content"`
}

func main() {
	n := flag.Int("n", 0, "max number of dates to process (0 = all)")
	recent := flag.Bool("recent", false, "process most recent dates first")
	force := flag.Bool("force", false, "reprocess dates that already have news files")
	flag.Parse()

	cfgPath := "config/jupitor.yaml"
	if p := os.Getenv("JUPITOR_CONFIG"); p != "" {
		cfgPath = p
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	dataDir := cfg.Storage.DataDir

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Alpaca clients.
	apiKey := os.Getenv("APCA_API_KEY_ID")
	apiSecret := os.Getenv("APCA_API_SECRET_KEY")
	if apiKey == "" {
		log.Fatal("APCA_API_KEY_ID not set")
	}

	ac := alpacaapi.NewClient(alpacaapi.ClientOpts{
		APIKey:    apiKey,
		APISecret: apiSecret,
	})
	mdc := marketdata.NewClient(marketdata.ClientOpts{
		APIKey:    apiKey,
		APISecret: apiSecret,
	})

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatalf("loading ET timezone: %v", err)
	}

	// List all available dates.
	dates, err := dashboard.ListHistoryDates(dataDir)
	if err != nil {
		log.Fatalf("listing history dates: %v", err)
	}
	if len(dates) == 0 {
		slog.Info("no history dates found")
		return
	}

	// Filter out dates that already have news files (unless -force).
	newsDir := filepath.Join(dataDir, "us", "news")
	if err := os.MkdirAll(newsDir, 0o755); err != nil {
		log.Fatalf("creating news dir: %v", err)
	}

	var todo []string
	for _, d := range dates {
		if !*force {
			outPath := filepath.Join(newsDir, d+".parquet")
			if _, err := os.Stat(outPath); err == nil {
				continue // already done
			}
		}
		todo = append(todo, d)
	}

	if *recent {
		// Reverse so most recent first.
		for i, j := 0, len(todo)-1; i < j; i, j = i+1, j-1 {
			todo[i], todo[j] = todo[j], todo[i]
		}
	}
	if *n > 0 && len(todo) > *n {
		todo = todo[:*n]
	}

	slog.Info("news history backfill", "total_dates", len(dates), "todo", len(todo), "force", *force)
	if len(todo) == 0 {
		slog.Info("all dates already processed")
		return
	}

	// Build calendar cache for previous trading day lookups.
	// Fetch the full calendar range once.
	calStart, _ := time.ParseInLocation("2006-01-02", dates[0], loc)
	calEnd, _ := time.ParseInLocation("2006-01-02", dates[len(dates)-1], loc)
	calStart = calStart.AddDate(0, 0, -10) // buffer for first date
	cal, err := ac.GetCalendar(alpacaapi.GetCalendarRequest{Start: calStart, End: calEnd})
	if err != nil {
		log.Fatalf("fetching Alpaca calendar: %v", err)
	}
	prevTD := buildPrevTradingDayMap(cal)

	// Shared StockTwits rate limiter: 1 request per 500ms across all goroutines.
	stLimiter := time.NewTicker(500 * time.Millisecond)
	defer stLimiter.Stop()

	for i, date := range todo {
		slog.Info("processing date", "date", date, "progress", fmt.Sprintf("%d/%d", i+1, len(todo)))
		records, err := processDate(dataDir, date, prevTD[date], loc, mdc, stLimiter)
		if err != nil {
			slog.Error("failed to process date", "date", date, "error", err)
			continue
		}

		outPath := filepath.Join(newsDir, date+".parquet")
		if err := parquet.WriteFile(outPath, records); err != nil {
			slog.Error("writing parquet", "date", date, "error", err)
			continue
		}
		slog.Info("wrote news file", "date", date, "articles", len(records), "path", outPath)
	}
}

// buildPrevTradingDayMap builds a map from each calendar date to its previous
// trading day.
func buildPrevTradingDayMap(cal []alpacaapi.CalendarDay) map[string]string {
	m := make(map[string]string, len(cal))
	for i := 1; i < len(cal); i++ {
		m[cal[i].Date] = cal[i-1].Date
	}
	return m
}

// processDate loads trades for a date, picks top symbols, and fetches news.
func processDate(dataDir, date, prevDate string, loc *time.Location, mdc *marketdata.Client, stLimiter *time.Ticker) ([]NewsRecord, error) {
	// Load trades and tier map.
	trades, err := dashboard.LoadHistoryTrades(dataDir, date)
	if err != nil {
		return nil, fmt.Errorf("loading trades: %w", err)
	}
	tierMap, err := dashboard.LoadTierMapForDate(dataDir, date)
	if err != nil {
		return nil, fmt.Errorf("loading tier map: %w", err)
	}

	// Aggregate to get per-symbol trade counts.
	stats := dashboard.AggregateTrades(trades)

	// Group by tier, sorted by trade count descending.
	type symCount struct {
		sym    string
		trades int
	}
	tierSyms := map[string][]symCount{}
	for sym, s := range stats {
		tier, ok := tierMap[sym]
		if !ok {
			continue
		}
		tierSyms[tier] = append(tierSyms[tier], symCount{sym, s.Trades})
	}
	for tier := range tierSyms {
		ss := tierSyms[tier]
		sort.Slice(ss, func(i, j int) bool { return ss[i].trades > ss[j].trades })
		tierSyms[tier] = ss
	}

	// All symbols: top 100 per tier for news fetching.
	symbolSet := make(map[string]bool)
	for _, tier := range []string{"ACTIVE", "MODERATE", "SPORADIC"} {
		ss := tierSyms[tier]
		limit := 100
		if len(ss) < limit {
			limit = len(ss)
		}
		for _, sc := range ss[:limit] {
			symbolSet[sc.sym] = true
		}
	}

	// Deep StockTwits symbols: top 20 MODERATE + top 20 SPORADIC.
	deepSet := make(map[string]bool)
	for _, tier := range []string{"MODERATE", "SPORADIC"} {
		ss := tierSyms[tier]
		limit := 20
		if len(ss) < limit {
			limit = len(ss)
		}
		for _, sc := range ss[:limit] {
			deepSet[sc.sym] = true
		}
	}

	symbols := make([]string, 0, len(symbolSet))
	for sym := range symbolSet {
		symbols = append(symbols, sym)
	}
	sort.Strings(symbols)

	// Compute time window: prevDate 4PM ET → date 8PM ET.
	t, _ := time.ParseInLocation("2006-01-02", date, loc)
	end := time.Date(t.Year(), t.Month(), t.Day(), 20, 0, 0, 0, loc)
	var start time.Time
	if prevDate != "" {
		p, _ := time.ParseInLocation("2006-01-02", prevDate, loc)
		start = time.Date(p.Year(), p.Month(), p.Day(), 16, 0, 0, 0, loc)
	} else {
		start = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
	}

	slog.Info("fetching news", "date", date, "symbols", len(symbols), "deep_st", len(deepSet),
		"window", fmt.Sprintf("%s → %s", start.Format("2006-01-02 15:04"), end.Format("2006-01-02 15:04")))

	// Fetch news concurrently (8 goroutines).
	var mu sync.Mutex
	var records []NewsRecord
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup

	for _, sym := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Alpaca news.
			if articles, err := news.FetchAlpacaNews(mdc, sym, start, end); err == nil {
				mu.Lock()
				for _, a := range articles {
					records = append(records, NewsRecord{
						Symbol:   sym,
						Source:   a.Source,
						Time:     a.Time.UnixMilli(),
						Headline: a.Headline,
						Content:  a.Content,
					})
				}
				mu.Unlock()
			} else {
				slog.Debug("alpaca news error", "symbol", sym, "error", err)
			}

			// Google News RSS.
			if articles, err := news.FetchGoogleNews(sym, start, end); err == nil {
				mu.Lock()
				for _, a := range articles {
					records = append(records, NewsRecord{
						Symbol:   sym,
						Source:   a.Source,
						Time:     a.Time.UnixMilli(),
						Headline: a.Headline,
						Content:  a.Content,
					})
				}
				mu.Unlock()
			}

			// GlobeNewswire RSS.
			if articles, err := news.FetchGlobeNewswire(sym, start, end); err == nil {
				mu.Lock()
				for _, a := range articles {
					records = append(records, NewsRecord{
						Symbol:   sym,
						Source:   a.Source,
						Time:     a.Time.UnixMilli(),
						Headline: a.Headline,
						Content:  a.Content,
					})
				}
				mu.Unlock()
			}

			// StockTwits: paginate for deep symbols, single page for others.
			paginate := deepSet[sym]
			if articles, err := news.FetchStockTwits(sym, start, end, paginate, stLimiter); err == nil {
				mu.Lock()
				for _, a := range articles {
					records = append(records, NewsRecord{
						Symbol:   sym,
						Source:   a.Source,
						Time:     a.Time.UnixMilli(),
						Headline: a.Headline,
						Content:  a.Content,
					})
				}
				mu.Unlock()
			}
		}(sym)
	}
	wg.Wait()

	// Sort by symbol then time.
	sort.Slice(records, func(i, j int) bool {
		if records[i].Symbol != records[j].Symbol {
			return records[i].Symbol < records[j].Symbol
		}
		return records[i].Time < records[j].Time
	})

	return records, nil
}
