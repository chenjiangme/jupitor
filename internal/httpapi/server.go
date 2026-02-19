package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
	"github.com/parquet-go/parquet-go"

	"jupitor/internal/dashboard"
	us "jupitor/internal/gather/us"
	"jupitor/internal/live"
	"jupitor/internal/news"
	"jupitor/internal/store"
	"jupitor/internal/tradeparams"
)

// NewsRecord matches the parquet schema in us-news-history.
type NewsRecord struct {
	Symbol   string `parquet:"symbol"`
	Source   string `parquet:"source"`
	Time     int64  `parquet:"time,timestamp(millisecond)"`
	Headline string `parquet:"headline"`
	Content  string `parquet:"content"`
}

// DashboardServer serves the dashboard HTTP API.
type DashboardServer struct {
	model   *live.LiveModel
	dataDir string
	loc     *time.Location
	log     *slog.Logger

	// Tier map (latest, loaded once at startup).
	tierMap map[string]string

	// History dates (loaded once, refreshed by news history backfill).
	historyMu    sync.RWMutex
	historyDates []string

	// Alpaca client for watchlist (nil if not configured).
	alpacaClient *alpacaapi.Client
	watchlistMu  sync.RWMutex
	watchlistIDs map[string]string // date -> Alpaca watchlist ID

	// Alpaca marketdata client for news (nil if not configured).
	mdClient *marketdata.Client

	// Background news cache: "SYMBOL:DATE" -> []NewsArticleJSON
	newsCache sync.Map
	// StockTwits rate limiter for background news refresh.
	stLimiter *time.Ticker
	// Accumulated set of symbols that ever appeared on the dashboard for today.
	newsSeenMu      sync.Mutex
	newsSeenDate    string
	newsSeenSymbols map[string]bool

	// Cache for per-symbol per-date history stats. Key: "SYMBOL:DATE".
	symbolHistoryCache sync.Map

	// Trade parameters (targets, etc.) with pub/sub for SSE push.
	tradeParams *tradeparams.Store

	// Reference data directory for trade-universe generation.
	refDir string

	// Replay cache: date -> sorted trades + tier map.
	replayMu    sync.RWMutex
	replayCache map[string][]store.TradeRecord
	replayTier  map[string]map[string]string
}

// NewDashboardServer creates a new dashboard HTTP server.
func NewDashboardServer(
	model *live.LiveModel,
	dataDir string,
	loc *time.Location,
	log *slog.Logger,
	tierMap map[string]string,
	historyDates []string,
	alpacaClient *alpacaapi.Client,
	mdClient *marketdata.Client,
	tradeParams *tradeparams.Store,
	refDir string,
) *DashboardServer {
	s := &DashboardServer{
		model:        model,
		dataDir:      dataDir,
		loc:          loc,
		log:          log,
		tierMap:      tierMap,
		historyDates: historyDates,
		alpacaClient: alpacaClient,
		watchlistIDs: make(map[string]string),
		mdClient:     mdClient,
		stLimiter:    time.NewTicker(500 * time.Millisecond),
		tradeParams:  tradeParams,
		refDir:       refDir,
		replayCache:  make(map[string][]store.TradeRecord),
		replayTier:   make(map[string]map[string]string),
	}

	return s
}

// Start launches background goroutines (news refresh, history backfill). Call
// this after creating the server, tied to the daemon's context for graceful shutdown.
func (s *DashboardServer) Start(ctx context.Context) {
	go s.startNewsRefresh(ctx)
	go s.startNewsHistoryBackfill(ctx)
}

// getHistoryDates returns a snapshot of the history dates slice (thread-safe).
func (s *DashboardServer) getHistoryDates() []string {
	s.historyMu.RLock()
	defer s.historyMu.RUnlock()
	return s.historyDates
}

// newsCacheFile returns the path to the news cache JSON file for a date.
func newsCacheFile(date string) string {
	return fmt.Sprintf("/tmp/us-stream-news-%s.json", date)
}

// loadNewsFromDisk loads the persisted news cache for a date into memory.
func (s *DashboardServer) loadNewsFromDisk(date string) int {
	data, err := os.ReadFile(newsCacheFile(date))
	if err != nil {
		return 0
	}
	var cached map[string][]NewsArticleJSON
	if err := json.Unmarshal(data, &cached); err != nil {
		s.log.Warn("loading news cache", "error", err)
		return 0
	}
	count := 0
	for sym, articles := range cached {
		key := sym + ":" + date
		s.newsCache.Store(key, articles)
		count += len(articles)
	}
	return count
}

// saveNewsToDisk persists the in-memory news cache for a date to disk.
func (s *DashboardServer) saveNewsToDisk(date string) {
	cached := make(map[string][]NewsArticleJSON)
	s.newsCache.Range(func(k, v any) bool {
		key := k.(string)
		// Keys are "SYMBOL:DATE" — only save entries for this date.
		if idx := strings.LastIndex(key, ":"); idx > 0 && key[idx+1:] == date {
			sym := key[:idx]
			cached[sym] = v.([]NewsArticleJSON)
		}
		return true
	})
	data, err := json.Marshal(cached)
	if err != nil {
		s.log.Error("marshalling news cache", "error", err)
		return
	}
	if err := os.WriteFile(newsCacheFile(date), data, 0644); err != nil {
		s.log.Error("writing news cache", "error", err)
	}
}

// startNewsRefresh periodically fetches news from all sources for today's top
// symbols and caches the results. Runs every 5 minutes.
func (s *DashboardServer) startNewsRefresh(ctx context.Context) {
	// Load persisted cache for today before fetching.
	today := time.Now().In(s.loc).Format("2006-01-02")
	if n := s.loadNewsFromDisk(today); n > 0 {
		s.log.Info("loaded news cache from disk", "date", today, "articles", n)
	}

	// Run immediately on startup, then every 5 minutes.
	s.refreshNewsCache()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	defer s.stLimiter.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshNewsCache()
		}
	}
}

// refreshNewsCache fetches news for all dashboard symbols from all 4 sources.
// Uses the same ComputeDayData logic as the dashboard endpoint so the symbol
// set matches what the bubble chart shows (session-aware filterTopN).
// Symbols are accumulated across refresh cycles: once a stock appears on the
// dashboard it stays in the refresh set for the rest of the day.
func (s *DashboardServer) refreshNewsCache() {
	if s.mdClient == nil {
		return
	}

	now := time.Now().In(s.loc)
	date := now.Format("2006-01-02")

	_, todayExIdx := s.model.TodaySnapshot()
	if len(todayExIdx) == 0 {
		s.log.Debug("news refresh: no trades yet")
		return
	}

	// Compute dashboard the same way as handleDashboard to get exact bubble chart symbols.
	todayOpen930 := time.Date(now.Year(), now.Month(), now.Day(), 9, 30, 0, 0, s.loc).UnixMilli()
	_, off := now.Zone()
	todayOpen930ET := todayOpen930 + int64(off)*1000
	todayData := dashboard.ComputeDayData("TODAY", todayExIdx, s.tierMap, todayOpen930ET, dashboard.SortPreTrades)

	symbolSet := make(map[string]bool)
	for _, tier := range todayData.Tiers {
		for _, cs := range tier.Symbols {
			symbolSet[cs.Symbol] = true
		}
	}

	// Include NEXT session symbols.
	_, nextExIdx := s.model.NextSnapshot()
	if len(nextExIdx) > 0 {
		nextOpen930ET := todayOpen930ET + 24*60*60*1000
		nextData := dashboard.ComputeDayData("NEXT", nextExIdx, s.tierMap, nextOpen930ET, dashboard.SortPreTrades)
		for _, tier := range nextData.Tiers {
			for _, cs := range tier.Symbols {
				symbolSet[cs.Symbol] = true
			}
		}
	}

	// Accumulate "ever seen" symbols for this date; track new arrivals.
	s.newsSeenMu.Lock()
	if s.newsSeenDate != date {
		s.newsSeenSymbols = make(map[string]bool)
		s.newsSeenDate = date
	}
	var newSymbols []string
	for sym := range symbolSet {
		if !s.newsSeenSymbols[sym] {
			newSymbols = append(newSymbols, sym)
		}
		s.newsSeenSymbols[sym] = true
	}
	// Copy the full accumulated set.
	allSymbols := make(map[string]bool, len(s.newsSeenSymbols))
	for sym := range s.newsSeenSymbols {
		allSymbols[sym] = true
	}
	s.newsSeenMu.Unlock()

	// Prioritize newly appeared symbols first, then the rest alphabetically.
	sort.Strings(newSymbols)
	newSet := make(map[string]bool, len(newSymbols))
	for _, sym := range newSymbols {
		newSet[sym] = true
	}
	var rest []string
	for sym := range allSymbols {
		if !newSet[sym] {
			rest = append(rest, sym)
		}
	}
	sort.Strings(rest)
	symbols := make([]string, 0, len(newSymbols)+len(rest))
	symbols = append(symbols, newSymbols...)
	symbols = append(symbols, rest...)

	if len(symbols) == 0 {
		return
	}

	// Time window: prev trading day 4PM ET → today 8PM ET.
	t, _ := time.ParseInLocation("2006-01-02", date, s.loc)
	end := time.Date(t.Year(), t.Month(), t.Day(), 20, 0, 0, 0, s.loc)
	// Find previous trading day from history dates.
	histDates := s.getHistoryDates()
	start := time.Date(t.Year(), t.Month(), t.Day(), 4, 0, 0, 0, s.loc) // fallback
	for i := len(histDates) - 1; i >= 0; i-- {
		if histDates[i] < date {
			p, _ := time.ParseInLocation("2006-01-02", histDates[i], s.loc)
			start = time.Date(p.Year(), p.Month(), p.Day(), 16, 0, 0, 0, s.loc)
			break
		}
	}

	s.log.Info("news refresh starting", "date", date, "symbols", len(symbols), "new", len(newSymbols))

	// Fetch concurrently (4 workers).
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup

	var totalArticles int64
	for _, sym := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			var articles []NewsArticleJSON
			appendAll := func(aa []news.Article) {
				for _, a := range aa {
					articles = append(articles, NewsArticleJSON{
						Time:     a.Time.UnixMilli(),
						Source:   a.Source,
						Headline: a.Headline,
						Content:  a.Content,
					})
				}
			}

			// Alpaca news.
			if aa, err := news.FetchAlpacaNews(s.mdClient, sym, start, end); err == nil {
				appendAll(aa)
			} else {
				s.log.Debug("news fetch error", "source", "alpaca", "symbol", sym, "error", err)
			}

			// Google News RSS.
			if aa, err := news.FetchGoogleNews(sym, start, end); err == nil {
				appendAll(aa)
			} else {
				s.log.Debug("news fetch error", "source", "google", "symbol", sym, "error", err)
			}

			// GlobeNewswire RSS.
			if aa, err := news.FetchGlobeNewswire(sym, start, end); err == nil {
				appendAll(aa)
			} else {
				s.log.Debug("news fetch error", "source", "globenewswire", "symbol", sym, "error", err)
			}

			// StockTwits (paginate to get all messages in the window).
			if aa, err := news.FetchStockTwits(sym, start, end, true, s.stLimiter); err == nil {
				appendAll(aa)
			} else {
				s.log.Debug("news fetch error", "source", "stocktwits", "symbol", sym, "error", err)
			}

			// Merge with existing cached articles (keep old articles from sources
			// that may have failed this cycle, and deduplicate by time+source).
			key := sym + ":" + date
			seen := make(map[string]bool, len(articles))
			for _, a := range articles {
				seen[fmt.Sprintf("%d:%s", a.Time, a.Source)] = true
			}
			if old, ok := s.newsCache.Load(key); ok {
				for _, a := range old.([]NewsArticleJSON) {
					k := fmt.Sprintf("%d:%s", a.Time, a.Source)
					if !seen[k] {
						articles = append(articles, a)
						seen[k] = true
					}
				}
			}

			// Sort by time.
			sort.Slice(articles, func(i, j int) bool {
				return articles[i].Time < articles[j].Time
			})

			s.newsCache.Store(key, articles)
			atomic.AddInt64(&totalArticles, int64(len(articles)))
		}(sym)
	}
	wg.Wait()

	s.log.Info("news refresh complete", "date", date, "symbols", len(symbols), "articles", totalArticles)

	// Persist to disk for fast restart.
	s.saveNewsToDisk(date)
}

// fetchNewsOnDemand fetches news for a single symbol on demand (cache miss).
// Uses single-page StockTwits (no deep pagination) for fast response.
func (s *DashboardServer) fetchNewsOnDemand(symbol, date string) []NewsArticleJSON {
	// Compute time window: prev trading day 4PM ET → date 8PM ET.
	t, _ := time.ParseInLocation("2006-01-02", date, s.loc)
	end := time.Date(t.Year(), t.Month(), t.Day(), 20, 0, 0, 0, s.loc)
	histDates := s.getHistoryDates()
	start := time.Date(t.Year(), t.Month(), t.Day(), 4, 0, 0, 0, s.loc)
	for i := len(histDates) - 1; i >= 0; i-- {
		if histDates[i] < date {
			p, _ := time.ParseInLocation("2006-01-02", histDates[i], s.loc)
			start = time.Date(p.Year(), p.Month(), p.Day(), 16, 0, 0, 0, s.loc)
			break
		}
	}

	var articles []NewsArticleJSON
	appendAll := func(aa []news.Article) {
		for _, a := range aa {
			articles = append(articles, NewsArticleJSON{
				Time:     a.Time.UnixMilli(),
				Source:   a.Source,
				Headline: a.Headline,
				Content:  a.Content,
			})
		}
	}

	if s.mdClient != nil {
		if aa, err := news.FetchAlpacaNews(s.mdClient, symbol, start, end); err == nil {
			appendAll(aa)
		}
	}
	if aa, err := news.FetchGoogleNews(symbol, start, end); err == nil {
		appendAll(aa)
	}
	if aa, err := news.FetchGlobeNewswire(symbol, start, end); err == nil {
		appendAll(aa)
	}
	// Single-page StockTwits fetch (no deep pagination for fast response).
	limiter := time.NewTicker(time.Millisecond)
	if aa, err := news.FetchStockTwits(symbol, start, end, false, limiter); err == nil {
		appendAll(aa)
	}
	limiter.Stop()

	sort.Slice(articles, func(i, j int) bool {
		return articles[i].Time < articles[j].Time
	})
	if articles == nil {
		articles = []NewsArticleJSON{}
	}

	key := symbol + ":" + date
	s.newsCache.Store(key, articles)
	s.log.Info("news on-demand fetch", "symbol", symbol, "date", date, "articles", len(articles))
	return articles
}

// fillIndexFileGaps copies the latest available SPX/NDX index files to dates
// that have universe files but no index files. Returns the number of dates filled.
// This ensures GenerateTradeUniverse can proceed for recent dates even if the
// Python us_index_data script hasn't been run.
func (s *DashboardServer) fillIndexFileGaps() int {
	universeDir := filepath.Join(s.dataDir, "us", "universe")
	spxDir := filepath.Join(s.dataDir, "us", "index", "spx")
	ndxDir := filepath.Join(s.dataDir, "us", "index", "ndx")

	// List universe dates.
	entries, err := os.ReadDir(universeDir)
	if err != nil {
		return 0
	}
	var universeDates []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".txt") {
			date := strings.TrimSuffix(name, ".txt")
			if len(date) == 10 && date[4] == '-' && date[7] == '-' {
				universeDates = append(universeDates, date)
			}
		}
	}
	sort.Strings(universeDates)

	// Find latest available SPX/NDX files.
	latestSPX := latestDateTxt(spxDir)
	latestNDX := latestDateTxt(ndxDir)
	if latestSPX == "" || latestNDX == "" {
		return 0
	}

	filled := 0
	for _, date := range universeDates {
		spxPath := filepath.Join(spxDir, date+".txt")
		ndxPath := filepath.Join(ndxDir, date+".txt")

		spxExists := fileExistsCheck(spxPath)
		ndxExists := fileExistsCheck(ndxPath)
		if spxExists && ndxExists {
			// Update latest available for future copies.
			latestSPX = spxPath
			latestNDX = ndxPath
			continue
		}

		// Copy from latest available.
		if !spxExists {
			if err := copyFile(latestSPX, spxPath); err != nil {
				s.log.Warn("copying SPX index file", "date", date, "error", err)
				continue
			}
		}
		if !ndxExists {
			if err := copyFile(latestNDX, ndxPath); err != nil {
				s.log.Warn("copying NDX index file", "date", date, "error", err)
				continue
			}
		}
		latestSPX = spxPath
		latestNDX = ndxPath
		filled++
	}
	return filled
}

// latestDateTxt returns the path to the latest YYYY-MM-DD.txt file in dir.
func latestDateTxt(dir string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	var latest string
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".txt") {
			date := strings.TrimSuffix(name, ".txt")
			if len(date) == 10 && date[4] == '-' && date[7] == '-' && name > latest {
				latest = name
			}
		}
	}
	if latest == "" {
		return ""
	}
	return filepath.Join(dir, latest)
}

func fileExistsCheck(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o644)
}

// startNewsHistoryBackfill runs the automated history pipeline in the background.
// Checks every 30 minutes for new data to process.
func (s *DashboardServer) startNewsHistoryBackfill(ctx context.Context) {

	// Wait for live system to settle before starting history backfill.
	select {
	case <-ctx.Done():
		return
	case <-time.After(2 * time.Minute):
	}

	s.runHistoryPipeline(ctx)

	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runHistoryPipeline(ctx)
		}
	}
}

// runHistoryPipeline runs the full automated pipeline:
//  1. Fill SPX/NDX index file gaps (copy latest to dates that have universe but no index files)
//  2. Generate trade-universe CSVs (needs universe + index + daily bars)
//  3. Generate stock-trades-ex-index (needs trade-universe + per-symbol trades)
//  4. Backfill news for dates with stock-trades-ex-index but no news parquet
//
// Also refreshes the server's historyDates list.
func (s *DashboardServer) runHistoryPipeline(ctx context.Context) {
	// Step 1: Fill index file gaps so trade-universe generation can proceed.
	if filled := s.fillIndexFileGaps(); filled > 0 {
		s.log.Info("filled index file gaps", "dates", filled)
	}

	// Step 2: Generate trade-universe CSVs for new dates.
	if s.refDir != "" {
		ref := us.LoadReferenceData(s.refDir)
		if wrote, err := us.GenerateTradeUniverse(ctx, s.dataDir, ref, s.log); err != nil {
			s.log.Warn("auto trade-universe generation", "error", err)
		} else if wrote {
			s.log.Info("auto trade-universe generation complete")
		}
	}

	// Step 3: Generate stock-trades-ex-index for recent dates (limit to latest 10).
	if wrote, err := us.GenerateStockTrades(ctx, s.dataDir, 10, true, s.log); err != nil {
		s.log.Warn("auto stock-trades-ex-index generation", "error", err)
	} else if wrote > 0 {
		s.log.Info("auto stock-trades-ex-index generation complete", "files", wrote)
	}

	// Re-list history dates to pick up newly generated files.
	dates, err := dashboard.ListHistoryDates(s.dataDir)
	if err != nil {
		s.log.Warn("history pipeline: listing dates", "error", err)
		return
	}

	// Update the server's history dates list.
	s.historyMu.Lock()
	s.historyDates = dates
	s.historyMu.Unlock()

	// Step 4: Backfill news for dates missing news parquet files.
	newsDir := filepath.Join(s.dataDir, "us", "news")
	os.MkdirAll(newsDir, 0o755)

	var todo []string
	for _, d := range dates {
		outPath := filepath.Join(newsDir, d+".parquet")
		if _, err := os.Stat(outPath); err == nil {
			continue // already done
		}
		todo = append(todo, d)
	}

	if len(todo) == 0 {
		return
	}

	// Process most recent first.
	for i, j := 0, len(todo)-1; i < j; i, j = i+1, j-1 {
		todo[i], todo[j] = todo[j], todo[i]
	}

	s.log.Info("news history backfill starting", "total_dates", len(dates), "todo", len(todo))

	for i, date := range todo {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Find previous trading day from history dates.
		prevDate := ""
		for j, d := range dates {
			if d == date && j > 0 {
				prevDate = dates[j-1]
				break
			}
		}

		s.log.Info("news history backfill: processing", "date", date, "progress", fmt.Sprintf("%d/%d", i+1, len(todo)))

		records, err := s.processNewsHistoryDate(ctx, date, prevDate)
		if err != nil {
			s.log.Error("news history backfill failed", "date", date, "error", err)
			continue
		}

		outPath := filepath.Join(newsDir, date+".parquet")
		if err := parquet.WriteFile(outPath, records); err != nil {
			s.log.Error("news history backfill: writing parquet", "date", date, "error", err)
			continue
		}

		s.log.Info("news history backfill complete", "date", date, "articles", len(records))
	}
}

// processNewsHistoryDate loads trades for a date, picks top symbols per tier,
// and fetches news from all 4 sources. Same logic as cmd/us-news-history.
func (s *DashboardServer) processNewsHistoryDate(ctx context.Context, date, prevDate string) ([]NewsRecord, error) {
	trades, err := dashboard.LoadHistoryTrades(s.dataDir, date)
	if err != nil {
		return nil, fmt.Errorf("loading trades: %w", err)
	}
	tierMap, err := dashboard.LoadTierMapForDate(s.dataDir, date)
	if err != nil {
		return nil, fmt.Errorf("loading tier map: %w", err)
	}

	stats := dashboard.AggregateTrades(trades)

	// Group by tier, sorted by trade count descending.
	type symCount struct {
		sym    string
		trades int
	}
	tierSyms := map[string][]symCount{}
	for sym, st := range stats {
		tier, ok := tierMap[sym]
		if !ok {
			continue
		}
		tierSyms[tier] = append(tierSyms[tier], symCount{sym, st.Trades})
	}
	for tier := range tierSyms {
		ss := tierSyms[tier]
		sort.Slice(ss, func(i, j int) bool { return ss[i].trades > ss[j].trades })
		tierSyms[tier] = ss
	}

	// Top 100 per tier for news fetching.
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

	// Deep StockTwits: top 20 MODERATE + SPORADIC.
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

	// Time window: prevDate 4PM ET → date 8PM ET.
	t, _ := time.ParseInLocation("2006-01-02", date, s.loc)
	end := time.Date(t.Year(), t.Month(), t.Day(), 20, 0, 0, 0, s.loc)
	var start time.Time
	if prevDate != "" {
		p, _ := time.ParseInLocation("2006-01-02", prevDate, s.loc)
		start = time.Date(p.Year(), p.Month(), p.Day(), 16, 0, 0, 0, s.loc)
	} else {
		start = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, s.loc)
	}

	s.log.Info("news history: fetching", "date", date, "symbols", len(symbols), "deep_st", len(deepSet))

	// Fetch concurrently (8 workers).
	var mu sync.Mutex
	var records []NewsRecord
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup

	for _, sym := range symbols {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			appendAll := func(aa []news.Article) {
				mu.Lock()
				for _, a := range aa {
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

			if aa, err := news.FetchAlpacaNews(s.mdClient, sym, start, end); err == nil {
				appendAll(aa)
			}
			if aa, err := news.FetchGoogleNews(sym, start, end); err == nil {
				appendAll(aa)
			}
			if aa, err := news.FetchGlobeNewswire(sym, start, end); err == nil {
				appendAll(aa)
			}
			paginate := deepSet[sym]
			if aa, err := news.FetchStockTwits(sym, start, end, paginate, s.stLimiter); err == nil {
				appendAll(aa)
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

// resolveWatchlistID returns the Alpaca watchlist ID for the given date,
// creating the watchlist on demand. Watchlists are named "jupitor-YYYY-MM-DD".
func (s *DashboardServer) resolveWatchlistID(date string) (string, error) {
	name := "jupitor-" + date

	// Fast path: check cache.
	s.watchlistMu.RLock()
	if id, ok := s.watchlistIDs[date]; ok {
		s.watchlistMu.RUnlock()
		return id, nil
	}
	s.watchlistMu.RUnlock()

	// Slow path: write lock, double-check, then fetch from API.
	s.watchlistMu.Lock()
	defer s.watchlistMu.Unlock()

	if id, ok := s.watchlistIDs[date]; ok {
		return id, nil
	}

	// Fetch all watchlists and cache jupitor-* entries.
	lists, err := s.alpacaClient.GetWatchlists()
	if err != nil {
		return "", fmt.Errorf("listing watchlists: %w", err)
	}
	for _, w := range lists {
		if strings.HasPrefix(w.Name, "jupitor-") {
			d := strings.TrimPrefix(w.Name, "jupitor-")
			s.watchlistIDs[d] = w.ID
		}
	}
	if id, ok := s.watchlistIDs[date]; ok {
		return id, nil
	}

	// Not found — create it.
	w, err := s.alpacaClient.CreateWatchlist(alpacaapi.CreateWatchlistRequest{Name: name})
	if err != nil {
		// Possibly hit 200 watchlist limit — prune 5 oldest jupitor-* and retry.
		s.pruneOldestWatchlists(lists, 5)
		w, err = s.alpacaClient.CreateWatchlist(alpacaapi.CreateWatchlistRequest{Name: name})
		if err != nil {
			return "", fmt.Errorf("creating watchlist %s: %w", name, err)
		}
	}
	s.watchlistIDs[date] = w.ID
	s.log.Info("watchlist created", "name", name, "id", w.ID)
	return w.ID, nil
}

// pruneOldestWatchlists deletes the N oldest jupitor-* watchlists by date.
func (s *DashboardServer) pruneOldestWatchlists(lists []alpacaapi.Watchlist, n int) {
	var dated []alpacaapi.Watchlist
	for _, w := range lists {
		if strings.HasPrefix(w.Name, "jupitor-") {
			dated = append(dated, w)
		}
	}
	sort.Slice(dated, func(i, j int) bool {
		return dated[i].Name < dated[j].Name
	})
	if len(dated) < n {
		n = len(dated)
	}
	for i := 0; i < n; i++ {
		if err := s.alpacaClient.DeleteWatchlist(dated[i].ID); err != nil {
			s.log.Warn("pruning watchlist", "name", dated[i].Name, "error", err)
		} else {
			d := strings.TrimPrefix(dated[i].Name, "jupitor-")
			delete(s.watchlistIDs, d)
			s.log.Info("pruned watchlist", "name", dated[i].Name)
		}
	}
}

// RegisterRoutes registers all API routes on the given mux.
func (s *DashboardServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/dashboard", s.handleDashboard)
	mux.HandleFunc("GET /api/dashboard/replay", s.handleReplay)
	mux.HandleFunc("GET /api/dashboard/history/{date}", s.handleHistory)
	mux.HandleFunc("GET /api/dates", s.handleDates)
	mux.HandleFunc("GET /api/watchlist", s.handleGetWatchlist)
	mux.HandleFunc("PUT /api/watchlist/{symbol}", s.handleAddWatchlist)
	mux.HandleFunc("DELETE /api/watchlist/{symbol}", s.handleRemoveWatchlist)
	mux.HandleFunc("GET /api/news/{symbol}", s.handleNews)
	mux.HandleFunc("GET /api/symbol-history/{symbol}", s.handleSymbolHistory)
	mux.HandleFunc("GET /api/targets", s.handleGetTargets)
	mux.HandleFunc("PUT /api/targets", s.handleSetTarget)
	mux.HandleFunc("DELETE /api/targets", s.handleDeleteTarget)
	mux.HandleFunc("GET /api/targets/stream", s.handleTargetStream)
}

// Handler returns an http.Handler with CORS middleware.
func (s *DashboardServer) Handler() http.Handler {
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	return corsMiddleware(mux)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encoding JSON response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// parseSortMode extracts the sort mode from the "sort" query param.
func parseSortMode(r *http.Request) int {
	s := r.URL.Query().Get("sort")
	if s == "" {
		return dashboard.SortPreTrades
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || n >= dashboard.SortModeCount {
		return dashboard.SortPreTrades
	}
	return n
}

// open930ET computes 9:30 AM ET as ET-shifted milliseconds for a date.
func open930ET(date string, loc *time.Location) int64 {
	t, _ := time.ParseInLocation("2006-01-02", date, loc)
	open930 := time.Date(t.Year(), t.Month(), t.Day(), 9, 30, 0, 0, loc)
	_, off := open930.Zone()
	return open930.UnixMilli() + int64(off)*1000
}

// postMarketStartET returns 4PM ET on the given date as ET-shifted milliseconds.
func postMarketStartET(date string) int64 {
	t, _ := time.Parse("2006-01-02", date)
	return time.Date(t.Year(), t.Month(), t.Day(), 16, 0, 0, 0, time.UTC).UnixMilli()
}

// postMarketEndET returns 8PM ET on the given date as ET-shifted milliseconds.
func postMarketEndET(date string) int64 {
	t, _ := time.Parse("2006-01-02", date)
	return time.Date(t.Year(), t.Month(), t.Day(), 20, 0, 0, 0, time.UTC).UnixMilli()
}

// nextDateFor returns the next history date after the given date, or "".
func (s *DashboardServer) nextDateFor(date string) string {
	histDates := s.getHistoryDates()
	for i, d := range histDates {
		if d == date && i+1 < len(histDates) {
			return histDates[i+1]
		}
	}
	return ""
}

// computeNewsCounts returns per-symbol news counts from the in-memory cache.
// StockTwits messages are bucketed by ET session; other sources counted as news.
// Only messages whose ET date matches `date` are counted.
func (s *DashboardServer) computeNewsCounts(date string) map[string]*SymbolNewsCounts {
	result := make(map[string]*SymbolNewsCounts)
	suffix := ":" + date
	s.newsCache.Range(func(k, v any) bool {
		key := k.(string)
		if !strings.HasSuffix(key, suffix) {
			return true
		}
		sym := key[:len(key)-len(suffix)]
		articles := v.([]NewsArticleJSON)
		nc := &SymbolNewsCounts{}
		for _, a := range articles {
			t := time.UnixMilli(a.Time).In(s.loc)
			if t.Format("2006-01-02") != date {
				continue
			}
			minutes := t.Hour()*60 + t.Minute()
			if a.Source == "stocktwits" {
				if minutes < 570 { // before 9:30 AM
					nc.StPre++
				} else if minutes < 960 { // before 4 PM
					nc.StReg++
				} else {
					nc.StPost++
				}
			} else {
				nc.News++
			}
		}
		result[sym] = nc
		return true
	})
	return result
}

// loadNewsCounts reads the news parquet file for a date and returns per-symbol counts.
func (s *DashboardServer) loadNewsCounts(date string) map[string]*SymbolNewsCounts {
	path := filepath.Join(s.dataDir, "us", "news", date+".parquet")
	records, err := parquet.ReadFile[NewsRecord](path)
	if err != nil {
		return nil
	}
	result := make(map[string]*SymbolNewsCounts)
	for i := range records {
		r := &records[i]
		nc := result[r.Symbol]
		if nc == nil {
			nc = &SymbolNewsCounts{}
			result[r.Symbol] = nc
		}
		t := time.UnixMilli(r.Time).In(s.loc)
		if t.Format("2006-01-02") != date {
			continue
		}
		minutes := t.Hour()*60 + t.Minute()
		if r.Source == "stocktwits" {
			if minutes < 570 {
				nc.StPre++
			} else if minutes < 960 {
				nc.StReg++
			} else {
				nc.StPost++
			}
		} else {
			nc.News++
		}
	}
	return result
}

func (s *DashboardServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	sortMode := parseSortMode(r)
	now := time.Now().In(s.loc)
	date := now.Format("2006-01-02")

	// Compute today's 9:30 AM ET cutoff.
	todayOpen930 := time.Date(now.Year(), now.Month(), now.Day(), 9, 30, 0, 0, s.loc).UnixMilli()
	_, off := now.Zone()
	todayOpen930ET := todayOpen930 + int64(off)*1000
	nextOpen930ET := todayOpen930ET + 24*60*60*1000

	_, todayExIdx := s.model.TodaySnapshot()
	_, nextExIdx := s.model.NextSnapshot()

	todayData := dashboard.ComputeDayData("TODAY", todayExIdx, s.tierMap, todayOpen930ET, sortMode)
	newsCounts := s.computeNewsCounts(date)
	todayJSON := convertDayData(todayData, newsCounts)
	todayJSON.Date = date

	resp := DashboardResponse{
		Date:      date,
		Today:     todayJSON,
		SortMode:  sortMode,
		SortLabel: dashboard.SortModeLabel(sortMode),
	}

	if len(nextExIdx) > 0 {
		nextData := dashboard.ComputeDayData("NEXT DAY", nextExIdx, s.tierMap, nextOpen930ET, sortMode)
		nd := convertDayData(nextData, newsCounts)
		resp.Next = &nd
	}

	writeJSON(w, resp)
}

func (s *DashboardServer) handleHistory(w http.ResponseWriter, r *http.Request) {
	date := r.PathValue("date")
	if date == "" {
		writeError(w, http.StatusBadRequest, "date required")
		return
	}

	sortMode := parseSortMode(r)

	// Load tier map for this specific date.
	tierMap, err := dashboard.LoadTierMapForDate(s.dataDir, date)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("tier map not found for %s", date))
		return
	}

	trades, err := dashboard.LoadHistoryTrades(s.dataDir, date)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("trades not found for %s", date))
		return
	}

	open930 := open930ET(date, s.loc)
	data := dashboard.ComputeDayData(date, trades, tierMap, open930, sortMode)
	newsCounts := s.loadNewsCounts(date)
	todayJSON := convertDayData(data, newsCounts)
	todayJSON.Date = date

	resp := DashboardResponse{
		Date:      date,
		Today:     todayJSON,
		SortMode:  sortMode,
		SortLabel: dashboard.SortModeLabel(sortMode),
	}

	// Load next day data.
	nextDate := s.nextDateFor(date)
	if nextDate != "" {
		nextTrades, err := dashboard.LoadHistoryTrades(s.dataDir, nextDate)
		if err == nil && len(nextTrades) > 0 {
			// Filter to post-market window (4PM-8PM ET).
			postEnd := postMarketEndET(date)
			var filtered []store.TradeRecord
			for i := range nextTrades {
				if nextTrades[i].Timestamp <= postEnd {
					filtered = append(filtered, nextTrades[i])
				}
			}
			if len(filtered) > 0 {
				nextOpen930 := open930ET(nextDate, s.loc)
				nextData := dashboard.ComputeDayData("NEXT: "+nextDate, filtered, tierMap, nextOpen930, sortMode)
				nd := convertDayData(nextData, newsCounts)
				nd.Date = nextDate
				resp.Next = &nd
			}
		}
	} else if hd := s.getHistoryDates(); len(hd) > 0 && date == hd[len(hd)-1] {
		// Latest date: read per-symbol trade files for post-market window.
		postStart := postMarketStartET(date)
		postEnd := postMarketEndET(date)
		symbols := make([]string, 0, len(tierMap))
		for sym := range tierMap {
			symbols = append(symbols, sym)
		}
		filtered := dashboard.LoadPerSymbolTrades(s.dataDir, date, postStart, postEnd, symbols)
		if len(filtered) > 0 {
			now := time.Now().In(s.loc)
			nextDateLabel := now.Format("2006-01-02")
			nextOpen930 := open930ET(nextDateLabel, s.loc)
			nextData := dashboard.ComputeDayData("NEXT: "+nextDateLabel, filtered, tierMap, nextOpen930, sortMode)
			nd := convertDayData(nextData, newsCounts)
			nd.Date = nextDateLabel
			resp.Next = &nd
		}
	}

	writeJSON(w, resp)
}

func (s *DashboardServer) handleReplay(w http.ResponseWriter, r *http.Request) {
	date := r.URL.Query().Get("date")
	if date == "" {
		writeError(w, http.StatusBadRequest, "date required")
		return
	}
	untilStr := r.URL.Query().Get("until")
	if untilStr == "" {
		writeError(w, http.StatusBadRequest, "until required")
		return
	}
	until, err := strconv.ParseInt(untilStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid until timestamp")
		return
	}
	sortMode := parseSortMode(r)

	// Determine ET offset for this date to convert between real Unix ms and
	// the internal ET-shifted convention (ET clock time stored as UTC).
	dateTime, _ := time.ParseInLocation("2006-01-02", date, s.loc)
	_, etOff := dateTime.Zone()             // e.g. -18000 for EST
	etOffMs := int64(etOff) * 1000          // e.g. -18000000
	untilET := until + etOffMs              // convert real → ET-shifted

	// Load trades + tier map (from live model or replay cache).
	today := time.Now().In(s.loc).Format("2006-01-02")
	var trades []store.TradeRecord
	var tierMap map[string]string

	if date == today {
		_, trades = s.model.TodaySnapshot()
		tierMap = s.tierMap
	} else {
		trades, tierMap = s.getReplayCache(date)
		if trades == nil {
			// Load from disk.
			loaded, loadErr := dashboard.LoadHistoryTrades(s.dataDir, date)
			if loadErr != nil {
				writeError(w, http.StatusNotFound, fmt.Sprintf("trades not found for %s", date))
				return
			}
			tm, tmErr := dashboard.LoadTierMapForDate(s.dataDir, date)
			if tmErr != nil {
				writeError(w, http.StatusNotFound, fmt.Sprintf("tier map not found for %s", date))
				return
			}
			// Sort by timestamp for binary search.
			sort.Slice(loaded, func(i, j int) bool {
				return loaded[i].Timestamp < loaded[j].Timestamp
			})
			s.putReplayCache(date, loaded, tm)
			trades = loaded
			tierMap = tm
		}
	}

	// Compute time range from full trades (ET-shifted), then convert to real Unix ms.
	var timeRange *TimeRange
	if len(trades) > 0 {
		minTS, maxTS := trades[0].Timestamp, trades[0].Timestamp
		for i := 1; i < len(trades); i++ {
			if trades[i].Timestamp < minTS {
				minTS = trades[i].Timestamp
			}
			if trades[i].Timestamp > maxTS {
				maxTS = trades[i].Timestamp
			}
		}
		timeRange = &TimeRange{Start: minTS - etOffMs, End: maxTS - etOffMs}
	}

	// Filter trades to timestamp <= untilET (ET-shifted comparison).
	var filtered []store.TradeRecord
	if date == today {
		// Live trades may not be sorted; do linear scan.
		for i := range trades {
			if trades[i].Timestamp <= untilET {
				filtered = append(filtered, trades[i])
			}
		}
	} else {
		// History trades are sorted — binary search.
		idx := sort.Search(len(trades), func(i int) bool {
			return trades[i].Timestamp > untilET
		})
		filtered = trades[:idx]
	}

	open930 := open930ET(date, s.loc)
	// Load news counts (live from cache, history from parquet).
	var newsCounts map[string]*SymbolNewsCounts
	if date == today {
		newsCounts = s.computeNewsCounts(date)
	} else {
		newsCounts = s.loadNewsCounts(date)
	}

	data := dashboard.ComputeDayData(date, filtered, tierMap, open930, sortMode)
	todayJSON := convertDayData(data, newsCounts)
	todayJSON.Date = date

	resp := DashboardResponse{
		Date:      date,
		Today:     todayJSON,
		SortMode:  sortMode,
		SortLabel: dashboard.SortModeLabel(sortMode),
		TimeRange: timeRange,
	}

	writeJSON(w, resp)
}

// getReplayCache returns cached trades and tier map for a date, or nil if not cached.
func (s *DashboardServer) getReplayCache(date string) ([]store.TradeRecord, map[string]string) {
	s.replayMu.RLock()
	defer s.replayMu.RUnlock()
	trades, ok := s.replayCache[date]
	if !ok {
		return nil, nil
	}
	return trades, s.replayTier[date]
}

// putReplayCache stores trades and tier map in the replay cache, evicting oldest if over 10 entries.
func (s *DashboardServer) putReplayCache(date string, trades []store.TradeRecord, tierMap map[string]string) {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()

	s.replayCache[date] = trades
	s.replayTier[date] = tierMap

	// Evict oldest if over 10 entries.
	if len(s.replayCache) > 10 {
		var oldest string
		for d := range s.replayCache {
			if oldest == "" || d < oldest {
				oldest = d
			}
		}
		delete(s.replayCache, oldest)
		delete(s.replayTier, oldest)
	}
}

func (s *DashboardServer) handleDates(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, DatesResponse{Dates: s.getHistoryDates()})
}

func (s *DashboardServer) handleGetWatchlist(w http.ResponseWriter, r *http.Request) {
	if s.alpacaClient == nil {
		writeJSON(w, WatchlistResponse{Symbols: []string{}})
		return
	}

	date := r.URL.Query().Get("date")
	if date == "" {
		date = time.Now().In(s.loc).Format("2006-01-02")
	}

	wlID, err := s.resolveWatchlistID(date)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to resolve watchlist")
		return
	}

	wl, err := s.alpacaClient.GetWatchlist(wlID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get watchlist")
		return
	}

	symbols := make([]string, 0, len(wl.Assets))
	for _, a := range wl.Assets {
		symbols = append(symbols, a.Symbol)
	}
	sort.Strings(symbols)
	writeJSON(w, WatchlistResponse{Symbols: symbols})
}

func (s *DashboardServer) handleAddWatchlist(w http.ResponseWriter, r *http.Request) {
	if s.alpacaClient == nil {
		writeError(w, http.StatusServiceUnavailable, "watchlist not configured")
		return
	}

	date := r.URL.Query().Get("date")
	if date == "" {
		date = time.Now().In(s.loc).Format("2006-01-02")
	}

	wlID, err := s.resolveWatchlistID(date)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to resolve watchlist")
		return
	}

	symbol := strings.ToUpper(r.PathValue("symbol"))
	_, err = s.alpacaClient.AddSymbolToWatchlist(wlID, alpacaapi.AddSymbolToWatchlistRequest{Symbol: symbol})
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to add %s: %v", symbol, err))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *DashboardServer) handleRemoveWatchlist(w http.ResponseWriter, r *http.Request) {
	if s.alpacaClient == nil {
		writeError(w, http.StatusServiceUnavailable, "watchlist not configured")
		return
	}

	date := r.URL.Query().Get("date")
	if date == "" {
		date = time.Now().In(s.loc).Format("2006-01-02")
	}

	wlID, err := s.resolveWatchlistID(date)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to resolve watchlist")
		return
	}

	symbol := strings.ToUpper(r.PathValue("symbol"))
	err = s.alpacaClient.RemoveSymbolFromWatchlist(wlID, alpacaapi.RemoveSymbolFromWatchlistRequest{Symbol: symbol})
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to remove %s: %v", symbol, err))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *DashboardServer) handleNews(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(r.PathValue("symbol"))
	date := r.URL.Query().Get("date")
	if date == "" {
		date = time.Now().In(s.loc).Format("2006-01-02")
	}

	now := time.Now().In(s.loc)
	today := now.Format("2006-01-02")
	tomorrow := now.AddDate(0, 0, 1).Format("2006-01-02")

	// For today/tomorrow, serve from background news cache or fetch on demand.
	if date == today || date == tomorrow {
		key := symbol + ":" + date
		if v, ok := s.newsCache.Load(key); ok {
			articles := v.([]NewsArticleJSON)
			writeJSON(w, NewsResponse{Symbol: symbol, Date: date, Articles: articles})
			return
		}
		// Not in cache — fetch on demand for this symbol.
		articles := s.fetchNewsOnDemand(symbol, date)
		writeJSON(w, NewsResponse{Symbol: symbol, Date: date, Articles: articles})
		return
	}

	// Fall back to parquet file for historical dates.
	path := filepath.Join(s.dataDir, "us", "news", date+".parquet")
	records, err := parquet.ReadFile[NewsRecord](path)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, NewsResponse{Symbol: symbol, Date: date, Articles: []NewsArticleJSON{}})
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to read news")
		return
	}

	var articles []NewsArticleJSON
	for i := range records {
		if records[i].Symbol == symbol {
			articles = append(articles, NewsArticleJSON{
				Time:     records[i].Time,
				Source:   records[i].Source,
				Headline: records[i].Headline,
				Content:  records[i].Content,
			})
		}
	}
	if articles == nil {
		articles = []NewsArticleJSON{}
	}

	writeJSON(w, NewsResponse{Symbol: symbol, Date: date, Articles: articles})
}

func (s *DashboardServer) handleSymbolHistory(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(r.PathValue("symbol"))
	before := r.URL.Query().Get("before")
	until := r.URL.Query().Get("until")
	limit := 200
	if ls := r.URL.Query().Get("limit"); ls != "" {
		if n, err := strconv.Atoi(ls); err == nil && n > 0 {
			limit = n
		}
	}

	// List per-symbol trade files: $DATA_1/us/trades/{SYMBOL}/*.parquet
	symDir := filepath.Join(s.dataDir, "us", "trades", symbol)
	entries, err := os.ReadDir(symDir)
	if err != nil {
		writeJSON(w, SymbolHistoryResponse{Symbol: symbol, Dates: []SymbolDateStats{}})
		return
	}

	// Collect date files sorted chronologically.
	var tradeDates []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".parquet") {
			continue
		}
		date := strings.TrimSuffix(e.Name(), ".parquet")
		if len(date) == 10 && date[4] == '-' && date[7] == '-' {
			tradeDates = append(tradeDates, date)
		}
	}
	sort.Strings(tradeDates)
	allDates := make([]string, len(tradeDates))
	copy(allDates, tradeDates)

	// Apply "until" filter: only dates <= the given date.
	if until != "" {
		end := sort.SearchStrings(tradeDates, until)
		if end < len(tradeDates) && tradeDates[end] == until {
			end++
		}
		tradeDates = tradeDates[:end]
	}

	// Apply "before" filter: only dates strictly before the given date.
	if before != "" {
		end := sort.SearchStrings(tradeDates, before)
		tradeDates = tradeDates[:end]
	}

	// Paginate: take the last `limit` dates.
	hasMore := false
	if len(tradeDates) > limit {
		hasMore = true
		tradeDates = tradeDates[len(tradeDates)-limit:]
	}

	// Load and aggregate each date, using cache.
	var dates []SymbolDateStats
	for _, date := range tradeDates {
		// Find prev date in the full list.
		idx := sort.SearchStrings(allDates, date)
		prevDate := ""
		if idx > 0 {
			prevDate = allDates[idx-1]
		}
		entry := s.loadSymbolDateStats(symbol, date, prevDate)
		if entry != nil {
			dates = append(dates, *entry)
		}
	}

	// Append live data (today, not cached) — only on the first page (no "before") and no "until" cap.
	todayDate := time.Now().In(s.loc).Format("2006-01-02")
	if before == "" && (until == "" || until >= todayDate) {
		_, todayExIdx := s.model.TodaySnapshot()
		if len(todayExIdx) > 0 {
			symTrades := dashboard.FilterTradesBySymbol(todayExIdx, symbol)
			if len(symTrades) > 0 {
				now := time.Now().In(s.loc)
				todayDate := now.Format("2006-01-02")
				todayOpen930 := time.Date(now.Year(), now.Month(), now.Day(), 9, 30, 0, 0, s.loc).UnixMilli()
				_, off := now.Zone()
				todayOpen930ET := todayOpen930 + int64(off)*1000

				pre, reg := dashboard.SplitBySession(symTrades, todayOpen930ET)
				preStats := dashboard.AggregateTrades(pre)
				regStats := dashboard.AggregateTrades(reg)

				entry := SymbolDateStats{Date: todayDate}
				if ps, ok := preStats[symbol]; ok {
					entry.Pre = convertSymbolStats(ps)
				}
				if rs, ok := regStats[symbol]; ok {
					entry.Reg = convertSymbolStats(rs)
				}
				if len(dates) == 0 || dates[len(dates)-1].Date != todayDate {
					dates = append(dates, entry)
				}
			}
		}
	}

	if dates == nil {
		dates = []SymbolDateStats{}
	}

	writeJSON(w, SymbolHistoryResponse{Symbol: symbol, Dates: dates, HasMore: hasMore})
}

func (s *DashboardServer) handleGetTargets(w http.ResponseWriter, r *http.Request) {
	date := r.URL.Query().Get("date")
	if date == "" {
		date = time.Now().In(s.loc).Format("2006-01-02")
	}

	writeJSON(w, map[string]any{"targets": s.tradeParams.Get(date)})
}

func (s *DashboardServer) handleSetTarget(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Date  string  `json:"date"`
		Key   string  `json:"key"`
		Value float64 `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Date == "" || req.Key == "" {
		writeError(w, http.StatusBadRequest, "date and key required")
		return
	}

	s.tradeParams.Set(req.Date, req.Key, req.Value)
	w.WriteHeader(http.StatusNoContent)
}

func (s *DashboardServer) handleDeleteTarget(w http.ResponseWriter, r *http.Request) {
	date := r.URL.Query().Get("date")
	key := r.URL.Query().Get("key")
	if date == "" || key == "" {
		writeError(w, http.StatusBadRequest, "date and key required")
		return
	}

	s.tradeParams.Delete(date, key)
	w.WriteHeader(http.StatusNoContent)
}

func (s *DashboardServer) handleTargetStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Subscribe to store events.
	subID, ch := s.tradeParams.Subscribe(64)
	defer s.tradeParams.Unsubscribe(subID)

	// Send snapshot.
	snap := tradeparams.Event{
		Type: "snapshot",
		Data: s.tradeParams.Snapshot(),
	}
	if data, err := json.Marshal(snap); err == nil {
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	// Stream incremental events with heartbeat.
	ctx := r.Context()
	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			if data, err := json.Marshal(evt); err == nil {
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		case <-heartbeat.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

// loadSymbolDateStats reads per-symbol trade files using the same (P 4PM, D 4PM]
// window as consolidated files: after-hours from prevDate's file + current date's
// file up to 4PM. Results are cached forever (history is immutable).
func (s *DashboardServer) loadSymbolDateStats(symbol, date, prevDate string) *SymbolDateStats {
	cacheKey := symbol + ":" + date
	if v, ok := s.symbolHistoryCache.Load(cacheKey); ok {
		return v.(*SymbolDateStats)
	}

	tradesDir := filepath.Join(s.dataDir, "us", "trades", symbol)

	// regularClose returns 4PM as ET-shifted millis (same convention as stock_trades.go).
	close4pm := func(d string) int64 {
		t, _ := time.Parse("2006-01-02", d)
		return time.Date(t.Year(), t.Month(), t.Day(), 16, 0, 0, 0, time.UTC).UnixMilli()
	}

	dateClose := close4pm(date)
	var trades []store.TradeRecord

	// Read previous date's file: trades after P 4PM (after-hours → pre-market).
	if prevDate != "" {
		prevClose := close4pm(prevDate)
		pPath := filepath.Join(tradesDir, prevDate+".parquet")
		if records, err := parquet.ReadFile[store.TradeRecord](pPath); err == nil {
			for _, r := range records {
				if r.Timestamp > prevClose {
					trades = append(trades, r)
				}
			}
		}
	}

	// Read current date's file: trades up to D 4PM.
	dPath := filepath.Join(tradesDir, date+".parquet")
	if records, err := parquet.ReadFile[store.TradeRecord](dPath); err == nil {
		for _, r := range records {
			if r.Timestamp <= dateClose {
				trades = append(trades, r)
			}
		}
	}

	// Apply exchange/condition filter (same as consolidated files).
	filtered := dashboard.FilterTradeRecords(trades)
	if len(filtered) == 0 {
		return nil
	}

	open930 := open930ET(date, s.loc)
	pre, reg := dashboard.SplitBySession(filtered, open930)
	preStats := dashboard.AggregateTrades(pre)
	regStats := dashboard.AggregateTrades(reg)

	entry := &SymbolDateStats{Date: date}
	if ps, ok := preStats[symbol]; ok {
		entry.Pre = convertSymbolStats(ps)
	}
	if rs, ok := regStats[symbol]; ok {
		entry.Reg = convertSymbolStats(rs)
	}

	s.symbolHistoryCache.Store(cacheKey, entry)
	return entry
}
