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

	// History dates (loaded once, can be refreshed).
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

	// Cache for per-symbol per-date history stats. Key: "SYMBOL:DATE".
	symbolHistoryCache sync.Map

	// Trade parameters (targets, etc.) with pub/sub for SSE push.
	tradeParams *tradeparams.Store
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
	}

	return s
}

// Start launches background goroutines (news refresh). Call this after creating
// the server, tied to the daemon's context for graceful shutdown.
func (s *DashboardServer) Start(ctx context.Context) {
	go s.startNewsRefresh(ctx)
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

// refreshNewsCache fetches news for today's top symbols from all 4 sources.
func (s *DashboardServer) refreshNewsCache() {
	if s.mdClient == nil {
		return
	}

	now := time.Now().In(s.loc)
	date := now.Format("2006-01-02")

	// Get today's trades and pick top symbols per tier.
	_, todayExIdx := s.model.TodaySnapshot()
	if len(todayExIdx) == 0 {
		s.log.Debug("news refresh: no trades yet")
		return
	}

	stats := dashboard.AggregateTrades(todayExIdx)

	type symCount struct {
		sym    string
		trades int
	}
	tierSyms := map[string][]symCount{}
	for sym, st := range stats {
		tier, ok := s.tierMap[sym]
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

	// Top 20 per tier.
	symbolSet := make(map[string]bool)
	for _, tier := range []string{"ACTIVE", "MODERATE", "SPORADIC"} {
		ss := tierSyms[tier]
		limit := 20
		if len(ss) < limit {
			limit = len(ss)
		}
		for _, sc := range ss[:limit] {
			symbolSet[sc.sym] = true
		}
	}

	symbols := make([]string, 0, len(symbolSet))
	for sym := range symbolSet {
		symbols = append(symbols, sym)
	}
	sort.Strings(symbols)

	if len(symbols) == 0 {
		return
	}

	// Time window: prev trading day 4PM ET → today 8PM ET.
	t, _ := time.ParseInLocation("2006-01-02", date, s.loc)
	end := time.Date(t.Year(), t.Month(), t.Day(), 20, 0, 0, 0, s.loc)
	// Find previous trading day from history dates.
	start := time.Date(t.Year(), t.Month(), t.Day(), 4, 0, 0, 0, s.loc) // fallback
	for i := len(s.historyDates) - 1; i >= 0; i-- {
		if s.historyDates[i] < date {
			p, _ := time.ParseInLocation("2006-01-02", s.historyDates[i], s.loc)
			start = time.Date(p.Year(), p.Month(), p.Day(), 16, 0, 0, 0, s.loc)
			break
		}
	}

	s.log.Info("news refresh starting", "date", date, "symbols", len(symbols))

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

			// Sort by time.
			sort.Slice(articles, func(i, j int) bool {
				return articles[i].Time < articles[j].Time
			})

			key := sym + ":" + date
			s.newsCache.Store(key, articles)
			atomic.AddInt64(&totalArticles, int64(len(articles)))
		}(sym)
	}
	wg.Wait()

	s.log.Info("news refresh complete", "date", date, "symbols", len(symbols), "articles", totalArticles)

	// Persist to disk for fast restart.
	s.saveNewsToDisk(date)
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
	for i, d := range s.historyDates {
		if d == date && i+1 < len(s.historyDates) {
			return s.historyDates[i+1]
		}
	}
	return ""
}

// loadNewsCounts reads the news parquet file for a date and returns symbol→count.
func (s *DashboardServer) loadNewsCounts(date string) map[string]int {
	path := filepath.Join(s.dataDir, "us", "news", date+".parquet")
	records, err := parquet.ReadFile[NewsRecord](path)
	if err != nil {
		return nil
	}
	counts := make(map[string]int)
	for i := range records {
		counts[records[i].Symbol]++
	}
	return counts
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
	newsCounts := s.loadNewsCounts(date)
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
	} else if len(s.historyDates) > 0 && date == s.historyDates[len(s.historyDates)-1] {
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

func (s *DashboardServer) handleDates(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, DatesResponse{Dates: s.historyDates})
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

	// For today/tomorrow, serve from background news cache.
	if date == today || date == tomorrow {
		key := symbol + ":" + date
		if v, ok := s.newsCache.Load(key); ok {
			articles := v.([]NewsArticleJSON)
			writeJSON(w, NewsResponse{Symbol: symbol, Date: date, Articles: articles})
			return
		}
		// Not in cache — return empty (background refresh will populate it).
		writeJSON(w, NewsResponse{Symbol: symbol, Date: date, Articles: []NewsArticleJSON{}})
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

	// Stream incremental events.
	ctx := r.Context()
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
