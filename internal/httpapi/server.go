package httpapi

import (
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
	"time"

	alpacaapi "github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/parquet-go/parquet-go"

	"jupitor/internal/dashboard"
	"jupitor/internal/live"
	"jupitor/internal/store"
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

	// Alpaca marketdata client for live news (nil if not configured).
	mdClient *marketdata.Client

	// Cache for per-symbol per-date history stats. Key: "SYMBOL:DATE".
	symbolHistoryCache sync.Map
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
	}

	return s
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
		// Latest date: use live model for next-day data.
		_, liveTrades := s.model.TodaySnapshot()
		if len(liveTrades) > 0 {
			postEnd := postMarketEndET(date)
			var filtered []store.TradeRecord
			for i := range liveTrades {
				if liveTrades[i].Timestamp <= postEnd {
					filtered = append(filtered, liveTrades[i])
				}
			}
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

	// Use live Alpaca API for today and tomorrow (next trading day).
	if s.mdClient != nil && (date == today || date == tomorrow) {
		articles, err := s.fetchLiveNews(symbol, date)
		if err != nil {
			s.log.Warn("live news fetch failed, falling back to parquet", "symbol", symbol, "date", date, "error", err)
		} else {
			writeJSON(w, NewsResponse{Symbol: symbol, Date: date, Articles: articles})
			return
		}
	}

	// Fall back to parquet file for historical dates (or if live fetch failed).
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

// fetchLiveNews fetches news from the Alpaca marketdata API for a symbol on a given date.
func (s *DashboardServer) fetchLiveNews(symbol, date string) ([]NewsArticleJSON, error) {
	t, err := time.ParseInLocation("2006-01-02", date, s.loc)
	if err != nil {
		return nil, fmt.Errorf("parsing date: %w", err)
	}
	start := time.Date(t.Year(), t.Month(), t.Day(), 4, 0, 0, 0, s.loc)
	end := time.Date(t.Year(), t.Month(), t.Day(), 20, 0, 0, 0, s.loc)

	news, err := s.mdClient.GetNews(marketdata.GetNewsRequest{
		Symbols:            []string{symbol},
		Start:              start,
		End:                end,
		TotalLimit:         50,
		IncludeContent:     true,
		ExcludeContentless: true,
		Sort:               marketdata.SortAsc,
	})
	if err != nil {
		return nil, fmt.Errorf("alpaca news API: %w", err)
	}

	articles := make([]NewsArticleJSON, 0, len(news))
	for _, a := range news {
		body := a.Summary
		if a.Content != "" {
			body = a.Content
		}
		articles = append(articles, NewsArticleJSON{
			Time:     a.CreatedAt.UnixMilli(),
			Source:   "alpaca",
			Headline: a.Headline,
			Content:  body,
		})
	}
	return articles, nil
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
