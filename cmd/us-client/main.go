package main

import (
	"context"
	"encoding/xml"
	"fmt"
	"html"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	alpacaapi "github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"jupitor/internal/dashboard"
	"jupitor/internal/live"
	"jupitor/internal/store"
)

// Styles.
var (
	tierActiveStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("10"))
	tierModerateStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("11"))
	tierSporadicStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("9"))
	symbolStyle       = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("12"))
	symbolHlStyle     = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("75"))  // brighter blue for highlight
	symbolWlStyle     = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("208")) // orange for watchlist
	symbolWlHlStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("214")) // brighter orange for watchlist+highlight
	gainStyle         = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))
	lossStyle         = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	colHeaderStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	dayLabelStyle     = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("0")).Background(lipgloss.Color("6"))
	dimStyle          = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	priceStyle        = lipgloss.NewStyle().Foreground(lipgloss.Color("15"))
	priceWlStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("208")) // orange OHLC for watchlist
	tradeCountStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("14"))
	turnoverStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("13"))
	historyBarStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("0")).Background(lipgloss.Color("3")) // black on yellow
	highlightBG     = lipgloss.Color("236")                                                                          // dark grey background
)

// hlStyle returns a copy of s with the highlight background applied when hl is true.
func hlStyle(s lipgloss.Style, hl bool) lipgloss.Style {
	if hl {
		return s.Background(highlightBG)
	}
	return s
}

func tierStyle(name string) lipgloss.Style {
	switch name {
	case "ACTIVE":
		return tierActiveStyle
	case "MODERATE":
		return tierModerateStyle
	case "SPORADIC":
		return tierSporadicStyle
	default:
		return lipgloss.NewStyle()
	}
}

func watchlistName(date string) string {
	return "jupitor-" + date
}

// Messages.
type tickMsg time.Time
type syncErrMsg struct{ err error }

type watchlistLoadedMsg struct {
	date    string
	id      string
	symbols map[string]bool
	err     error
}

type watchlistToggleMsg struct {
	symbol string
	added  bool
	err    error
}

type newsArticle struct {
	Time     time.Time
	Source   string // e.g. "Alpaca", "Reuters", "CNBC"
	Headline string
	Content  string // plain text (already stripped)
}

type newsLoadedMsg struct {
	symbol   string
	date     string
	prevDate string // previous trading day (for caching)
	news     []newsArticle
	err      error
}

type newsCountMsg struct {
	date   string
	counts map[string]int
	err    error
}

type newsCountRefreshMsg struct{}

type historyCacheEntry struct {
	data     dashboard.DayData
	nextData dashboard.DayData
	tierMap  map[string]string
	trades   int
	sortMode int
}

type selectableEntry struct {
	day    int    // 0=primary, 1=next
	symbol string
}

type historyLoadedMsg struct {
	date     string
	data     dashboard.DayData
	nextData dashboard.DayData
	tierMap  map[string]string
	trades  int
	err     error
}

// preloadedMsg is like historyLoadedMsg but for background preloading.
// It only populates the cache; it doesn't update the view.
type preloadedMsg struct {
	date     string
	data     dashboard.DayData
	nextData dashboard.DayData
	tierMap  map[string]string
	trades   int
	err      error
}

func tickCmd() tea.Cmd {
	return tea.Tick(5*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func newsCountTickCmd() tea.Cmd {
	return tea.Tick(5*time.Minute, func(t time.Time) tea.Msg {
		return newsCountRefreshMsg{}
	})
}

// Model.
type model struct {
	// Live mode.
	liveModel  *live.LiveModel
	tierMap    map[string]string
	loc        *time.Location
	todayData  dashboard.DayData
	nextData   dashboard.DayData
	seen       int
	todayCount int
	nextCount  int
	now        time.Time

	// Shared.
	dataDir       string
	tradingDate   string
	latestTS      string
	sortMode int
	viewport      viewport.Model
	ready         bool
	width, height int
	syncCancel    context.CancelFunc
	logger        *slog.Logger

	// Selection.
	selectedDay    int    // 0=primary day, 1=next day
	selectedSymbol string

	// Watchlist.
	alpacaClient     *alpacaapi.Client // nil if no API keys
	watchlistDate    string            // date the current watchlist is for
	watchlistID      string
	watchlistSymbols map[string]bool
	watchlistOnly    bool // w key toggle: only show watchlist symbols

	// News.
	mdClient    *marketdata.Client
	newsCache   map[string][]newsArticle // key: "SYMBOL:YYYY-MM-DD"
	newsSymbol  string                   // symbol of in-flight fetch
	newsDate    string                   // date of in-flight fetch
	newsLoading bool
	prevTDCache map[string]string // date -> previous trading day (from Alpaca Calendar)

	// News counts (batch fetch for column display).
	newsCountCache   map[string]map[string]int // date -> symbol -> count
	newsCountLoading bool
	newsCountDate    string // date of in-flight count fetch

	// History mode.
	historyMode     bool
	historyDates    []string // sorted available dates
	historyIdx      int      // index into historyDates
	historyData     dashboard.DayData
	historyNextData dashboard.DayData
	historyTierMap  map[string]string
	historyTrades   int
	historyLoading  bool
	historyCache    map[string]*historyCacheEntry

	// Background preload queue (sequential, one at a time).
	preloadQueue   []string // dates remaining to preload
	preloadRunning bool     // true while a preload cmd is in flight
}

func initialModel(lm *live.LiveModel, tierMap map[string]string, loc *time.Location, cancel context.CancelFunc, dataDir string, histDates []string, logger *slog.Logger, ac *alpacaapi.Client, mdc *marketdata.Client) model {
	return model{
		liveModel:        lm,
		tierMap:          tierMap,
		loc:              loc,
		now:              time.Now().In(loc),
		syncCancel:       cancel,
		dataDir:          dataDir,
		historyDates:     histDates,
		logger:           logger,
		historyCache:     make(map[string]*historyCacheEntry),
		alpacaClient:     ac,
		watchlistSymbols: make(map[string]bool),
		mdClient:         mdc,
		newsCache:        make(map[string][]newsArticle),
		prevTDCache:      make(map[string]string),
		newsCountCache:   make(map[string]map[string]int),
	}
}

type preloadStartMsg struct{}

func (m model) Init() tea.Cmd {
	return tea.Batch(tickCmd(), func() tea.Msg { return preloadStartMsg{} })
}

// loadWatchlist gets or creates the per-date watchlist and returns its symbols.
func loadWatchlist(ac *alpacaapi.Client, date string) watchlistLoadedMsg {
	name := watchlistName(date)
	lists, err := ac.GetWatchlists()
	if err != nil {
		return watchlistLoadedMsg{date: date, err: err}
	}
	for _, w := range lists {
		if w.Name == name {
			// GetWatchlists doesn't include assets; fetch the full watchlist.
			full, err := ac.GetWatchlist(w.ID)
			if err != nil {
				return watchlistLoadedMsg{date: date, err: err}
			}
			syms := make(map[string]bool, len(full.Assets))
			for _, a := range full.Assets {
				syms[a.Symbol] = true
			}
			return watchlistLoadedMsg{date: date, id: w.ID, symbols: syms}
		}
	}
	// Create it.
	w, err := ac.CreateWatchlist(alpacaapi.CreateWatchlistRequest{Name: name})
	if err != nil {
		return watchlistLoadedMsg{date: date, err: err}
	}
	return watchlistLoadedMsg{date: date, id: w.ID, symbols: make(map[string]bool)}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.syncCancel()
			return m, tea.Quit
		case "s":
			m.sortMode = (m.sortMode + 1) % dashboard.SortModeCount
			if m.historyMode {
				m.rebuildHistory()
			} else {
				m.refreshLive()
			}
			m.viewport.SetContent(m.renderContent())
			return m, nil
		case " ":
			if m.selectedSymbol != "" && m.alpacaClient != nil && m.watchlistID != "" {
				sym := m.selectedSymbol
				ac := m.alpacaClient
				wlID := m.watchlistID
				if m.watchlistSymbols[sym] {
					delete(m.watchlistSymbols, sym)
					m.viewport.SetContent(m.renderContent())
					return m, func() tea.Msg {
						err := ac.RemoveSymbolFromWatchlist(wlID, alpacaapi.RemoveSymbolFromWatchlistRequest{Symbol: sym})
						return watchlistToggleMsg{symbol: sym, added: false, err: err}
					}
				}
				m.watchlistSymbols[sym] = true
				m.viewport.SetContent(m.renderContent())
				newsCmd := m.maybeLoadNews()
				return m, tea.Batch(func() tea.Msg {
					_, err := ac.AddSymbolToWatchlist(wlID, alpacaapi.AddSymbolToWatchlistRequest{Symbol: sym})
					return watchlistToggleMsg{symbol: sym, added: true, err: err}
				}, newsCmd)
			}
			return m, nil
		case "up", "down":
			entries := m.flatSelections()
			if len(entries) == 0 {
				return m, nil
			}
			cur := -1
			for i, e := range entries {
				if e.day == m.selectedDay && e.symbol == m.selectedSymbol {
					cur = i
					break
				}
			}
			if msg.String() == "up" {
				if cur > 0 {
					cur--
				} else {
					cur = 0
				}
			} else {
				if cur < len(entries)-1 {
					cur++
				}
			}
			if cur < 0 {
				cur = 0
			}
			m.selectedDay = entries[cur].day
			m.selectedSymbol = entries[cur].symbol
			m.viewport.SetContent(m.renderContent())
			m.ensureVisible()
			return m, m.maybeLoadNews()
		case "left":
			return m, m.navigateHistory(-1)
		case "right":
			return m, m.navigateHistory(1)
		case "home":
			if m.historyMode {
				m.historyMode = false
				m.historyLoading = false
				m.selectedSymbol = "" // force reset in refreshLive
				m.refreshLive()
				m.viewport.SetContent(m.renderContent())
				m.viewport.GotoTop()
			}
			return m, tea.Batch(m.maybeLoadNews(), m.loadNewsCounts(), m.reloadWatchlistIfNeeded())
		case "w":
			m.watchlistOnly = !m.watchlistOnly
			// Validate selection: current symbol may be filtered out.
			entries := m.flatSelections()
			found := false
			for _, e := range entries {
				if e.day == m.selectedDay && e.symbol == m.selectedSymbol {
					found = true
					break
				}
			}
			if !found {
				if len(entries) > 0 {
					m.selectedDay = entries[0].day
					m.selectedSymbol = entries[0].symbol
				} else {
					m.selectedSymbol = ""
				}
			}
			m.viewport.SetContent(m.renderContent())
			return m, m.maybeLoadNews()
		}

	case tea.MouseMsg:
		if msg.Button == tea.MouseButtonLeft && msg.Action == tea.MouseActionPress {
			// Header = line 0, viewport starts at line 1.
			contentLine := msg.Y - 1 + m.viewport.YOffset
			if day, sym := m.selectionAtLine(contentLine); sym != "" {
				m.selectedDay = day
				m.selectedSymbol = sym
				m.viewport.SetContent(m.renderContent())
				return m, m.maybeLoadNews()
			}
			// Check for column header click (sort by Trd/TO/Gain%).
			if mode := m.sortModeAtX(msg.X); mode >= 0 && mode != m.sortMode {
				m.sortMode = mode
				if m.historyMode {
					m.rebuildHistory()
				} else {
					m.refreshLive()
				}
				m.viewport.SetContent(m.renderContent())
				return m, nil
			}
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		headerH := 1
		footerH := 1
		vpHeight := m.height - headerH - footerH
		if vpHeight < 1 {
			vpHeight = 1
		}
		if !m.ready {
			m.viewport = viewport.New(m.width, vpHeight)
			m.viewport.MouseWheelEnabled = true
			m.ready = true
			m.refreshLive()
			m.viewport.SetContent(m.renderContent())
			return m, tea.Batch(m.loadNewsCounts(), m.reloadWatchlistIfNeeded())
		}
		m.viewport.Width = m.width
		m.viewport.Height = vpHeight
		return m, nil

	case tickMsg:
		if !m.historyMode {
			m.refreshLive()
			if m.ready {
				m.viewport.SetContent(m.renderContent())
			}
		}
		return m, tickCmd()

	case historyLoadedMsg:
		m.historyLoading = false
		if msg.err != nil {
			m.logger.Error("loading history", "date", msg.date, "error", msg.err)
			return m, nil
		}
		// Cache the result.
		m.historyCache[msg.date] = &historyCacheEntry{
			data: msg.data, nextData: msg.nextData, tierMap: msg.tierMap, trades: msg.trades,
			sortMode: m.sortMode,
		}
		m.historyData = msg.data
		m.historyNextData = msg.nextData
		m.historyTierMap = msg.tierMap
		m.historyTrades = msg.trades
		m.tradingDate = msg.date
		m.resetSelection()
		if m.ready {
			m.viewport.SetContent(m.renderContent())
			m.viewport.GotoTop()
		}
		// Ensure 5-date buffer around current position.
		return m, tea.Batch(m.ensureBuffer(m.historyIdx), m.maybeLoadNews(), m.loadNewsCounts(), m.reloadWatchlistIfNeeded())

	case preloadedMsg:
		m.preloadRunning = false
		if msg.err != nil {
			m.logger.Warn("preload failed", "date", msg.date, "error", msg.err)
		} else {
			m.historyCache[msg.date] = &historyCacheEntry{
				data: msg.data, nextData: msg.nextData, tierMap: msg.tierMap, trades: msg.trades,
				sortMode: m.sortMode,
			}
			m.logger.Info("preload cached", "date", msg.date, "trades", msg.trades,
				"cached", len(m.historyCache), "queued", len(m.preloadQueue))
		}
		// Chain next preload.
		return m, m.preloadNext()

	case watchlistLoadedMsg:
		if msg.err != nil {
			m.logger.Warn("loading watchlist", "date", msg.date, "error", msg.err)
		} else {
			m.watchlistDate = msg.date
			m.watchlistID = msg.id
			m.watchlistSymbols = msg.symbols
			m.logger.Info("watchlist loaded", "date", msg.date, "id", msg.id, "symbols", len(msg.symbols))
			if m.ready {
				m.viewport.SetContent(m.renderContent())
			}
		}
		return m, m.maybeLoadNews()

	case watchlistToggleMsg:
		if msg.err != nil {
			m.logger.Warn("watchlist toggle failed", "symbol", msg.symbol, "error", msg.err)
			// Revert optimistic update.
			if msg.added {
				delete(m.watchlistSymbols, msg.symbol)
			} else {
				m.watchlistSymbols[msg.symbol] = true
			}
			if m.ready {
				m.viewport.SetContent(m.renderContent())
			}
		} else {
			m.logger.Info("watchlist toggled", "symbol", msg.symbol, "added", msg.added)
		}
		return m, nil

	case newsLoadedMsg:
		m.newsLoading = false
		if msg.err != nil {
			m.logger.Warn("loading news", "symbol", msg.symbol, "date", msg.date, "error", msg.err)
		} else {
			m.newsCache[msg.symbol+":"+msg.date] = msg.news
			if msg.prevDate != "" {
				m.prevTDCache[msg.date] = msg.prevDate
			}
			m.logger.Info("news loaded", "symbol", msg.symbol, "date", msg.date,
				"prevDate", msg.prevDate, "articles", len(msg.news))
		}
		if m.ready {
			m.viewport.SetContent(m.renderContent())
		}
		return m, nil

	case newsCountMsg:
		m.newsCountLoading = false
		if msg.err != nil {
			m.logger.Warn("loading news counts", "date", msg.date, "error", msg.err)
		} else {
			m.newsCountCache[msg.date] = msg.counts
			m.logger.Info("news counts loaded", "date", msg.date, "symbols", len(msg.counts))
			if m.ready {
				m.viewport.SetContent(m.renderContent())
			}
		}
		return m, newsCountTickCmd()

	case newsCountRefreshMsg:
		if !m.historyMode {
			date := m.viewedDate()
			delete(m.newsCountCache, date)
			return m, m.loadNewsCounts()
		}
		return m, newsCountTickCmd()

	case preloadStartMsg:
		return m, m.preloadInitial()

	case syncErrMsg:
		return m, tea.Quit
	}

	if m.ready {
		m.viewport, cmd = m.viewport.Update(msg)
	}
	return m, cmd
}

// flatSelections builds an ordered list of all selectable (day, symbol) entries
// in render order: primary day tiers then next day tiers.
func (m *model) flatSelections() []selectableEntry {
	var entries []selectableEntry
	addDay := func(d dashboard.DayData, dayIdx int) {
		for _, tier := range d.Tiers {
			for _, c := range tier.Symbols {
				if m.watchlistOnly && !m.watchlistSymbols[c.Symbol] {
					continue
				}
				entries = append(entries, selectableEntry{day: dayIdx, symbol: c.Symbol})
			}
		}
	}
	if m.historyMode {
		addDay(m.historyData, 0)
		addDay(m.historyNextData, 1)
	} else {
		addDay(m.todayData, 0)
		addDay(m.nextData, 1)
	}
	return entries
}

// defaultSelection returns the first symbol in the MODERATE tier (or first symbol in any tier).
func defaultSelection(d dashboard.DayData) string {
	for _, tier := range d.Tiers {
		if tier.Name == "MODERATE" && len(tier.Symbols) > 0 {
			return tier.Symbols[0].Symbol
		}
	}
	// Fallback: first symbol in any tier.
	for _, tier := range d.Tiers {
		if len(tier.Symbols) > 0 {
			return tier.Symbols[0].Symbol
		}
	}
	return ""
}

// resetSelection sets the selection to the MODERATE default on the primary day.
func (m *model) resetSelection() {
	m.selectedDay = 0
	if m.historyMode {
		m.selectedSymbol = defaultSelection(m.historyData)
	} else {
		m.selectedSymbol = defaultSelection(m.todayData)
	}
}

// reloadWatchlistIfNeeded returns a tea.Cmd to reload the watchlist if the
// viewed date has changed since the last load.
func (m *model) reloadWatchlistIfNeeded() tea.Cmd {
	if m.alpacaClient == nil {
		return nil
	}
	date := m.viewedDate()
	if date == "" || date == m.watchlistDate {
		return nil
	}
	ac := m.alpacaClient
	return func() tea.Msg {
		return loadWatchlist(ac, date)
	}
}

// viewedDate returns the date currently being viewed (history date or live trading date).
func (m *model) viewedDate() string {
	if m.historyMode && m.historyIdx >= 0 && m.historyIdx < len(m.historyDates) {
		return m.historyDates[m.historyIdx]
	}
	return m.tradingDate
}

// maybeLoadNews returns a tea.Cmd to fetch news for the selected symbol if it's
// not already cached and not already loading. Fetches from both Alpaca and Google
// News RSS, merges results chronologically. The time range spans from the previous
// trading day's market close (4PM ET) to the viewed date's post-market end (8PM ET).
func (m *model) maybeLoadNews() tea.Cmd {
	sym := m.selectedSymbol
	if sym == "" {
		return nil
	}
	date := m.viewedDate()
	if date == "" {
		return nil
	}
	cacheKey := sym + ":" + date
	if _, ok := m.newsCache[cacheKey]; ok {
		return nil
	}
	if m.newsLoading && m.newsSymbol == sym && m.newsDate == date {
		return nil
	}
	m.newsLoading = true
	m.newsSymbol = sym
	m.newsDate = date
	mdc := m.mdClient
	ac := m.alpacaClient
	loc := m.loc
	cachedPrev := m.prevTDCache[date]
	return func() tea.Msg {
		prevDate := cachedPrev
		if prevDate == "" && ac != nil {
			d, _ := time.ParseInLocation("2006-01-02", date, loc)
			lookback := d.AddDate(0, 0, -10)
			cal, err := ac.GetCalendar(alpacaapi.GetCalendarRequest{Start: lookback, End: d})
			if err == nil && len(cal) >= 2 {
				for i := len(cal) - 1; i >= 0; i-- {
					if cal[i].Date < date {
						prevDate = cal[i].Date
						break
					}
				}
			}
		}

		// Time range: previous trading day 4PM ET → viewed date 8PM ET.
		t, _ := time.ParseInLocation("2006-01-02", date, loc)
		end := time.Date(t.Year(), t.Month(), t.Day(), 20, 0, 0, 0, loc)
		var start time.Time
		if prevDate != "" {
			p, _ := time.ParseInLocation("2006-01-02", prevDate, loc)
			start = time.Date(p.Year(), p.Month(), p.Day(), 16, 0, 0, 0, loc)
		} else {
			start = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
		}

		var all []newsArticle

		// Fetch Alpaca news.
		if mdc != nil {
			alpacaNews, err := mdc.GetNews(marketdata.GetNewsRequest{
				Symbols:            []string{sym},
				Start:              start,
				End:                end,
				TotalLimit:         10,
				IncludeContent:     true,
				ExcludeContentless: true,
				Sort:               marketdata.SortAsc,
			})
			if err == nil {
				for _, a := range alpacaNews {
					body := ""
					if a.Content != "" {
						body = extractSymbolContent(a.Content, sym)
					} else if a.Summary != "" {
						body = a.Summary
					}
					all = append(all, newsArticle{
						Time:     a.CreatedAt,
						Source:   "📊",
						Headline: a.Headline,
						Content:  body,
					})
				}
			}
		}

		// Fetch Google News RSS.
		if gn, err := fetchGoogleNews(sym, start, end); err == nil {
			all = append(all, gn...)
		}

		// Sort merged results chronologically.
		sort.Slice(all, func(i, j int) bool {
			return all[i].Time.Before(all[j].Time)
		})

		return newsLoadedMsg{symbol: sym, date: date, prevDate: prevDate, news: all}
	}
}

// loadNewsCounts fetches news article counts for all MODERATE+SPORADIC symbols
// on the viewed date in a single Alpaca API call.
func (m *model) loadNewsCounts() tea.Cmd {
	date := m.viewedDate()
	if date == "" || m.mdClient == nil {
		return nil
	}
	if _, ok := m.newsCountCache[date]; ok {
		return nil
	}
	if m.newsCountLoading && m.newsCountDate == date {
		return nil
	}

	var primary dashboard.DayData
	if m.historyMode {
		primary = m.historyData
	} else {
		primary = m.todayData
	}

	var symbols []string
	for _, tier := range primary.Tiers {
		if tier.Name == "MODERATE" || tier.Name == "SPORADIC" {
			for _, c := range tier.Symbols {
				symbols = append(symbols, c.Symbol)
			}
		}
	}
	if len(symbols) == 0 {
		return nil
	}

	m.newsCountLoading = true
	m.newsCountDate = date
	mdc := m.mdClient
	ac := m.alpacaClient
	loc := m.loc
	cachedPrev := m.prevTDCache[date]

	return func() tea.Msg {
		prevDate := cachedPrev
		if prevDate == "" && ac != nil {
			d, _ := time.ParseInLocation("2006-01-02", date, loc)
			lookback := d.AddDate(0, 0, -10)
			cal, _ := ac.GetCalendar(alpacaapi.GetCalendarRequest{Start: lookback, End: d})
			if len(cal) >= 2 {
				for i := len(cal) - 1; i >= 0; i-- {
					if cal[i].Date < date {
						prevDate = cal[i].Date
						break
					}
				}
			}
		}

		t, _ := time.ParseInLocation("2006-01-02", date, loc)
		end := time.Date(t.Year(), t.Month(), t.Day(), 20, 0, 0, 0, loc)
		var start time.Time
		if prevDate != "" {
			p, _ := time.ParseInLocation("2006-01-02", prevDate, loc)
			start = time.Date(p.Year(), p.Month(), p.Day(), 16, 0, 0, 0, loc)
		} else {
			start = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
		}

		counts := make(map[string]int)

		// Alpaca news counts.
		news, err := mdc.GetNews(marketdata.GetNewsRequest{
			Symbols:            symbols,
			Start:              start,
			End:                end,
			TotalLimit:         50,
			ExcludeContentless: true,
			Sort:               marketdata.SortDesc,
		})
		if err != nil {
			return newsCountMsg{date: date, err: err}
		}
		for _, a := range news {
			for _, s := range a.Symbols {
				counts[s]++
			}
		}

		// Google News RSS counts (per symbol, concurrent).
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, sym := range symbols {
			wg.Add(1)
			go func(s string) {
				defer wg.Done()
				articles, err := fetchGoogleNews(s, start, end)
				if err != nil {
					return
				}
				mu.Lock()
				counts[s] += len(articles)
				mu.Unlock()
			}(sym)
		}
		wg.Wait()

		return newsCountMsg{date: date, counts: counts}
	}
}

// RSS types for Google News.
type rssResponse struct {
	Channel struct {
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
}

type rssItem struct {
	Title   string `xml:"title"`
	PubDate string `xml:"pubDate"`
	Desc    string `xml:"description"`
	Source  string `xml:"source"`
}

// fetchGoogleNews fetches news from Google News RSS for the given symbol,
// filtered to the [start, end] time window.
func fetchGoogleNews(symbol string, start, end time.Time) ([]newsArticle, error) {
	q := url.QueryEscape(symbol + " stock")
	u := "https://news.google.com/rss/search?q=" + q + "&hl=en-US&gl=US&ceid=US:en"

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rss rssResponse
	if err := xml.NewDecoder(resp.Body).Decode(&rss); err != nil {
		return nil, err
	}

	var articles []newsArticle
	for _, item := range rss.Channel.Items {
		t, err := time.Parse(time.RFC1123Z, item.PubDate)
		if err != nil {
			t, err = time.Parse(time.RFC1123, item.PubDate)
			if err != nil {
				continue
			}
		}
		if t.Before(start) || t.After(end) {
			continue
		}
		headline := item.Title
		// Google News appends " - Source Name" to titles; strip it.
		if idx := strings.LastIndex(headline, " - "); idx > 0 {
			headline = headline[:idx]
		}
		articles = append(articles, newsArticle{
			Time:     t,
			Source:   "📰",
			Headline: headline,
			Content:  stripHTML(item.Desc),
		})
	}
	return articles, nil
}

// selectedLine returns the 0-based line number of the selected symbol in rendered content.
// Returns -1 if not found.
func (m *model) selectedLine() int {
	line := 0
	foundIn := func(d dashboard.DayData, dayIdx int) int {
		// Day label = 1 line.
		line++
		if len(d.Tiers) == 0 {
			line++ // "(no matching symbols)"
			return -1
		}
		for _, tier := range d.Tiers {
			// Skip tier entirely if watchlistOnly and no visible symbols.
			if m.watchlistOnly {
				hasAny := false
				for _, c := range tier.Symbols {
					if m.watchlistSymbols[c.Symbol] {
						hasAny = true
						break
					}
				}
				if !hasAny {
					continue
				}
			}
			line += 3 // blank + tier header + col header
			for _, c := range tier.Symbols {
				if m.watchlistOnly && !m.watchlistSymbols[c.Symbol] {
					continue
				}
				if dayIdx == m.selectedDay && c.Symbol == m.selectedSymbol {
					return line
				}
				line++
			}
		}
		return -1
	}

	var primary, next dashboard.DayData
	if m.historyMode {
		primary, next = m.historyData, m.historyNextData
	} else {
		primary, next = m.todayData, m.nextData
	}

	if l := foundIn(primary, 0); l >= 0 {
		return l
	}
	if next.Label != "" {
		line++ // blank line between days
		if l := foundIn(next, 1); l >= 0 {
			return l
		}
	}
	return -1
}

// selectionAtLine returns the (day, symbol) at the given 0-based content line,
// or (-1, "") if the line is not a symbol row.
func (m *model) selectionAtLine(target int) (int, string) {
	line := 0
	scanDay := func(d dashboard.DayData, dayIdx int) (int, string) {
		line++ // day label
		if len(d.Tiers) == 0 {
			line++ // "(no matching symbols)"
			return -1, ""
		}
		for _, tier := range d.Tiers {
			if m.watchlistOnly {
				hasAny := false
				for _, c := range tier.Symbols {
					if m.watchlistSymbols[c.Symbol] {
						hasAny = true
						break
					}
				}
				if !hasAny {
					continue
				}
			}
			line += 3 // blank + tier header + col header
			for _, c := range tier.Symbols {
				if m.watchlistOnly && !m.watchlistSymbols[c.Symbol] {
					continue
				}
				if line == target {
					return dayIdx, c.Symbol
				}
				line++
			}
		}
		return -1, ""
	}

	var primary, next dashboard.DayData
	if m.historyMode {
		primary, next = m.historyData, m.historyNextData
	} else {
		primary, next = m.todayData, m.nextData
	}

	if day, sym := scanDay(primary, 0); sym != "" {
		return day, sym
	}
	if next.Label != "" {
		line++ // blank line between days
		if day, sym := scanDay(next, 1); sym != "" {
			return day, sym
		}
	}
	return -1, ""
}

// sortModeAtX maps an X coordinate click to a sort mode based on the column
// layout. Returns -1 if the click doesn't land on a sortable column (Trd/TO/Gain%).
// Column layout: prefix(16) + session block(64) [+ gap(2) + session block(64)].
// Within each session block: Open(7) sp High(7) sp Low(7) sp Close(7) sp Trd(6) sp TO(9) sp Gain%(7) sp Loss%(7).
func (m *model) sortModeAtX(x int) int {
	const prefix = 16    // "  #   Symbol    "
	const sessionW = 64  // one session block width
	const gap = 2        // "  " between sessions

	// Determine session layout from current data.
	var primary dashboard.DayData
	if m.historyMode {
		primary = m.historyData
	} else {
		primary = m.todayData
	}
	hasPre := primary.PreCount > 0
	hasReg := primary.RegCount > 0

	// hitColumn returns the column name for an x offset within a session block.
	// Trd: 32..37, TO: 39..47, Gain%: 49..55
	hitColumn := func(off int) string {
		if off >= 32 && off <= 37 {
			return "trd"
		}
		if off >= 39 && off <= 47 {
			return "to"
		}
		if off >= 49 && off <= 55 {
			return "gain"
		}
		return ""
	}

	if hasPre && hasReg {
		// PRE session: prefix .. prefix+sessionW-1
		if x >= prefix && x < prefix+sessionW {
			switch hitColumn(x - prefix) {
			case "trd":
				return dashboard.SortPreTrades
			case "to":
				return dashboard.SortPreTurnover
			case "gain":
				return dashboard.SortPreGain
			}
		}
		// REG session: prefix+sessionW+gap .. prefix+2*sessionW+gap-1
		regOff := prefix + sessionW + gap
		if x >= regOff && x < regOff+sessionW {
			switch hitColumn(x - regOff) {
			case "trd":
				return dashboard.SortRegTrades
			case "to":
				return dashboard.SortRegTurnover
			case "gain":
				return dashboard.SortRegGain
			}
		}
	} else if hasPre {
		if x >= prefix && x < prefix+sessionW {
			switch hitColumn(x - prefix) {
			case "trd":
				return dashboard.SortPreTrades
			case "to":
				return dashboard.SortPreTurnover
			case "gain":
				return dashboard.SortPreGain
			}
		}
	} else if hasReg {
		if x >= prefix && x < prefix+sessionW {
			switch hitColumn(x - prefix) {
			case "trd":
				return dashboard.SortRegTrades
			case "to":
				return dashboard.SortRegTurnover
			case "gain":
				return dashboard.SortRegGain
			}
		}
	}

	// News column: only clickable if news counts are loaded.
	if nc := m.newsCountCache[m.viewedDate()]; nc != nil {
		var newsStart int
		if hasPre && hasReg {
			newsStart = prefix + 2*sessionW + gap
		} else {
			newsStart = prefix + sessionW
		}
		if x >= newsStart && x < newsStart+5 {
			return dashboard.SortNews
		}
	}
	return -1
}

// ensureVisible scrolls the viewport so the selected line is visible.
func (m *model) ensureVisible() {
	line := m.selectedLine()
	if line < 0 {
		return
	}
	yOff := m.viewport.YOffset
	vpH := m.viewport.Height
	if line < yOff {
		m.viewport.SetYOffset(line)
	} else if line >= yOff+vpH {
		m.viewport.SetYOffset(line - vpH + 1)
	}
}

func (m *model) navigateHistory(delta int) tea.Cmd {
	if m.historyLoading {
		return nil
	}

	if !m.historyMode {
		if delta > 0 {
			return nil // already at live
		}
		if len(m.historyDates) == 0 {
			return nil
		}
		m.historyMode = true
		m.historyIdx = len(m.historyDates) - 1
	} else {
		newIdx := m.historyIdx + delta
		if newIdx >= len(m.historyDates) {
			// Back to live mode.
			m.historyMode = false
			m.selectedSymbol = "" // force reset in refreshLive
			m.refreshLive()
			m.viewport.SetContent(m.renderContent())
			m.viewport.GotoTop()
			return tea.Batch(m.maybeLoadNews(), m.loadNewsCounts(), m.reloadWatchlistIfNeeded())
		}
		if newIdx < 0 {
			return nil // at oldest date
		}
		m.historyIdx = newIdx
	}

	date := m.historyDates[m.historyIdx]
	m.tradingDate = date

	// Check cache first.
	if entry, ok := m.historyCache[date]; ok {
		if entry.sortMode != m.sortMode {
			dashboard.ResortDayData(&entry.data, m.sortMode)
			dashboard.ResortDayData(&entry.nextData, m.sortMode)
			entry.sortMode = m.sortMode
		}
		m.historyData = entry.data
		m.historyNextData = entry.nextData
		m.historyTierMap = entry.tierMap
		m.historyTrades = entry.trades
		m.resetSelection()
		if m.ready {
			m.viewport.SetContent(m.renderContent())
			m.viewport.GotoTop()
		}
		// Ensure 5-date buffer around current position.
		return tea.Batch(m.ensureBuffer(m.historyIdx), m.maybeLoadNews(), m.loadNewsCounts(), m.reloadWatchlistIfNeeded())
	}

	// Not cached — load asynchronously.
	m.historyLoading = true
	return m.loadHistoryCmd(date)
}

// nextDateFor returns the next history date after the given date, or "".
func (m *model) nextDateFor(date string) string {
	for i, d := range m.historyDates {
		if d == date && i+1 < len(m.historyDates) {
			return m.historyDates[i+1]
		}
	}
	return ""
}

func (m *model) loadHistoryCmd(date string) tea.Cmd {
	dataDir := m.dataDir
	loc := m.loc
	sortMode := m.sortMode
	nextDate := m.nextDateFor(date)

	// For the latest history date, provide live trades as next-day source.
	var liveTrades []store.TradeRecord
	if nextDate == "" && len(m.historyDates) > 0 && date == m.historyDates[len(m.historyDates)-1] {
		_, liveTrades = m.liveModel.TodaySnapshot()
		m.logger.Info("loadHistoryCmd: using live trades for next-day",
			"date", date, "nextDate", nextDate, "liveTrades", len(liveTrades),
			"lastHistDate", m.historyDates[len(m.historyDates)-1])
	} else {
		m.logger.Info("loadHistoryCmd: no live trades",
			"date", date, "nextDate", nextDate,
			"histLen", len(m.historyDates))
	}

	return func() tea.Msg {
		data, nextData, tierMap, trades, err := loadDateData(dataDir, date, nextDate, loc, sortMode, liveTrades)
		if err != nil {
			return historyLoadedMsg{date: date, err: err}
		}
		return historyLoadedMsg{date: date, data: data, nextData: nextData, tierMap: tierMap, trades: trades}
	}
}

// loadDateData loads history data for a date including the next day's trades.
// liveTrades, if non-nil, provides today's live trades to use as next-day data
// when the date is the latest history date (no next-date file on disk).
func loadDateData(dataDir, date, nextDate string, loc *time.Location, sortMode int, liveTrades []store.TradeRecord) (data, nextData dashboard.DayData, tierMap map[string]string, trades int, err error) {
	tierMap, err = dashboard.LoadTierMapForDate(dataDir, date)
	if err != nil {
		return
	}
	var recs []store.TradeRecord
	recs, err = dashboard.LoadHistoryTrades(dataDir, date)
	if err != nil {
		return
	}
	trades = len(recs)
	open930 := open930ETForDate(date, loc)
	data = dashboard.ComputeDayData(date, recs, tierMap, open930, sortMode)

	// Try loading next-day from history file, or fall back to live trades.
	var nextRecs []store.TradeRecord
	var nextDateLabel string
	if nextDate != "" {
		if loaded, e := dashboard.LoadHistoryTrades(dataDir, nextDate); e == nil && len(loaded) > 0 {
			nextRecs = loaded
			nextDateLabel = nextDate
		}
	} else if len(liveTrades) > 0 {
		nextRecs = liveTrades
		now := time.Now().In(loc)
		nextDateLabel = now.Format("2006-01-02")
	}

	if len(nextRecs) > 0 {
		// History view: only show post-market of current date (4PM–8PM ET).
		// At that point in time, the next day's pre-market hasn't happened.
		postEnd := postMarketEndET(date)
		var filtered []store.TradeRecord
		for i := range nextRecs {
			if nextRecs[i].Timestamp <= postEnd {
				filtered = append(filtered, nextRecs[i])
			}
		}
		if len(filtered) > 0 {
			nextOpen930 := open930ETForDate(nextDateLabel, loc)
			nextData = dashboard.ComputeDayData("NEXT: "+nextDateLabel, filtered, tierMap, nextOpen930, sortMode)
		}
	}
	return
}

// ensureBuffer ensures 5 dates are queued for preloading around the
// given index (5 back + 1 forward). Deduplicates against cache and
// existing queue. Starts the preload chain if not already running.
func (m *model) ensureBuffer(idx int) tea.Cmd {
	// Collect desired dates: 5 back + 1 forward from idx.
	queued := make(map[string]bool)
	for _, d := range m.preloadQueue {
		queued[d] = true
	}

	var toAdd []string
	for i := idx + 1; i >= idx-5; i-- {
		if i < 0 || i >= len(m.historyDates) {
			continue
		}
		date := m.historyDates[i]
		if _, ok := m.historyCache[date]; ok {
			continue
		}
		if queued[date] {
			continue
		}
		toAdd = append(toAdd, date)
	}

	if len(toAdd) > 0 {
		// Prepend to front of queue (highest priority).
		m.preloadQueue = append(toAdd, m.preloadQueue...)
	}

	// Start chain if not already running.
	if !m.preloadRunning {
		return m.preloadNext()
	}
	return nil
}

// preloadInitial queues the latest 5 history dates for preloading.
func (m *model) preloadInitial() tea.Cmd {
	if len(m.historyDates) == 0 {
		return nil
	}
	// Use ensureBuffer around the last index (as if viewing latest date).
	return m.ensureBuffer(len(m.historyDates) - 1)
}

// preloadNext pops the next date from the preload queue and starts loading it.
func (m *model) preloadNext() tea.Cmd {
	// Skip already-cached dates.
	for len(m.preloadQueue) > 0 {
		if _, ok := m.historyCache[m.preloadQueue[0]]; ok {
			m.preloadQueue = m.preloadQueue[1:]
			continue
		}
		break
	}

	if len(m.preloadQueue) == 0 {
		m.preloadRunning = false
		return nil
	}

	date := m.preloadQueue[0]
	m.preloadQueue = m.preloadQueue[1:]
	m.preloadRunning = true

	dataDir := m.dataDir
	loc := m.loc
	sortMode := m.sortMode
	nextDate := m.nextDateFor(date)

	var liveTrades []store.TradeRecord
	latestDate := ""
	if len(m.historyDates) > 0 {
		latestDate = m.historyDates[len(m.historyDates)-1]
	}
	if nextDate == "" && date == latestDate {
		_, liveTrades = m.liveModel.TodaySnapshot()
	}

	m.logger.Info("preload start", "date", date, "queued", len(m.preloadQueue), "cached", len(m.historyCache))

	return func() tea.Msg {
		data, nextData, tierMap, trades, err := loadDateData(dataDir, date, nextDate, loc, sortMode, liveTrades)
		if err != nil {
			return preloadedMsg{date: date, err: err}
		}
		return preloadedMsg{date: date, data: data, nextData: nextData, tierMap: tierMap, trades: trades}
	}
}

func (m *model) rebuildHistory() {
	if len(m.historyDates) == 0 || m.historyIdx < 0 || m.historyIdx >= len(m.historyDates) {
		return
	}
	date := m.historyDates[m.historyIdx]
	entry, ok := m.historyCache[date]
	if !ok {
		return
	}
	dashboard.ResortDayData(&entry.data, m.sortMode)
	dashboard.ResortDayData(&entry.nextData, m.sortMode)
	entry.sortMode = m.sortMode
	m.historyData = entry.data
	m.historyNextData = entry.nextData
}

func open930ETForDate(date string, loc *time.Location) int64 {
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

func (m *model) refreshLive() {
	_, todayExIdx := m.liveModel.TodaySnapshot()
	_, nextExIdx := m.liveModel.NextSnapshot()
	m.seen = m.liveModel.SeenCount()
	m.todayCount = len(todayExIdx)
	m.nextCount = len(nextExIdx)
	m.now = time.Now().In(m.loc)
	m.tradingDate = m.now.Format("2006-01-02")

	var maxTS int64
	for i := range todayExIdx {
		if todayExIdx[i].Timestamp > maxTS {
			maxTS = todayExIdx[i].Timestamp
		}
	}
	for i := range nextExIdx {
		if nextExIdx[i].Timestamp > maxTS {
			maxTS = nextExIdx[i].Timestamp
		}
	}
	if maxTS > 0 {
		m.latestTS = time.UnixMilli(maxTS).UTC().Format("15:04:05")
	} else {
		m.latestTS = "--:--:--"
	}

	todayOpen930 := time.Date(m.now.Year(), m.now.Month(), m.now.Day(), 9, 30, 0, 0, m.loc).UnixMilli()
	_, off := m.now.Zone()
	todayOpen930ET := todayOpen930 + int64(off)*1000
	nextOpen930ET := todayOpen930ET + 24*60*60*1000

	m.todayData = dashboard.ComputeDayData("TODAY", todayExIdx, m.tierMap, todayOpen930ET, m.sortMode)
	if len(nextExIdx) > 0 {
		m.nextData = dashboard.ComputeDayData("NEXT DAY", nextExIdx, m.tierMap, nextOpen930ET, m.sortMode)
	} else {
		m.nextData = dashboard.DayData{}
	}

	// Validate selection: reset if empty or no longer present.
	if m.selectedSymbol == "" {
		m.resetSelection()
	} else {
		entries := m.flatSelections()
		found := false
		for _, e := range entries {
			if e.day == m.selectedDay && e.symbol == m.selectedSymbol {
				found = true
				break
			}
		}
		if !found {
			m.resetSelection()
		}
	}
}

func (m model) View() string {
	if !m.ready {
		return "Loading..."
	}

	sortLabel := dashboard.SortModeLabel(m.sortMode)
	wlTag := ""
	if m.watchlistOnly {
		wlTag = " [WL]"
	}

	var headerBar string
	if m.historyMode {
		pos := fmt.Sprintf("%d/%d", m.historyIdx+1, len(m.historyDates))
		headerText := fmt.Sprintf(
			" Ex-Index History  %s    trades: %s    sort: %s%s    [%s] ",
			m.tradingDate,
			dashboard.FormatInt(m.historyTrades),
			sortLabel,
			wlTag,
			pos,
		)
		if m.historyLoading {
			headerText = fmt.Sprintf(" Ex-Index History  %s    loading... ", m.tradingDate)
		}
		headerBar = historyBarStyle.Render(padOrTrunc(headerText, m.width))
	} else {
		preloadInfo := ""
		if m.preloadRunning || len(m.preloadQueue) > 0 {
			preloadInfo = fmt.Sprintf("    hist: %d/%d", len(m.historyCache), len(m.historyDates))
		}
		headerText := fmt.Sprintf(
			" %s  %s ET    seen: %s  today: %s  next: %s    sort: %s%s%s ",
			m.tradingDate,
			m.latestTS,
			dashboard.FormatInt(m.seen),
			dashboard.FormatInt(m.todayCount),
			dashboard.FormatInt(m.nextCount),
			sortLabel,
			wlTag,
			preloadInfo,
		)
		headerBar = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("15")).
			Background(lipgloss.Color("4")).
			Render(padOrTrunc(headerText, m.width))
	}

	pct := m.viewport.ScrollPercent() * 100
	footerLeft := " q quit  s sort  w watchlist  left/right history  home live  up/dn select  space watch  pgup/dn scroll"
	footerRight := fmt.Sprintf("%.0f%% ", pct)
	gap := m.width - len(footerLeft) - len(footerRight)
	if gap < 0 {
		gap = 0
	}
	footerText := footerLeft + strings.Repeat(" ", gap) + footerRight
	footerBar := lipgloss.NewStyle().
		Foreground(lipgloss.Color("15")).
		Background(lipgloss.Color("8")).
		Render(padOrTrunc(footerText, m.width))

	return headerBar + "\n" + m.viewport.View() + "\n" + footerBar
}

func (m model) renderContent() string {
	var b strings.Builder
	selDay0 := ""
	selDay1 := ""
	if m.selectedDay == 0 {
		selDay0 = m.selectedSymbol
	} else {
		selDay1 = m.selectedSymbol
	}
	wl := m.watchlistSymbols
	wlOnly := m.watchlistOnly
	nc := m.newsCountCache[m.viewedDate()] // may be nil
	if m.historyMode {
		if m.historyLoading {
			b.WriteString(dimStyle.Render("  Loading..."))
			b.WriteString("\n")
		} else {
			renderDay(&b, m.historyData, m.width, selDay0, wl, wlOnly, nc, m.sortMode)
			if m.historyNextData.Label != "" {
				b.WriteString("\n")
				renderDay(&b, m.historyNextData, m.width, selDay1, wl, wlOnly, nc, m.sortMode)
			}
		}
	} else {
		renderDay(&b, m.todayData, m.width, selDay0, wl, wlOnly, nc, m.sortMode)
		if m.nextData.Label != "" {
			b.WriteString("\n")
			renderDay(&b, m.nextData, m.width, selDay1, wl, wlOnly, nc, m.sortMode)
		}
	}

	// News section for selected symbol.
	if sym := m.selectedSymbol; sym != "" {
		date := m.viewedDate()
		b.WriteString("\n")
		newsHeader := fmt.Sprintf("  NEWS: %s  %s", sym, date)
		b.WriteString(symbolWlStyle.Render(newsHeader))
		b.WriteString("\n")
		cacheKey := sym + ":" + date
		if articles, ok := m.newsCache[cacheKey]; ok {
			if len(articles) == 0 {
				b.WriteString(dimStyle.Render("  (no articles)"))
				b.WriteString("\n")
			}
			const newsIndent = "                  " // 18 chars: "  MM/DD HH:MM E  "
			bodyWidth := m.width - len(newsIndent)
			for _, a := range articles {
				ts := a.Time.In(m.loc).Format("01/02 15:04")
				headline := a.Headline
				maxHL := m.width - 18 // prefix width
				if maxHL > 0 && len(headline) > maxHL {
					headline = headline[:maxHL-3] + "..."
				}
				b.WriteString(dimStyle.Render("  "+ts+" ") + a.Source + dimStyle.Render(" ") + headline)
				b.WriteString("\n")
				if a.Content != "" && bodyWidth > 0 {
					for _, line := range wrapLines(a.Content, bodyWidth, 3) {
						b.WriteString(dimStyle.Render(newsIndent + line))
						b.WriteString("\n")
					}
				}
			}
		} else if m.newsLoading && m.newsSymbol == sym && m.newsDate == date {
			b.WriteString(dimStyle.Render("  Loading news..."))
			b.WriteString("\n")
		}
	}

	return b.String()
}

func renderDay(b *strings.Builder, d dashboard.DayData, width int, selectedSymbol string, watchlist map[string]bool, watchlistOnly bool, newsCounts map[string]int, sortMode int) {
	hasPre := d.PreCount > 0
	hasReg := d.RegCount > 0

	// Build day label with only non-zero session counts.
	labelParts := []string{"  " + d.Label}
	if hasPre {
		labelParts = append(labelParts, fmt.Sprintf("pre: %s", dashboard.FormatInt(d.PreCount)))
	}
	if hasReg {
		labelParts = append(labelParts, fmt.Sprintf("reg: %s", dashboard.FormatInt(d.RegCount)))
	}
	labelText := strings.Join(labelParts, "    ") + "  "
	b.WriteString(dayLabelStyle.Width(width).Render(labelText))
	b.WriteString("\n")

	if len(d.Tiers) == 0 {
		b.WriteString(dimStyle.Render("  (no matching symbols)"))
		b.WriteString("\n")
		return
	}

	// Sort by news count when SortNews is active.
	if sortMode == dashboard.SortNews && newsCounts != nil {
		for i := range d.Tiers {
			sort.SliceStable(d.Tiers[i].Symbols, func(a, b int) bool {
				na := newsCounts[d.Tiers[i].Symbols[a].Symbol]
				nb := newsCounts[d.Tiers[i].Symbols[b].Symbol]
				return na > nb
			})
		}
	}

	for _, tier := range d.Tiers {
		// Skip tier entirely if watchlistOnly and no visible symbols.
		if watchlistOnly {
			hasAny := false
			for _, c := range tier.Symbols {
				if watchlist[c.Symbol] {
					hasAny = true
					break
				}
			}
			if !hasAny {
				continue
			}
		}

		b.WriteString("\n")
		style := tierStyle(tier.Name)
		tierHeader := fmt.Sprintf(" %s  %s symbols ", tier.Name, dashboard.FormatInt(tier.Count))
		b.WriteString(style.Render(tierHeader))
		lineLen := width - len(tierHeader) - 1
		if lineLen > 0 {
			b.WriteString(dimStyle.Render(" " + strings.Repeat("─", lineLen)))
		}
		b.WriteString("\n")

		// Column headers: show PRE and/or REG based on data.
		sessionHdr := "%7s %7s %7s %7s %6s %9s %7s %7s"
		ncHdr := ""
		if newsCounts != nil {
			ncHdr = fmt.Sprintf(" %4s", "News")
		}
		var colLine string
		switch {
		case hasPre && hasReg:
			colLine = fmt.Sprintf(
				"  %-3s %-8s  "+sessionHdr+"  "+sessionHdr,
				"#", "Symbol",
				"Open", "High", "Low", "Close", "Trd", "TO", "Gain%", "Loss%",
				"Open", "High", "Low", "Close", "Trd", "TO", "Gain%", "Loss%",
			) + ncHdr
		default:
			colLine = fmt.Sprintf(
				"  %-3s %-8s  "+sessionHdr,
				"#", "Symbol",
				"Open", "High", "Low", "Close", "Trd", "TO", "Gain%", "Loss%",
			) + ncHdr
		}
		b.WriteString(colHeaderStyle.Render(colLine))
		b.WriteString("\n")

		displayNum := 0
		for _, c := range tier.Symbols {
			if watchlistOnly && !watchlist[c.Symbol] {
				continue
			}
			displayNum++
			hl := c.Symbol == selectedSymbol
			wlMark := " "
			if watchlist[c.Symbol] {
				wlMark = "*"
			}
			num := fmt.Sprintf(" %s%-3d", wlMark, displayNum)
			sym := fmt.Sprintf(" %-8s", c.Symbol)
			b.WriteString(hlStyle(dimStyle, hl).Render(num))
			inWl := watchlist[c.Symbol]
			symStyle := symbolStyle
			switch {
			case inWl && hl:
				symStyle = symbolWlHlStyle
			case inWl:
				symStyle = symbolWlStyle
			case hl:
				symStyle = symbolHlStyle
			}
			b.WriteString(hlStyle(symStyle, hl).Render(sym))
			b.WriteString(hlStyle(lipgloss.NewStyle(), hl).Render("  "))
			if hasPre && hasReg {
				writeSessionCols(b, c.Pre, hl, inWl)
				b.WriteString(hlStyle(lipgloss.NewStyle(), hl).Render("  "))
				writeSessionCols(b, c.Reg, hl, inWl)
			} else if hasPre {
				writeSessionCols(b, c.Pre, hl, inWl)
			} else {
				writeSessionCols(b, c.Reg, hl, inWl)
			}
			if newsCounts != nil {
				ncStr := fmt.Sprintf(" %4s", "-")
				if n, ok := newsCounts[c.Symbol]; ok && n > 0 {
					ncStr = fmt.Sprintf(" %4d", n)
				}
				b.WriteString(hlStyle(dimStyle, hl).Render(ncStr))
			}
			if hl {
				// Pad remaining width with highlight background.
				b.WriteString(lipgloss.NewStyle().Background(highlightBG).Render(" "))
			}
			b.WriteString("\n")
		}
	}
}

func writeSessionCols(b *strings.Builder, s *dashboard.SymbolStats, hl, inWl bool) {
	sp := hlStyle(lipgloss.NewStyle(), hl).Render(" ")
	if s == nil {
		b.WriteString(hlStyle(dimStyle, hl).Render(fmt.Sprintf(
			"%7s %7s %7s %7s %6s %9s %7s %7s",
			"—", "—", "—", "—", "—", "—", "—", "—")))
		return
	}

	openPad := fmt.Sprintf("%7s", dashboard.FormatPrice(s.Open))
	hiPad := fmt.Sprintf("%7s", dashboard.FormatPrice(s.High))
	loPad := fmt.Sprintf("%7s", dashboard.FormatPrice(s.Low))
	closePad := fmt.Sprintf("%7s", dashboard.FormatPrice(s.Close))
	trdPad := fmt.Sprintf("%6s", dashboard.FormatCount(s.Trades))
	toPad := fmt.Sprintf("%9s", dashboard.FormatTurnover(s.Turnover))
	gainPad := fmt.Sprintf("%7s", dashboard.FormatGain(s.MaxGain))
	lossPad := fmt.Sprintf("%7s", dashboard.FormatLoss(s.MaxLoss))

	ps := priceStyle
	if inWl {
		ps = priceWlStyle
	}
	b.WriteString(hlStyle(ps, hl).Render(openPad))
	b.WriteString(sp)
	b.WriteString(hlStyle(ps, hl).Render(hiPad))
	b.WriteString(sp)
	b.WriteString(hlStyle(ps, hl).Render(loPad))
	b.WriteString(sp)
	b.WriteString(hlStyle(ps, hl).Render(closePad))
	b.WriteString(sp)
	if s.Trades < 1000 {
		b.WriteString(hlStyle(dimStyle, hl).Render(trdPad))
	} else {
		b.WriteString(hlStyle(tradeCountStyle, hl).Render(trdPad))
	}
	b.WriteString(sp)
	if s.Turnover < 1e6 {
		b.WriteString(hlStyle(dimStyle, hl).Render(toPad))
	} else {
		b.WriteString(hlStyle(turnoverStyle, hl).Render(toPad))
	}
	b.WriteString(sp)
	gainWins := s.MaxGain >= s.MaxLoss
	if s.MaxGain < 0.10 {
		b.WriteString(hlStyle(dimStyle, hl).Render(gainPad))
	} else if gainWins {
		b.WriteString(hlStyle(gainStyle, hl).Render(gainPad))
	} else {
		b.WriteString(hlStyle(dimStyle, hl).Render(gainPad))
	}
	b.WriteString(sp)
	if s.MaxLoss < 0.10 {
		b.WriteString(hlStyle(dimStyle, hl).Render(lossPad))
	} else if !gainWins {
		b.WriteString(hlStyle(lossStyle, hl).Render(lossPad))
	} else {
		b.WriteString(hlStyle(dimStyle, hl).Render(lossPad))
	}
}

// padOrTrunc pads s with spaces to width, or truncates if longer.
var htmlTagRe = regexp.MustCompile(`<[^>]*>`)
var htmlParaRe = regexp.MustCompile(`(?i)</?(p|br|div|li|h[1-6])\b[^>]*>`)

// stripHTML removes HTML tags, decodes HTML entities, and collapses whitespace.
func stripHTML(s string) string {
	s = htmlTagRe.ReplaceAllString(s, " ")
	s = html.UnescapeString(s) // &nbsp; &amp; &lt; &#8217; etc.
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
}

// extractSymbolContent extracts paragraphs from HTML content that mention the
// given stock symbol. Falls back to the full stripped content if no paragraph
// matches. Multi-stock articles (Benzinga) typically have one <p> per stock.
func extractSymbolContent(html, symbol string) string {
	// Split on block-level tags into paragraph chunks.
	chunks := htmlParaRe.Split(html, -1)
	var matched []string
	upper := strings.ToUpper(symbol)
	for _, chunk := range chunks {
		plain := stripHTML(chunk)
		if plain == "" {
			continue
		}
		if strings.Contains(strings.ToUpper(plain), upper) {
			matched = append(matched, plain)
		}
	}
	if len(matched) > 0 {
		return strings.Join(matched, " ")
	}
	// No paragraph matched — return full content.
	return stripHTML(html)
}

// wrapLines wraps text to the given width, returning at most maxLines lines.
func wrapLines(text string, width, maxLines int) []string {
	if width <= 0 || maxLines <= 0 {
		return nil
	}
	var lines []string
	for len(text) > 0 && len(lines) < maxLines {
		if len(text) <= width {
			lines = append(lines, text)
			break
		}
		// Find last space within width.
		cut := width
		if i := strings.LastIndex(text[:width], " "); i > 0 {
			cut = i
		}
		lines = append(lines, text[:cut])
		text = strings.TrimLeft(text[cut:], " ")
	}
	// If there's remaining text after maxLines, add ellipsis to last line.
	if len(text) > 0 && len(lines) == maxLines {
		last := lines[len(lines)-1]
		if len(last) > 3 {
			lines[len(lines)-1] = last[:len(last)-3] + "..."
		}
	}
	return lines
}

func padOrTrunc(s string, width int) string {
	n := len(s)
	if n >= width {
		return s[:width]
	}
	return s + strings.Repeat(" ", width-n)
}

func main() {
	dataDir := os.Getenv("DATA_1")
	if dataDir == "" {
		fmt.Fprintln(os.Stderr, "DATA_1 environment variable not set")
		os.Exit(1)
	}

	addr := "localhost:50051"
	if a := os.Getenv("STREAM_ADDR"); a != "" {
		addr = a
	}

	logPath := fmt.Sprintf("/tmp/us-client-%s.log", time.Now().Format("2006-01-02"))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "opening log file: %v\n", err)
		os.Exit(1)
	}
	defer logFile.Close()
	logger := slog.New(slog.NewTextHandler(logFile, &slog.HandlerOptions{Level: slog.LevelInfo}))

	tierMap, err := dashboard.LoadTierMap(dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading tier map: %v\n", err)
		os.Exit(1)
	}
	logger.Info("loaded tier map", "symbols", len(tierMap))

	histDates, err := dashboard.ListHistoryDates(dataDir)
	if err != nil {
		logger.Warn("listing history dates", "error", err)
	}
	logger.Info("history dates available", "count", len(histDates))

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading timezone: %v\n", err)
		os.Exit(1)
	}
	now := time.Now().In(loc)
	close4pm := time.Date(now.Year(), now.Month(), now.Day(), 16, 0, 0, 0, loc)
	_, offset := close4pm.Zone()
	todayCutoff := close4pm.UnixMilli() + int64(offset)*1000

	lm := live.NewLiveModel(todayCutoff)
	client := live.NewClient(addr, lm, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := client.Sync(ctx); err != nil && ctx.Err() == nil {
			logger.Error("sync error", "error", err)
		}
	}()

	// Wait for initial snapshot to complete. First wait for the snapshot
	// burst to start and flow (count growing rapidly), then detect completion
	// when the rate drops (< 100 new trades per 100ms for 500ms).
	fmt.Fprint(os.Stderr, "syncing snapshot...")
	lastCount := 0
	stableFor := 0
	sawBurst := false
	for stableFor < 5 {
		time.Sleep(100 * time.Millisecond)
		count := lm.SeenCount()
		delta := count - lastCount
		if delta >= 100 {
			sawBurst = true
		}
		if sawBurst && delta < 100 {
			stableFor++
		} else {
			stableFor = 0
		}
		lastCount = count
	}
	fmt.Fprintf(os.Stderr, " %s trades\n", dashboard.FormatInt(lastCount))

	// Optional Alpaca trading client for watchlist support.
	var alpacaClient *alpacaapi.Client
	var mdClient *marketdata.Client
	if apiKey := os.Getenv("APCA_API_KEY_ID"); apiKey != "" {
		apiSecret := os.Getenv("APCA_API_SECRET_KEY")
		alpacaClient = alpacaapi.NewClient(alpacaapi.ClientOpts{
			APIKey:    apiKey,
			APISecret: apiSecret,
		})
		mdClient = marketdata.NewClient(marketdata.ClientOpts{
			APIKey:    apiKey,
			APISecret: apiSecret,
		})
		logger.Info("alpaca client initialized for watchlist and news")
	}

	p := tea.NewProgram(
		initialModel(lm, tierMap, loc, cancel, dataDir, histDates, logger, alpacaClient, mdClient),
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
