package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	alpacaapi "github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
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

const watchlistName = "jupitor"

// Messages.
type tickMsg time.Time
type syncErrMsg struct{ err error }

type watchlistLoadedMsg struct {
	id      string
	symbols map[string]bool
	err     error
}

type watchlistToggleMsg struct {
	symbol string
	added  bool
	err    error
}

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
	watchlistID      string
	watchlistSymbols map[string]bool

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

func initialModel(lm *live.LiveModel, tierMap map[string]string, loc *time.Location, cancel context.CancelFunc, dataDir string, histDates []string, logger *slog.Logger, ac *alpacaapi.Client) model {
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
	}
}

type preloadStartMsg struct{}

func (m model) Init() tea.Cmd {
	cmds := []tea.Cmd{tickCmd(), func() tea.Msg { return preloadStartMsg{} }}
	if m.alpacaClient != nil {
		ac := m.alpacaClient
		cmds = append(cmds, func() tea.Msg {
			return loadWatchlist(ac)
		})
	}
	return tea.Batch(cmds...)
}

// loadWatchlist gets or creates the "jupitor" watchlist and returns its symbols.
func loadWatchlist(ac *alpacaapi.Client) watchlistLoadedMsg {
	lists, err := ac.GetWatchlists()
	if err != nil {
		return watchlistLoadedMsg{err: err}
	}
	for _, w := range lists {
		if w.Name == watchlistName {
			// GetWatchlists doesn't include assets; fetch the full watchlist.
			full, err := ac.GetWatchlist(w.ID)
			if err != nil {
				return watchlistLoadedMsg{err: err}
			}
			syms := make(map[string]bool, len(full.Assets))
			for _, a := range full.Assets {
				syms[a.Symbol] = true
			}
			return watchlistLoadedMsg{id: w.ID, symbols: syms}
		}
	}
	// Create it.
	w, err := ac.CreateWatchlist(alpacaapi.CreateWatchlistRequest{Name: watchlistName})
	if err != nil {
		return watchlistLoadedMsg{err: err}
	}
	return watchlistLoadedMsg{id: w.ID, symbols: make(map[string]bool)}
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
				return m, func() tea.Msg {
					_, err := ac.AddSymbolToWatchlist(wlID, alpacaapi.AddSymbolToWatchlistRequest{Symbol: sym})
					return watchlistToggleMsg{symbol: sym, added: true, err: err}
				}
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
			return m, nil
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
			return m, nil
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
		} else {
			m.viewport.Width = m.width
			m.viewport.Height = vpHeight
		}
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
		return m, m.ensureBuffer(m.historyIdx)

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
			m.logger.Warn("loading watchlist", "error", msg.err)
		} else {
			m.watchlistID = msg.id
			m.watchlistSymbols = msg.symbols
			m.logger.Info("watchlist loaded", "id", msg.id, "symbols", len(msg.symbols))
			if m.ready {
				m.viewport.SetContent(m.renderContent())
			}
		}
		return m, nil

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
			line += 3 // blank + tier header + col header
			for _, c := range tier.Symbols {
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
			return nil
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
		return m.ensureBuffer(m.historyIdx)
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

	var headerBar string
	if m.historyMode {
		pos := fmt.Sprintf("%d/%d", m.historyIdx+1, len(m.historyDates))
		headerText := fmt.Sprintf(
			" Ex-Index History  %s    trades: %s    sort: %s    [%s] ",
			m.tradingDate,
			dashboard.FormatInt(m.historyTrades),
			sortLabel,
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
			" %s  %s ET    seen: %s  today: %s  next: %s    sort: %s%s ",
			m.tradingDate,
			m.latestTS,
			dashboard.FormatInt(m.seen),
			dashboard.FormatInt(m.todayCount),
			dashboard.FormatInt(m.nextCount),
			sortLabel,
			preloadInfo,
		)
		headerBar = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("15")).
			Background(lipgloss.Color("4")).
			Render(padOrTrunc(headerText, m.width))
	}

	pct := m.viewport.ScrollPercent() * 100
	footerLeft := " q quit  s sort  left/right history  home live  up/dn select  space watch  pgup/dn scroll"
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
	if m.historyMode {
		if m.historyLoading {
			b.WriteString(dimStyle.Render("  Loading..."))
			b.WriteString("\n")
		} else {
			renderDay(&b, m.historyData, m.width, selDay0, wl)
			if m.historyNextData.Label != "" {
				b.WriteString("\n")
				renderDay(&b, m.historyNextData, m.width, selDay1, wl)
			}
		}
	} else {
		renderDay(&b, m.todayData, m.width, selDay0, wl)
		if m.nextData.Label != "" {
			b.WriteString("\n")
			renderDay(&b, m.nextData, m.width, selDay1, wl)
		}
	}
	return b.String()
}

func renderDay(b *strings.Builder, d dashboard.DayData, width int, selectedSymbol string, watchlist map[string]bool) {
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

	for _, tier := range d.Tiers {
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
		var colLine string
		switch {
		case hasPre && hasReg:
			colLine = fmt.Sprintf(
				"  %-3s %-8s  "+sessionHdr+"  "+sessionHdr,
				"#", "Symbol",
				"Open", "High", "Low", "Close", "Trd", "TO", "Gain%", "Loss%",
				"Open", "High", "Low", "Close", "Trd", "TO", "Gain%", "Loss%",
			)
		default:
			colLine = fmt.Sprintf(
				"  %-3s %-8s  "+sessionHdr,
				"#", "Symbol",
				"Open", "High", "Low", "Close", "Trd", "TO", "Gain%", "Loss%",
			)
		}
		b.WriteString(colHeaderStyle.Render(colLine))
		b.WriteString("\n")

		for i, c := range tier.Symbols {
			hl := c.Symbol == selectedSymbol
			wlMark := " "
			if watchlist[c.Symbol] {
				wlMark = "*"
			}
			num := fmt.Sprintf(" %s%-3d", wlMark, i+1)
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
				writeSessionCols(b, c.Pre, hl)
				b.WriteString(hlStyle(lipgloss.NewStyle(), hl).Render("  "))
				writeSessionCols(b, c.Reg, hl)
			} else if hasPre {
				writeSessionCols(b, c.Pre, hl)
			} else {
				writeSessionCols(b, c.Reg, hl)
			}
			if hl {
				// Pad remaining width with highlight background.
				b.WriteString(lipgloss.NewStyle().Background(highlightBG).Render(" "))
			}
			b.WriteString("\n")
		}
	}
}

func writeSessionCols(b *strings.Builder, s *dashboard.SymbolStats, hl bool) {
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

	b.WriteString(hlStyle(priceStyle, hl).Render(openPad))
	b.WriteString(sp)
	b.WriteString(hlStyle(priceStyle, hl).Render(hiPad))
	b.WriteString(sp)
	b.WriteString(hlStyle(priceStyle, hl).Render(loPad))
	b.WriteString(sp)
	b.WriteString(hlStyle(priceStyle, hl).Render(closePad))
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
	if apiKey := os.Getenv("APCA_API_KEY_ID"); apiKey != "" {
		alpacaClient = alpacaapi.NewClient(alpacaapi.ClientOpts{
			APIKey:    apiKey,
			APISecret: os.Getenv("APCA_API_SECRET_KEY"),
		})
		logger.Info("alpaca client initialized for watchlist")
	}

	p := tea.NewProgram(
		initialModel(lm, tierMap, loc, cancel, dataDir, histDates, logger, alpacaClient),
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
