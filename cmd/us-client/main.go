package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

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
	gainStyle         = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))
	lossStyle         = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	colHeaderStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	dayLabelStyle     = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("0")).Background(lipgloss.Color("6"))
	dimStyle          = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	priceStyle        = lipgloss.NewStyle().Foreground(lipgloss.Color("15"))
	tradeCountStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("14"))
	turnoverStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("13"))
	historyBarStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("0")).Background(lipgloss.Color("3")) // black on yellow
)

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

// Messages.
type tickMsg time.Time
type syncErrMsg struct{ err error }

type historyCacheEntry struct {
	data     dashboard.DayData
	nextData dashboard.DayData
	tierMap  map[string]string
	trades   int
	sortMode int
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

	// Background preload tracking.
	preloadTotal int // total dates to preload
	preloadDone  int // completed preloads
}

func initialModel(lm *live.LiveModel, tierMap map[string]string, loc *time.Location, cancel context.CancelFunc, dataDir string, histDates []string, logger *slog.Logger) model {
	return model{
		liveModel:    lm,
		tierMap:      tierMap,
		loc:          loc,
		now:          time.Now().In(loc),
		syncCancel:   cancel,
		dataDir:      dataDir,
		historyDates: histDates,
		logger:       logger,
		historyCache: make(map[string]*historyCacheEntry),
	}
}

type preloadStartMsg struct{}

func (m model) Init() tea.Cmd {
	return tea.Batch(tickCmd(), func() tea.Msg { return preloadStartMsg{} })
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
		case "left":
			return m, m.navigateHistory(-1)
		case "right":
			return m, m.navigateHistory(1)
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
		if m.ready {
			m.viewport.SetContent(m.renderContent())
			m.viewport.GotoTop()
		}
		// Preload adjacent dates.
		return m, m.preloadAdjacent()

	case preloadedMsg:
		if msg.err != nil {
			m.logger.Warn("preload failed", "date", msg.date, "error", msg.err)
			m.preloadDone++
			return m, nil
		}
		m.historyCache[msg.date] = &historyCacheEntry{
			data: msg.data, nextData: msg.nextData, tierMap: msg.tierMap, trades: msg.trades,
			sortMode: m.sortMode,
		}
		m.preloadDone++
		m.logger.Info("preload cached", "date", msg.date, "trades", msg.trades,
			"progress", fmt.Sprintf("%d/%d", m.preloadDone, m.preloadTotal))
		if !m.historyMode && m.ready {
			m.viewport.SetContent(m.renderContent())
		}
		return m, nil

	case preloadStartMsg:
		return m, m.preloadAllHistory()

	case syncErrMsg:
		return m, tea.Quit
	}

	if m.ready {
		m.viewport, cmd = m.viewport.Update(msg)
	}
	return m, cmd
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
		if m.ready {
			m.viewport.SetContent(m.renderContent())
			m.viewport.GotoTop()
		}
		// Still preload adjacent.
		return m.preloadAdjacent()
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
		// Only include pre-market trades (before 9:30 AM) to simulate
		// the live "next day" view (post-market + overnight + pre-market).
		nextOpen930 := open930ETForDate(nextDateLabel, loc)
		var preOnly []store.TradeRecord
		for i := range nextRecs {
			if nextRecs[i].Timestamp < nextOpen930 {
				preOnly = append(preOnly, nextRecs[i])
			}
		}
		if len(preOnly) > 0 {
			nextData = dashboard.ComputeDayData("NEXT: "+nextDateLabel, preOnly, tierMap, nextOpen930, sortMode)
		}
	}
	return
}

func (m *model) preloadAdjacent() tea.Cmd {
	var cmds []tea.Cmd
	latestDate := ""
	if len(m.historyDates) > 0 {
		latestDate = m.historyDates[len(m.historyDates)-1]
	}

	// Preload 5 dates back (older) and 1 forward (newer) from current position.
	var indices []int
	for i := m.historyIdx - 5; i <= m.historyIdx+1; i++ {
		if i != m.historyIdx && i >= 0 && i < len(m.historyDates) {
			indices = append(indices, i)
		}
	}

	var preloadDates []string
	for _, idx := range indices {
		date := m.historyDates[idx]
		if _, ok := m.historyCache[date]; ok {
			continue // already cached
		}
		preloadDates = append(preloadDates, date)
		dataDir := m.dataDir
		loc := m.loc
		sortMode := m.sortMode
		nextDate := m.nextDateFor(date)

		var liveTrades []store.TradeRecord
		if nextDate == "" && date == latestDate {
			_, liveTrades = m.liveModel.TodaySnapshot()
		}

		cmds = append(cmds, func() tea.Msg {
			m.logger.Info("preload started", "date", date)
			data, nextData, tierMap, trades, err := loadDateData(dataDir, date, nextDate, loc, sortMode, liveTrades)
			if err != nil {
				return preloadedMsg{date: date, err: err}
			}
			m.logger.Info("preload done", "date", date, "trades", trades)
			return preloadedMsg{date: date, data: data, nextData: nextData, tierMap: tierMap, trades: trades}
		})
	}

	m.logger.Info("preloadAdjacent", "idx", m.historyIdx, "date", m.historyDates[m.historyIdx],
		"toPreload", preloadDates, "cached", len(m.historyCache))

	if len(cmds) == 0 {
		return nil
	}
	return tea.Batch(cmds...)
}

// preloadAllHistory preloads all history dates in the background.
// Called at startup so history navigation is instant.
func (m *model) preloadAllHistory() tea.Cmd {
	if len(m.historyDates) == 0 {
		return nil
	}

	latestDate := m.historyDates[len(m.historyDates)-1]

	var cmds []tea.Cmd
	for i := len(m.historyDates) - 1; i >= 0; i-- {
		date := m.historyDates[i]
		if _, ok := m.historyCache[date]; ok {
			continue
		}
		dataDir := m.dataDir
		loc := m.loc
		sortMode := m.sortMode
		nextDate := m.nextDateFor(date)

		var liveTrades []store.TradeRecord
		if nextDate == "" && date == latestDate {
			_, liveTrades = m.liveModel.TodaySnapshot()
		}

		cmds = append(cmds, func() tea.Msg {
			data, nextData, tierMap, trades, err := loadDateData(dataDir, date, nextDate, loc, sortMode, liveTrades)
			if err != nil {
				return preloadedMsg{date: date, err: err}
			}
			return preloadedMsg{date: date, data: data, nextData: nextData, tierMap: tierMap, trades: trades}
		})
	}

	m.preloadTotal = len(cmds)
	m.preloadDone = 0
	m.logger.Info("preloadAllHistory started", "dates", len(cmds))

	if len(cmds) == 0 {
		return nil
	}
	return tea.Batch(cmds...)
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
		if m.preloadTotal > 0 && m.preloadDone < m.preloadTotal {
			preloadInfo = fmt.Sprintf("    hist: %d/%d", m.preloadDone, m.preloadTotal)
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
	footerLeft := " q quit  s sort  left/right history  up/down scroll"
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
	if m.historyMode {
		if m.historyLoading {
			b.WriteString(dimStyle.Render("  Loading..."))
			b.WriteString("\n")
		} else {
			renderDay(&b, m.historyData, m.width)
			if m.historyNextData.Label != "" {
				b.WriteString("\n")
				renderDay(&b, m.historyNextData, m.width)
			}
		}
	} else {
		renderDay(&b, m.todayData, m.width)
		if m.nextData.Label != "" {
			b.WriteString("\n")
			renderDay(&b, m.nextData, m.width)
		}
	}
	return b.String()
}

func renderDay(b *strings.Builder, d dashboard.DayData, width int) {
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
			num := fmt.Sprintf("  %-3d", i+1)
			sym := fmt.Sprintf(" %-8s", c.Symbol)
			b.WriteString(dimStyle.Render(num))
			b.WriteString(symbolStyle.Render(sym))
			b.WriteString("  ")
			if hasPre && hasReg {
				writeSessionCols(b, c.Pre)
				b.WriteString("  ")
				writeSessionCols(b, c.Reg)
			} else if hasPre {
				writeSessionCols(b, c.Pre)
			} else {
				writeSessionCols(b, c.Reg)
			}
			b.WriteString("\n")
		}
	}
}

func writeSessionCols(b *strings.Builder, s *dashboard.SymbolStats) {
	if s == nil {
		b.WriteString(dimStyle.Render(fmt.Sprintf(
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

	b.WriteString(priceStyle.Render(openPad))
	b.WriteString(" ")
	b.WriteString(priceStyle.Render(hiPad))
	b.WriteString(" ")
	b.WriteString(priceStyle.Render(loPad))
	b.WriteString(" ")
	b.WriteString(priceStyle.Render(closePad))
	b.WriteString(" ")
	if s.Trades < 1000 {
		b.WriteString(dimStyle.Render(trdPad))
	} else {
		b.WriteString(tradeCountStyle.Render(trdPad))
	}
	b.WriteString(" ")
	if s.Turnover < 1e6 {
		b.WriteString(dimStyle.Render(toPad))
	} else {
		b.WriteString(turnoverStyle.Render(toPad))
	}
	b.WriteString(" ")
	gainWins := s.MaxGain >= s.MaxLoss
	if s.MaxGain < 0.10 {
		b.WriteString(dimStyle.Render(gainPad))
	} else if gainWins {
		b.WriteString(gainStyle.Render(gainPad))
	} else {
		b.WriteString(dimStyle.Render(gainPad))
	}
	b.WriteString(" ")
	if s.MaxLoss < 0.10 {
		b.WriteString(dimStyle.Render(lossPad))
	} else if !gainWins {
		b.WriteString(lossStyle.Render(lossPad))
	} else {
		b.WriteString(dimStyle.Render(lossPad))
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

	p := tea.NewProgram(
		initialModel(lm, tierMap, loc, cancel, dataDir, histDates, logger),
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
