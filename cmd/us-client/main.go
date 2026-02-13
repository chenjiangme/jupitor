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
	data    dashboard.DayData
	tierMap map[string]string
	trades  int
}

type historyLoadedMsg struct {
	date    string
	data    dashboard.DayData
	tierMap map[string]string
	trades  int
	err     error
}

// preloadedMsg is like historyLoadedMsg but for background preloading.
// It only populates the cache; it doesn't update the view.
type preloadedMsg struct {
	date    string
	data    dashboard.DayData
	tierMap map[string]string
	trades  int
	err     error
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
	sortByRegular bool
	viewport      viewport.Model
	ready         bool
	width, height int
	syncCancel    context.CancelFunc
	logger        *slog.Logger

	// History mode.
	historyMode    bool
	historyDates   []string // sorted available dates
	historyIdx     int      // index into historyDates
	historyData    dashboard.DayData
	historyTierMap map[string]string
	historyTrades  int
	historyLoading bool
	historyCache   map[string]*historyCacheEntry
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

func (m model) Init() tea.Cmd {
	return tickCmd()
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
			m.sortByRegular = !m.sortByRegular
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
			data: msg.data, tierMap: msg.tierMap, trades: msg.trades,
		}
		m.historyData = msg.data
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
			return m, nil
		}
		m.historyCache[msg.date] = &historyCacheEntry{
			data: msg.data, tierMap: msg.tierMap, trades: msg.trades,
		}
		return m, nil

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
		m.historyData = entry.data
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

func (m *model) loadHistoryCmd(date string) tea.Cmd {
	dataDir := m.dataDir
	loc := m.loc
	sortByReg := m.sortByRegular
	return func() tea.Msg {
		tierMap, err := dashboard.LoadTierMapForDate(dataDir, date)
		if err != nil {
			return historyLoadedMsg{date: date, err: err}
		}
		trades, err := dashboard.LoadHistoryTrades(dataDir, date)
		if err != nil {
			return historyLoadedMsg{date: date, err: err}
		}
		open930 := open930ETForDate(date, loc)
		data := dashboard.ComputeDayData(date, trades, tierMap, open930, sortByReg)
		return historyLoadedMsg{date: date, data: data, tierMap: tierMap, trades: len(trades)}
	}
}

func (m *model) preloadAdjacent() tea.Cmd {
	var cmds []tea.Cmd
	for _, idx := range []int{m.historyIdx - 1, m.historyIdx + 1} {
		if idx < 0 || idx >= len(m.historyDates) {
			continue
		}
		date := m.historyDates[idx]
		if _, ok := m.historyCache[date]; ok {
			continue // already cached
		}
		dataDir := m.dataDir
		loc := m.loc
		sortByReg := m.sortByRegular
		cmds = append(cmds, func() tea.Msg {
			tierMap, err := dashboard.LoadTierMapForDate(dataDir, date)
			if err != nil {
				return preloadedMsg{date: date, err: err}
			}
			trades, err := dashboard.LoadHistoryTrades(dataDir, date)
			if err != nil {
				return preloadedMsg{date: date, err: err}
			}
			open930 := open930ETForDate(date, loc)
			data := dashboard.ComputeDayData(date, trades, tierMap, open930, sortByReg)
			return preloadedMsg{date: date, data: data, tierMap: tierMap, trades: len(trades)}
		})
	}
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
	dashboard.ResortDayData(&entry.data, m.sortByRegular)
	m.historyData = entry.data
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

	m.todayData = dashboard.ComputeDayData("TODAY", todayExIdx, m.tierMap, todayOpen930ET, m.sortByRegular)
	if len(nextExIdx) > 0 {
		m.nextData = dashboard.ComputeDayData("NEXT DAY", nextExIdx, m.tierMap, nextOpen930ET, false)
	} else {
		m.nextData = dashboard.DayData{}
	}
}

func (m model) View() string {
	if !m.ready {
		return "Loading..."
	}

	sortLabel := "PRE"
	if m.sortByRegular {
		sortLabel = "REG"
	}

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
		headerBar = historyBarStyle.Width(m.width).Render(headerText)
	} else {
		headerText := fmt.Sprintf(
			" Live Ex-Index  %s  latest: %s ET    seen: %s  today: %s  next: %s    sort: %s ",
			m.tradingDate,
			m.latestTS,
			dashboard.FormatInt(m.seen),
			dashboard.FormatInt(m.todayCount),
			dashboard.FormatInt(m.nextCount),
			sortLabel,
		)
		headerBar = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("15")).
			Background(lipgloss.Color("4")).
			Width(m.width).
			Render(headerText)
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
		Width(m.width).
		Render(footerText)

	return headerBar + "\n" + m.viewport.View() + "\n" + footerBar
}

func (m model) renderContent() string {
	var b strings.Builder
	if m.historyMode {
		if m.historyLoading {
			b.WriteString(dimStyle.Render("  Loading..."))
			b.WriteString("\n")
		} else {
			renderDay(&b, m.historyData, m.width, false)
		}
	} else {
		renderDay(&b, m.todayData, m.width, false)
		if m.nextData.Label != "" {
			b.WriteString("\n")
			renderDay(&b, m.nextData, m.width, true)
		}
	}
	return b.String()
}

func renderDay(b *strings.Builder, d dashboard.DayData, width int, preOnly bool) {
	labelText := fmt.Sprintf("  %s    pre: %s    reg: %s  ",
		d.Label, dashboard.FormatInt(d.PreCount), dashboard.FormatInt(d.RegCount))
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

		var colLine string
		if preOnly {
			colLine = fmt.Sprintf(
				"  %-3s %-8s  %7s %7s %7s %7s %6s %9s %7s %7s",
				"#", "Symbol",
				"Open", "High", "Low", "Close", "Trd", "TO", "Gain%", "Loss%",
			)
		} else {
			colLine = fmt.Sprintf(
				"  %-3s %-8s  %7s %7s %7s %7s %6s %9s %7s %7s  %7s %7s %7s %7s %6s %9s %7s %7s",
				"#", "Symbol",
				"Open", "High", "Low", "Close", "Trd", "TO", "Gain%", "Loss%",
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
			writeSessionCols(b, c.Pre)
			if !preOnly {
				b.WriteString("  ")
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
	b.WriteString(tradeCountStyle.Render(trdPad))
	b.WriteString(" ")
	b.WriteString(turnoverStyle.Render(toPad))
	b.WriteString(" ")
	gainWins := s.MaxGain >= s.MaxLoss
	if s.MaxGain > 0 {
		if gainWins {
			b.WriteString(gainStyle.Render(gainPad))
		} else {
			b.WriteString(dimStyle.Render(gainPad))
		}
	} else {
		b.WriteString(gainPad)
	}
	b.WriteString(" ")
	if s.MaxLoss > 0 {
		if !gainWins {
			b.WriteString(lossStyle.Render(lossPad))
		} else {
			b.WriteString(dimStyle.Render(lossPad))
		}
	} else {
		b.WriteString(lossPad)
	}
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

	// Wait for initial snapshot to complete (count stabilizes for 500ms).
	fmt.Fprint(os.Stderr, "syncing snapshot...")
	lastCount := 0
	stableFor := 0
	for stableFor < 5 {
		time.Sleep(100 * time.Millisecond)
		count := lm.SeenCount()
		if count > 0 && count == lastCount {
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
