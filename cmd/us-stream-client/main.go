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
	tierActiveStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("10")) // green
	tierModerateStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("11")) // yellow
	tierSporadicStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("9"))  // red
	symbolStyle       = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("12")) // bright blue
	gainStyle         = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))             // green
	lossStyle         = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))              // red
	headerStyle       = lipgloss.NewStyle().Bold(true)
	colHeaderStyle    = lipgloss.NewStyle().Bold(true).Underline(true)
	footerStyle       = lipgloss.NewStyle().Faint(true)
	dayLabelStyle     = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("14")) // cyan
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

func tickCmd() tea.Cmd {
	return tea.Tick(5*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// Model.
type model struct {
	liveModel     *live.LiveModel
	tierMap       map[string]string
	loc           *time.Location
	todayData     dashboard.DayData
	nextData      dashboard.DayData
	seen          int
	todayCount    int
	nextCount     int
	now           time.Time
	sortByRegular bool
	viewport      viewport.Model
	ready         bool
	width, height int
	syncCancel    context.CancelFunc
}

func initialModel(lm *live.LiveModel, tierMap map[string]string, loc *time.Location, cancel context.CancelFunc) model {
	return model{
		liveModel:  lm,
		tierMap:    tierMap,
		loc:        loc,
		now:        time.Now().In(loc),
		syncCancel: cancel,
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
			m.refreshData()
			m.viewport.SetContent(m.renderContent())
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
			m.refreshData()
			m.viewport.SetContent(m.renderContent())
		} else {
			m.viewport.Width = m.width
			m.viewport.Height = vpHeight
		}
		return m, nil

	case tickMsg:
		m.refreshData()
		if m.ready {
			m.viewport.SetContent(m.renderContent())
		}
		return m, tickCmd()

	case syncErrMsg:
		return m, tea.Quit
	}

	if m.ready {
		m.viewport, cmd = m.viewport.Update(msg)
	}
	return m, cmd
}

func (m *model) refreshData() {
	_, todayExIdx := m.liveModel.TodaySnapshot()
	_, nextExIdx := m.liveModel.NextSnapshot()
	m.seen = m.liveModel.SeenCount()
	m.todayCount = len(todayExIdx)
	m.nextCount = len(nextExIdx)
	m.now = time.Now().In(m.loc)

	todayOpen930 := time.Date(m.now.Year(), m.now.Month(), m.now.Day(), 9, 30, 0, 0, m.loc).UnixMilli()
	_, off := m.now.Zone()
	todayOpen930ET := todayOpen930 + int64(off)*1000
	nextOpen930ET := todayOpen930ET + 24*60*60*1000

	m.todayData = dashboard.ComputeDayData("TODAY", todayExIdx, m.tierMap, todayOpen930ET, m.sortByRegular)
	if len(nextExIdx) > 0 {
		m.nextData = dashboard.ComputeDayData("NEXT DAY", nextExIdx, m.tierMap, nextOpen930ET, m.sortByRegular)
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

	header := headerStyle.Render(fmt.Sprintf(
		"Live Ex-Index Dashboard — %s    (seen: %s  today: %s  next: %s)    [sort: %s]",
		m.now.Format("2006-01-02 15:04:05 MST"),
		dashboard.FormatInt(m.seen),
		dashboard.FormatInt(m.todayCount),
		dashboard.FormatInt(m.nextCount),
		sortLabel,
	))

	pct := m.viewport.ScrollPercent() * 100
	footer := footerStyle.Render(fmt.Sprintf(
		"  q quit | s toggle sort | arrows/pgup/pgdn scroll    %.0f%%", pct,
	))

	return header + "\n" + m.viewport.View() + "\n" + footer
}

func (m model) renderContent() string {
	var b strings.Builder
	renderDay(&b, m.todayData)
	if m.nextData.Label != "" {
		b.WriteString("\n")
		renderDay(&b, m.nextData)
	}
	return b.String()
}

func renderDay(b *strings.Builder, d dashboard.DayData) {
	b.WriteString(dayLabelStyle.Render(fmt.Sprintf(
		"========== %s (pre: %s  reg: %s) ==========",
		d.Label, dashboard.FormatInt(d.PreCount), dashboard.FormatInt(d.RegCount),
	)))
	b.WriteString("\n")

	for _, tier := range d.Tiers {
		b.WriteString("\n")
		style := tierStyle(tier.Name)
		b.WriteString(style.Render(tier.Name))
		b.WriteString(fmt.Sprintf("    %s symbols\n", dashboard.FormatInt(tier.Count)))

		b.WriteString(colHeaderStyle.Render(fmt.Sprintf(
			"  %-3s %-8s | %7s %7s %6s %6s %6s %9s | %7s %7s %6s %6s %6s %9s",
			"#", "Symbol",
			"preO", "preC", "Gain%", "Loss%", "Trd", "TO",
			"regO", "regC", "Gain%", "Loss%", "Trd", "TO",
		)))
		b.WriteString("\n")

		for i, c := range tier.Symbols {
			// Pad text first, then apply color (ANSI codes break width counting).
			num := fmt.Sprintf("  %-3d ", i+1)
			sym := fmt.Sprintf("%-8s", c.Symbol)
			b.WriteString(num)
			b.WriteString(symbolStyle.Render(sym))
			b.WriteString(" | ")
			writeSessionCols(b, c.Pre)
			b.WriteString(" | ")
			writeSessionCols(b, c.Reg)
			b.WriteString("\n")
		}
	}
}

func writeSessionCols(b *strings.Builder, s *dashboard.SymbolStats) {
	if s == nil {
		b.WriteString(fmt.Sprintf("%7s %7s %6s %6s %6s %9s", "-", "-", "-", "-", "-", "-"))
		return
	}
	b.WriteString(fmt.Sprintf("%7s %7s ",
		dashboard.FormatPrice(s.Open),
		dashboard.FormatPrice(s.Close),
	))

	gain := dashboard.FormatGain(s.MaxGain)
	loss := dashboard.FormatLoss(s.MaxLoss)

	// Pad before coloring so width is correct.
	gainPad := fmt.Sprintf("%6s", gain)
	lossPad := fmt.Sprintf("%6s", loss)
	if gain != "" {
		b.WriteString(gainStyle.Render(gainPad))
	} else {
		b.WriteString(gainPad)
	}
	b.WriteString(" ")
	if loss != "" {
		b.WriteString(lossStyle.Render(lossPad))
	} else {
		b.WriteString(lossPad)
	}

	b.WriteString(fmt.Sprintf(" %6s %9s",
		dashboard.FormatInt(s.Trades),
		dashboard.FormatTurnover(s.Turnover),
	))
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

	// Logger writes to file since bubbletea owns stdout.
	logPath := fmt.Sprintf("/tmp/us-stream-client-%s.log", time.Now().Format("2006-01-02"))
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

	// Start gRPC sync in background.
	go func() {
		if err := client.Sync(ctx); err != nil && ctx.Err() == nil {
			logger.Error("sync error", "error", err)
		}
	}()

	// Wait briefly for initial data.
	time.Sleep(2 * time.Second)

	p := tea.NewProgram(
		initialModel(lm, tierMap, loc, cancel),
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
