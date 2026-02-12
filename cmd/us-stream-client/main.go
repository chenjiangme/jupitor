package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"jupitor/internal/live"
	"jupitor/internal/store"
)

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

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Load tier map from latest trade-universe CSV.
	tierMap, err := loadTierMap(dataDir)
	if err != nil {
		logger.Error("loading tier map", "error", err)
		os.Exit(1)
	}
	logger.Info("loaded tier map", "symbols", len(tierMap))

	// Compute today's cutoff = 4PM ET in ET-shifted millisecond frame
	// (must match how the stream server stores timestamps via utcToETMilli).
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		logger.Error("loading timezone", "error", err)
		os.Exit(1)
	}
	now := time.Now().In(loc)
	close4pm := time.Date(now.Year(), now.Month(), now.Day(), 16, 0, 0, 0, loc)
	_, offset := close4pm.Zone()
	todayCutoff := close4pm.UnixMilli() + int64(offset)*1000

	model := live.NewLiveModel(todayCutoff)
	client := live.NewClient(addr, model, logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Start sync in background.
	go func() {
		if err := client.Sync(ctx); err != nil && ctx.Err() == nil {
			logger.Error("sync error", "error", err)
			cancel()
		}
	}()

	// Enable raw mode for 'q' to quit (non-fatal if not a terminal).
	restore, rawErr := enableRawMode()
	if rawErr != nil {
		logger.Warn("raw mode unavailable, q to quit disabled", "error", rawErr)
	} else {
		defer restore()
		go func() {
			buf := make([]byte, 1)
			for {
				n, err := os.Stdin.Read(buf)
				if err != nil || n == 0 {
					return
				}
				if buf[0] == 'q' || buf[0] == 'Q' {
					cancel()
					return
				}
			}
		}()
	}

	// Wait briefly for initial data, then start refresh loop.
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return
	}
	printDashboard(model, tierMap, loc)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			printDashboard(model, tierMap, loc)
		case <-ctx.Done():
			fmt.Println("\nshutdown")
			return
		}
	}
}

// symbolStats holds aggregated trade statistics for a single symbol.
type symbolStats struct {
	Symbol    string
	Trades    int
	High      float64
	Low       float64
	Open      float64 // first trade price (by timestamp)
	Close     float64 // last trade price (by timestamp)
	OpenTS    int64   // timestamp of first trade
	CloseTS   int64   // timestamp of last trade
	TotalSize int64
	Turnover  float64 // sum(price * size)
}

func printDashboard(model *live.LiveModel, tierMap map[string]string, loc *time.Location) {
	_, todayExIdx := model.TodaySnapshot()
	_, nextExIdx := model.NextSnapshot()
	seen := model.SeenCount()

	now := time.Now().In(loc)
	todayOpen930 := time.Date(now.Year(), now.Month(), now.Day(), 9, 30, 0, 0, loc).UnixMilli()
	_, off := now.Zone()
	todayOpen930ET := todayOpen930 + int64(off)*1000
	// Next day's 9:30 AM — post-market trades are all pre-market for next day.
	nextOpen930ET := todayOpen930ET + 24*60*60*1000

	// Clear screen and print header.
	fmt.Print("\033[H\033[2J")
	fmt.Printf("Live Ex-Index Dashboard — %s    (seen: %s  today: %s  next: %s)\n",
		now.Format("2006-01-02 15:04:05 MST"),
		formatInt(seen), formatInt(len(todayExIdx)), formatInt(len(nextExIdx)))

	// TODAY section.
	printDay("TODAY", todayExIdx, tierMap, todayOpen930ET)

	// NEXT DAY section.
	if len(nextExIdx) > 0 {
		printDay("NEXT DAY", nextExIdx, tierMap, nextOpen930ET)
	}
}

func printDay(label string, trades []store.TradeRecord, tierMap map[string]string, open930ET int64) {
	var preTrades, regTrades []store.TradeRecord
	for i := range trades {
		if trades[i].Timestamp < open930ET {
			preTrades = append(preTrades, trades[i])
		} else {
			regTrades = append(regTrades, trades[i])
		}
	}

	preStats := aggregateTrades(preTrades)
	regStats := aggregateTrades(regTrades)

	fmt.Printf("\n========== %s (pre: %s  reg: %s) ==========\n",
		label, formatInt(len(preTrades)), formatInt(len(regTrades)))

	for _, session := range []struct {
		name  string
		stats map[string]*symbolStats
	}{
		{"PRE-MARKET", preStats},
		{"REGULAR", regStats},
	} {
		if len(session.stats) == 0 {
			continue
		}
		fmt.Printf("\n--- %s ---\n", session.name)
		printSession(session.stats, tierMap)
	}
}

func printSession(stats map[string]*symbolStats, tierMap map[string]string) {
	tiers := map[string][]*symbolStats{
		"ACTIVE":   {},
		"MODERATE": {},
		"SPORADIC": {},
	}
	tierCounts := map[string]int{"ACTIVE": 0, "MODERATE": 0, "SPORADIC": 0}

	for sym, s := range stats {
		tier, ok := tierMap[sym]
		if !ok {
			continue
		}
		tiers[tier] = append(tiers[tier], s)
		tierCounts[tier]++
	}

	for _, ss := range tiers {
		sort.Slice(ss, func(i, j int) bool {
			return ss[i].Trades > ss[j].Trades
		})
	}

	for _, tier := range []string{"ACTIVE", "MODERATE", "SPORADIC"} {
		ss := tiers[tier]
		if tierCounts[tier] == 0 {
			continue
		}
		fmt.Printf("%s (top 10 by trades)%stotal: %s symbols\n",
			tier, strings.Repeat(" ", 40-len(tier)-len("(top 10 by trades)")), formatInt(tierCounts[tier]))
		fmt.Printf("  %-3s %-8s %8s %8s %8s %8s %8s %8s %12s %7s %7s\n",
			"#", "Symbol", "O", "H", "L", "C", "VWAP", "Trades", "Turnover", "Gain%", "Loss%")

		n := len(ss)
		if n > 10 {
			n = 10
		}
		for i := 0; i < n; i++ {
			s := ss[i]
			vwap := 0.0
			if s.TotalSize > 0 {
				vwap = s.Turnover / float64(s.TotalSize)
			}
			gain := ""
			if s.Open > 0 {
				gain = fmt.Sprintf("%+.1f%%", (s.High-s.Open)/s.Open*100)
			}
			loss := ""
			if s.Low > 0 {
				loss = fmt.Sprintf("-%.1f%%", (s.Open-s.Low)/s.Low*100)
			}
			fmt.Printf("  %-3d %-8s %8s %8s %8s %8s %8s %8s %12s %7s %7s\n",
				i+1,
				s.Symbol,
				formatPrice(s.Open),
				formatPrice(s.High),
				formatPrice(s.Low),
				formatPrice(s.Close),
				formatPrice(vwap),
				formatInt(s.Trades),
				formatTurnover(s.Turnover),
				gain,
				loss,
			)
		}
		fmt.Println()
	}
}

func aggregateTrades(records []store.TradeRecord) map[string]*symbolStats {
	m := make(map[string]*symbolStats)
	for i := range records {
		r := &records[i]
		s, ok := m[r.Symbol]
		if !ok {
			s = &symbolStats{
				Symbol:  r.Symbol,
				Low:     math.MaxFloat64,
				Open:    r.Price,
				OpenTS:  r.Timestamp,
				Close:   r.Price,
				CloseTS: r.Timestamp,
			}
			m[r.Symbol] = s
		}
		s.Trades++
		s.Turnover += r.Price * float64(r.Size)
		s.TotalSize += r.Size
		if r.Price > s.High {
			s.High = r.Price
		}
		if r.Price < s.Low {
			s.Low = r.Price
		}
		if r.Timestamp < s.OpenTS {
			s.OpenTS = r.Timestamp
			s.Open = r.Price
		}
		if r.Timestamp >= s.CloseTS {
			s.CloseTS = r.Timestamp
			s.Close = r.Price
		}
	}
	return m
}

// loadTierMap reads the latest trade-universe CSV and returns symbol→tier
// for ex-index stocks (non-empty tier field).
func loadTierMap(dataDir string) (map[string]string, error) {
	dir := filepath.Join(dataDir, "us", "trade-universe")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading trade-universe dir: %w", err)
	}

	// Find latest CSV by name (lexicographic = chronological for YYYY-MM-DD).
	var latest string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".csv") {
			continue
		}
		if e.Name() > latest {
			latest = e.Name()
		}
	}
	if latest == "" {
		return nil, fmt.Errorf("no trade-universe CSV files found in %s", dir)
	}

	path := filepath.Join(dir, latest)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	tierMap := make(map[string]string)
	scanner := bufio.NewScanner(f)
	first := true
	for scanner.Scan() {
		if first {
			first = false
			continue // skip header
		}
		fields := strings.Split(scanner.Text(), ",")
		if len(fields) < 5 {
			continue
		}
		tier := strings.TrimSpace(fields[4])
		if tier != "" {
			tierMap[fields[0]] = tier
		}
	}

	slog.Info("loaded trade-universe CSV", "file", latest, "exIndexSymbols", len(tierMap))
	return tierMap, scanner.Err()
}

func formatInt(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	start := len(s) % 3
	if start > 0 {
		b.WriteString(s[:start])
	}
	for i := start; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func formatTurnover(v float64) string {
	switch {
	case v >= 1e9:
		return fmt.Sprintf("$%.1fB", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("$%.1fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("$%.1fK", v/1e3)
	default:
		return fmt.Sprintf("$%.0f", v)
	}
}

func formatPrice(p float64) string {
	if p == math.MaxFloat64 || p == 0 {
		return "-"
	}
	return fmt.Sprintf("$%.2f", p)
}

// enableRawMode puts stdin into raw mode so single keypresses can be read.
// Returns a restore function that must be called on exit.
func enableRawMode() (restore func(), err error) {
	fd := int(os.Stdin.Fd())
	var orig syscall.Termios
	if _, _, e := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd),
		uintptr(syscall.TIOCGETA), uintptr(unsafe.Pointer(&orig)), 0, 0, 0); e != 0 {
		return nil, fmt.Errorf("TIOCGETA: %w", e)
	}
	raw := orig
	raw.Lflag &^= syscall.ICANON | syscall.ECHO
	raw.Cc[syscall.VMIN] = 1
	raw.Cc[syscall.VTIME] = 0
	if _, _, e := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd),
		uintptr(syscall.TIOCSETA), uintptr(unsafe.Pointer(&raw)), 0, 0, 0); e != 0 {
		return nil, fmt.Errorf("TIOCSETA: %w", e)
	}
	return func() {
		syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd),
			uintptr(syscall.TIOCSETA), uintptr(unsafe.Pointer(&orig)), 0, 0, 0)
	}, nil
}
