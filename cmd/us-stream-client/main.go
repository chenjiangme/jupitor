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

	// Compute today's cutoff = 4PM ET as Unix ms.
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		logger.Error("loading timezone", "error", err)
		os.Exit(1)
	}
	now := time.Now().In(loc)
	close4pm := time.Date(now.Year(), now.Month(), now.Day(), 16, 0, 0, 0, loc)
	todayCutoff := close4pm.UnixMilli()

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
	_, exIndex := model.TodaySnapshot()
	seen := model.SeenCount()

	stats := aggregateTrades(exIndex)

	// Group by tier.
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

	// Sort each tier by trade count descending.
	for _, ss := range tiers {
		sort.Slice(ss, func(i, j int) bool {
			return ss[i].Trades > ss[j].Trades
		})
	}

	// Clear screen and print.
	now := time.Now().In(loc)
	fmt.Print("\033[H\033[2J")
	fmt.Printf("Live Ex-Index Dashboard — %s    (seen: %s)\n\n",
		now.Format("2006-01-02 15:04:05 MST"), formatInt(seen))

	for _, tier := range []string{"ACTIVE", "MODERATE", "SPORADIC"} {
		ss := tiers[tier]
		fmt.Printf("%s (top 10 by trades)%stotal: %s symbols\n",
			tier, strings.Repeat(" ", 40-len(tier)-len("(top 10 by trades)")), formatInt(tierCounts[tier]))
		fmt.Printf("  %-3s %-8s %8s %8s %8s %8s %8s %8s %12s %7s\n",
			"#", "Symbol", "O", "H", "L", "C", "VWAP", "Trades", "Turnover", "Gain%")

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
			fmt.Printf("  %-3d %-8s %8s %8s %8s %8s %8s %8s %12s %7s\n",
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
