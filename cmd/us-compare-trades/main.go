// One-shot tool: compare live stream trades (from gRPC) vs stock-trades-ex-index
// parquet for today, to find discrepancies.
//
// Usage:
//
//	go run cmd/us-compare-trades/main.go [date]
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	"strconv"

	"jupitor/internal/dashboard"
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

	loc, _ := time.LoadLocation("America/New_York")
	now := time.Now().In(loc)

	date := now.Format("2006-01-02")
	if len(os.Args) > 1 {
		date = os.Args[1]
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// --- Load live data from gRPC ---
	fmt.Fprintf(os.Stderr, "connecting to %s...\n", addr)
	close4pm := time.Date(now.Year(), now.Month(), now.Day(), 16, 0, 0, 0, loc)
	_, offset := close4pm.Zone()
	todayCutoff := close4pm.UnixMilli() + int64(offset)*1000

	lm := live.NewLiveModel(todayCutoff)
	client := live.NewClient(addr, lm, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = client.Sync(ctx)
	}()

	// Wait for snapshot to stabilize.
	fmt.Fprintf(os.Stderr, "syncing snapshot...")
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
	cancel()
	fmt.Fprintf(os.Stderr, " %d trades seen\n", lastCount)

	_, liveExIdx := lm.TodaySnapshot()

	// --- Load parquet data ---
	fmt.Fprintf(os.Stderr, "loading parquet for %s...\n", date)
	pqTrades, err := dashboard.LoadHistoryTrades(dataDir, date)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading parquet: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "parquet: %d trades\n", len(pqTrades))

	// --- Group by symbol ---
	liveBySymbol := groupBySymbol(liveExIdx)
	pqBySymbol := groupBySymbol(pqTrades)

	// --- Collect all symbols ---
	allSyms := make(map[string]bool)
	for s := range liveBySymbol {
		allSyms[s] = true
	}
	for s := range pqBySymbol {
		allSyms[s] = true
	}

	type diff struct {
		symbol    string
		liveCnt   int
		pqCnt     int
		delta     int
		deltaFrac float64
	}

	var onlyLive, onlyPQ []diff
	var both []diff

	for sym := range allSyms {
		lc := len(liveBySymbol[sym])
		pc := len(pqBySymbol[sym])
		d := diff{symbol: sym, liveCnt: lc, pqCnt: pc, delta: lc - pc}
		if pc > 0 {
			d.deltaFrac = float64(lc-pc) / float64(pc)
		}
		if lc == 0 {
			onlyPQ = append(onlyPQ, d)
		} else if pc == 0 {
			onlyLive = append(onlyLive, d)
		} else {
			both = append(both, d)
		}
	}

	sort.Slice(onlyLive, func(i, j int) bool { return onlyLive[i].liveCnt > onlyLive[j].liveCnt })
	sort.Slice(onlyPQ, func(i, j int) bool { return onlyPQ[i].pqCnt > onlyPQ[j].pqCnt })
	sort.Slice(both, func(i, j int) bool {
		ai, aj := abs(both[i].delta), abs(both[j].delta)
		return ai > aj
	})

	// --- Print summary ---
	fmt.Printf("\n=== COMPARISON: live vs parquet for %s ===\n", date)
	fmt.Printf("Live ex-index trades:    %d\n", len(liveExIdx))
	fmt.Printf("Parquet ex-index trades: %d\n", len(pqTrades))
	fmt.Printf("Live symbols:  %d\n", len(liveBySymbol))
	fmt.Printf("PQ symbols:    %d\n", len(pqBySymbol))
	fmt.Printf("Only in live:  %d\n", len(onlyLive))
	fmt.Printf("Only in PQ:    %d\n", len(onlyPQ))
	fmt.Printf("In both:       %d\n", len(both))

	if len(onlyLive) > 0 {
		fmt.Printf("\n--- Symbols ONLY in live (top 20) ---\n")
		fmt.Printf("  %-8s %8s\n", "Symbol", "Live")
		for i, d := range onlyLive {
			if i >= 20 {
				break
			}
			fmt.Printf("  %-8s %8d\n", d.symbol, d.liveCnt)
		}
	}

	if len(onlyPQ) > 0 {
		// Only show symbols with significant trades (>=1000).
		fmt.Printf("\n--- Symbols ONLY in parquet (trades >= 1000) ---\n")
		fmt.Printf("  %-8s %8s\n", "Symbol", "Trades")
		for _, d := range onlyPQ {
			if d.pqCnt < 1000 {
				break
			}
			fmt.Printf("  %-8s %8d\n", d.symbol, d.pqCnt)
		}
	}

	fmt.Printf("\n--- Biggest count differences (top 30) ---\n")
	fmt.Printf("  %-8s %8s %8s %8s %8s\n", "Symbol", "Live", "PQ", "Delta", "Delta%")
	for i, d := range both {
		if i >= 30 {
			break
		}
		pct := ""
		if d.pqCnt > 0 {
			pct = fmt.Sprintf("%+.1f%%", d.deltaFrac*100)
		}
		fmt.Printf("  %-8s %8d %8d %+8d %8s\n", d.symbol, d.liveCnt, d.pqCnt, d.delta, pct)
	}

	// --- Check timestamp ranges ---
	fmt.Printf("\n--- Timestamp range check (sample symbols) ---\n")
	sampleSyms := []string{}
	for i, d := range both {
		if i >= 5 {
			break
		}
		sampleSyms = append(sampleSyms, d.symbol)
	}
	for _, sym := range sampleSyms {
		lt := liveBySymbol[sym]
		pt := pqBySymbol[sym]
		lMin, lMax := tsRange(lt)
		pMin, pMax := tsRange(pt)
		fmt.Printf("  %-8s  live: [%s .. %s]  pq: [%s .. %s]\n",
			sym,
			fmtTS(lMin, loc), fmtTS(lMax, loc),
			fmtTS(pMin, loc), fmtTS(pMax, loc))
	}

	// --- Check trade ID uniqueness in parquet ---
	fmt.Printf("\n--- Trade ID uniqueness in parquet ---\n")
	type idKey struct {
		ID       int64
		Exchange string
	}
	seen := make(map[idKey]string, len(pqTrades))
	dupes := 0
	crossSymDupes := 0
	for _, r := range pqTrades {
		id, _ := strconv.ParseInt(r.ID, 10, 64)
		k := idKey{ID: id, Exchange: r.Exchange}
		if prev, ok := seen[k]; ok {
			dupes++
			if prev != r.Symbol {
				crossSymDupes++
				if crossSymDupes <= 10 {
					fmt.Printf("  CROSS-SYM DUPE: id=%d exch=%s sym1=%s sym2=%s\n",
						id, r.Exchange, prev, r.Symbol)
				}
			}
		} else {
			seen[k] = r.Symbol
		}
	}
	fmt.Printf("  Total records:       %d\n", len(pqTrades))
	fmt.Printf("  Unique (id,exch):    %d\n", len(seen))
	fmt.Printf("  Duplicate keys:      %d\n", dupes)
	fmt.Printf("  Cross-symbol dupes:  %d\n", crossSymDupes)
}

func groupBySymbol(trades []store.TradeRecord) map[string][]store.TradeRecord {
	m := make(map[string][]store.TradeRecord)
	for i := range trades {
		m[trades[i].Symbol] = append(m[trades[i].Symbol], trades[i])
	}
	return m
}

func tsRange(trades []store.TradeRecord) (min, max int64) {
	if len(trades) == 0 {
		return 0, 0
	}
	min, max = trades[0].Timestamp, trades[0].Timestamp
	for _, t := range trades[1:] {
		if t.Timestamp < min {
			min = t.Timestamp
		}
		if t.Timestamp > max {
			max = t.Timestamp
		}
	}
	return
}

func fmtTS(ms int64, loc *time.Location) string {
	if ms == 0 {
		return "--:--:--"
	}
	// Timestamps in live model are ET-shifted UTC millis, parquet also ET-shifted.
	// Just treat as UTC to show the ET time.
	return time.UnixMilli(ms).UTC().Format("15:04:05")
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
