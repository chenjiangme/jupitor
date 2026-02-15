// One-shot tool: check a symbol's trade counts across raw files, ex-index,
// and live stream to diagnose discrepancies.
//
// Usage:
//
//	go run cmd/us-check-symbol/main.go FSLY [2026-02-12]
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"jupitor/internal/live"
	"jupitor/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: us-check-symbol SYMBOL [DATE]")
		os.Exit(1)
	}
	sym := strings.ToUpper(os.Args[1])
	dataDir := os.Getenv("DATA_1")
	loc, _ := time.LoadLocation("America/New_York")
	now := time.Now().In(loc)
	date := now.Format("2006-01-02")
	if len(os.Args) > 2 {
		date = os.Args[2]
	}

	// Determine prev trading date (simple: skip weekends).
	d, _ := time.ParseInLocation("2006-01-02", date, loc)
	prev := d.AddDate(0, 0, -1)
	for prev.Weekday() == time.Saturday || prev.Weekday() == time.Sunday {
		prev = prev.AddDate(0, 0, -1)
	}
	prevDate := prev.Format("2006-01-02")

	fmt.Printf("=== %s on %s (prev=%s) ===\n\n", sym, date, prevDate)

	// --- Raw trade files ---
	fmt.Println("--- Raw trade files ($DATA_1/us/trades/) ---")
	for _, dt := range []string{prevDate, date} {
		path := filepath.Join(dataDir, "us", "trades", sym, dt+".parquet")
		records, err := parquet.ReadFile[store.TradeRecord](path)
		if err != nil {
			fmt.Printf("  %s: %v\n", dt, err)
			continue
		}
		var minTS, maxTS int64
		for i, r := range records {
			if i == 0 || r.Timestamp < minTS {
				minTS = r.Timestamp
			}
			if r.Timestamp > maxTS {
				maxTS = r.Timestamp
			}
		}
		fmt.Printf("  %s: %d trades  [%s .. %s]\n", dt, len(records),
			time.UnixMilli(minTS).In(loc).Format("15:04:05"),
			time.UnixMilli(maxTS).In(loc).Format("15:04:05"))
	}

	// --- Ex-index file (ET-shifted timestamps) ---
	fmt.Println("\n--- stock-trades-ex-index ---")
	exPath := filepath.Join(dataDir, "us", "stock-trades-ex-index", date+".parquet")
	allRecords, err := parquet.ReadFile[store.TradeRecord](exPath)
	if err != nil {
		fmt.Printf("  error: %v\n", err)
	} else {
		open930 := time.Date(d.Year(), d.Month(), d.Day(), 9, 30, 0, 0, loc)
		_, off := open930.Zone()
		open930ET := open930.UnixMilli() + int64(off)*1000

		var symRecs []store.TradeRecord
		for _, r := range allRecords {
			if r.Symbol == sym {
				symRecs = append(symRecs, r)
			}
		}
		pre, reg := 0, 0
		var minTS, maxTS int64
		for i, r := range symRecs {
			if i == 0 || r.Timestamp < minTS {
				minTS = r.Timestamp
			}
			if r.Timestamp > maxTS {
				maxTS = r.Timestamp
			}
			if r.Timestamp < open930ET {
				pre++
			} else {
				reg++
			}
		}
		fmt.Printf("  %d trades (pre=%d reg=%d)  [%s .. %s]\n",
			len(symRecs), pre, reg,
			time.UnixMilli(minTS).UTC().Format("15:04:05"),
			time.UnixMilli(maxTS).UTC().Format("15:04:05"))
	}

	// --- Live stream ---
	fmt.Println("\n--- Live stream (gRPC) ---")
	addr := "localhost:50051"
	if a := os.Getenv("STREAM_ADDR"); a != "" {
		addr = a
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	close4pm := time.Date(now.Year(), now.Month(), now.Day(), 16, 0, 0, 0, loc)
	_, offset := close4pm.Zone()
	todayCutoff := close4pm.UnixMilli() + int64(offset)*1000

	lm := live.NewLiveModel(todayCutoff)
	client := live.NewClient(addr, lm, logger)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = client.Sync(ctx) }()

	fmt.Fprintf(os.Stderr, "syncing...")
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
	fmt.Fprintf(os.Stderr, " done (%d seen)\n", lastCount)

	_, exIdx := lm.TodaySnapshot()
	open930 := time.Date(d.Year(), d.Month(), d.Day(), 9, 30, 0, 0, loc)
	_, off := open930.Zone()
	open930ET := open930.UnixMilli() + int64(off)*1000

	var liveSym []store.TradeRecord
	for _, r := range exIdx {
		if r.Symbol == sym {
			liveSym = append(liveSym, r)
		}
	}
	livePre, liveReg := 0, 0
	var liveMin, liveMax int64
	for i, r := range liveSym {
		if i == 0 || r.Timestamp < liveMin {
			liveMin = r.Timestamp
		}
		if r.Timestamp > liveMax {
			liveMax = r.Timestamp
		}
		if r.Timestamp < open930ET {
			livePre++
		} else {
			liveReg++
		}
	}
	fmt.Printf("  %d trades (pre=%d reg=%d)  [%s .. %s]\n",
		len(liveSym), livePre, liveReg,
		time.UnixMilli(liveMin).UTC().Format("15:04:05"),
		time.UnixMilli(liveMax).UTC().Format("15:04:05"))

	// --- Exchange breakdown ---
	fmt.Println("\n--- Exchange breakdown ---")
	type exchCount struct {
		exch     string
		pqCount  int
		livCount int
	}
	exchMap := make(map[string]*exchCount)
	for _, r := range allRecords {
		if r.Symbol != sym {
			continue
		}
		ec, ok := exchMap[r.Exchange]
		if !ok {
			ec = &exchCount{exch: r.Exchange}
			exchMap[r.Exchange] = ec
		}
		ec.pqCount++
	}
	for _, r := range liveSym {
		ec, ok := exchMap[r.Exchange]
		if !ok {
			ec = &exchCount{exch: r.Exchange}
			exchMap[r.Exchange] = ec
		}
		ec.livCount++
	}
	fmt.Printf("  %-6s %8s %8s %8s\n", "Exch", "PQ", "Live", "Delta")
	for _, ec := range exchMap {
		fmt.Printf("  %-6s %8d %8d %+8d\n", ec.exch, ec.pqCount, ec.livCount, ec.livCount-ec.pqCount)
	}
}
