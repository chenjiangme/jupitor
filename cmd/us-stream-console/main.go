package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"jupitor/internal/dashboard"
	"jupitor/internal/live"
)

// sortByRegular: 0 = sort by pre-market trades, 1 = sort by regular trades.
var sortByRegular atomic.Int32

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
	tierMap, err := dashboard.LoadTierMap(dataDir)
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

	// Channel for immediate refresh on key press.
	refreshCh := make(chan struct{}, 1)

	// Enable raw mode for 'q' to quit, 's' to toggle sort (non-fatal if not a terminal).
	restore, rawErr := enableRawMode()
	if rawErr != nil {
		logger.Warn("raw mode unavailable, q/s keys disabled", "error", rawErr)
	} else {
		defer restore()
		go func() {
			buf := make([]byte, 1)
			for {
				n, err := os.Stdin.Read(buf)
				if err != nil || n == 0 {
					return
				}
				switch buf[0] {
				case 'q', 'Q':
					cancel()
					return
				case 's', 'S':
					if sortByRegular.Load() == 0 {
						sortByRegular.Store(1)
					} else {
						sortByRegular.Store(0)
					}
					select {
					case refreshCh <- struct{}{}:
					default:
					}
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
		case <-refreshCh:
			printDashboard(model, tierMap, loc)
		case <-ctx.Done():
			fmt.Println("\nshutdown")
			return
		}
	}
}

func printDashboard(model *live.LiveModel, tierMap map[string]string, loc *time.Location) {
	_, todayExIdx := model.TodaySnapshot()
	_, nextExIdx := model.NextSnapshot()
	seen := model.SeenCount()

	now := time.Now().In(loc)
	todayOpen930 := time.Date(now.Year(), now.Month(), now.Day(), 9, 30, 0, 0, loc).UnixMilli()
	_, off := now.Zone()
	todayOpen930ET := todayOpen930 + int64(off)*1000
	nextOpen930ET := todayOpen930ET + 24*60*60*1000

	byReg := sortByRegular.Load() != 0
	sortLabel := "PRE"
	if byReg {
		sortLabel = "REG"
	}

	// Clear screen and print header.
	fmt.Print("\033[H\033[2J")
	fmt.Printf("Live Ex-Index Dashboard — %s    (seen: %s  today: %s  next: %s)    [sort: %s, press s to toggle]\n",
		now.Format("2006-01-02 15:04:05 MST"),
		dashboard.FormatInt(seen), dashboard.FormatInt(len(todayExIdx)), dashboard.FormatInt(len(nextExIdx)), sortLabel)

	todayData := dashboard.ComputeDayData("TODAY", todayExIdx, tierMap, todayOpen930ET, byReg)
	printDay(todayData, false)

	if len(nextExIdx) > 0 {
		nextData := dashboard.ComputeDayData("NEXT DAY", nextExIdx, tierMap, nextOpen930ET, false)
		printDay(nextData, true)
	}
}

func printDay(d dashboard.DayData, preOnly bool) {
	fmt.Printf("\n========== %s (pre: %s  reg: %s) ==========\n",
		d.Label, dashboard.FormatInt(d.PreCount), dashboard.FormatInt(d.RegCount))

	for _, tier := range d.Tiers {
		fmt.Printf("\n%s    %s symbols\n", tier.Name, dashboard.FormatInt(tier.Count))
		if preOnly {
			fmt.Printf("  %-3s %-8s | %7s %7s %7s %7s %6s %9s %7s %7s\n",
				"#", "Symbol",
				"Open", "High", "Low", "Close", "Trd", "TO", "Gain%", "Loss%")
		} else {
			fmt.Printf("  %-3s %-8s | %7s %7s %7s %7s %6s %9s %7s %7s | %7s %7s %7s %7s %6s %9s %7s %7s\n",
				"#", "Symbol",
				"Open", "High", "Low", "Close", "Trd", "TO", "Gain%", "Loss%",
				"Open", "High", "Low", "Close", "Trd", "TO", "Gain%", "Loss%")
		}

		for i, c := range tier.Symbols {
			if preOnly {
				fmt.Printf("  %-3d %-8s | %s\n",
					i+1, c.Symbol,
					formatSessionCols(c.Pre))
			} else {
				fmt.Printf("  %-3d %-8s | %s | %s\n",
					i+1, c.Symbol,
					formatSessionCols(c.Pre),
					formatSessionCols(c.Reg))
			}
		}
		fmt.Println()
	}
}

func formatSessionCols(s *dashboard.SymbolStats) string {
	if s == nil {
		return fmt.Sprintf("%7s %7s %7s %7s %6s %9s %7s %7s", "-", "-", "-", "-", "-", "-", "-", "-")
	}
	return fmt.Sprintf("%7s %7s %7s %7s %6s %9s %7s %7s",
		dashboard.FormatPrice(s.Open),
		dashboard.FormatPrice(s.High),
		dashboard.FormatPrice(s.Low),
		dashboard.FormatPrice(s.Close),
		dashboard.FormatCount(s.Trades),
		dashboard.FormatTurnover(s.Turnover),
		dashboard.FormatGain(s.MaxGain),
		dashboard.FormatLoss(s.MaxLoss))
}

// enableRawMode puts stdin into raw mode so single keypresses can be read.
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
