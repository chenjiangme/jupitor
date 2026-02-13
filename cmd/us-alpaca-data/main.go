package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"jupitor/internal/config"
	"jupitor/internal/gather/us"
	"jupitor/internal/store"
)

func main() {
	exIndexOnly := flag.Bool("ex-index-only", false, "only backfill non-ETF, non-index (SPX/NDX) stocks")
	flag.Parse()

	cfgPath := "config/jupitor.yaml"
	if p := os.Getenv("JUPITOR_CONFIG"); p != "" {
		cfgPath = p
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Dual logger: stdout + /tmp log file.
	logFileName := fmt.Sprintf("/tmp/us-alpaca-data-%s.log", time.Now().Format("2006-01-02"))
	logFile, err := os.Create(logFileName)
	if err != nil {
		log.Fatalf("failed to create log file: %v", err)
	}
	defer logFile.Close()

	w := io.MultiWriter(os.Stdout, logFile)
	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	pstore := store.NewParquetStore(cfg.Storage.DataDir)

	csvPath := "reference/us/symbol_5_chars.csv"

	gatherer := us.NewDailyBarGatherer(
		cfg.Alpaca.APIKey,
		cfg.Alpaca.APISecret,
		cfg.Alpaca.DataURL,
		pstore,                        // barStore
		pstore,                        // tradeStore
		cfg.Gather.USDaily.BatchSize,  // bar batch size
		cfg.Gather.USDaily.MaxWorkers, // bar workers
		cfg.Gather.USTrade.MaxWorkers, // trade workers
		cfg.Gather.USDaily.StartDate,
		csvPath,
		cfg.Alpaca.BaseURL,
		"reference/us",
	)

	if *exIndexOnly {
		gatherer.SetExIndexOnly(true)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info("starting us-alpaca-data daemon", "logFile", logFileName, "exIndexOnly", *exIndexOnly)
	if err := gatherer.Run(ctx); err != nil {
		log.Fatalf("daemon error: %v", err)
	}
}
