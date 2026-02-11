// One-shot tool: generate filtered stock-trades parquet files for all
// consecutive trade-universe date pairs.
//
// Usage:
//
//	go run cmd/us-stock-trades/main.go [-n 5]
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"

	"jupitor/internal/config"
	"jupitor/internal/gather/us"
)

func main() {
	n := flag.Int("n", 0, "max number of dates to process (0 = all)")
	flag.Parse()

	cfgPath := "config/jupitor.yaml"
	if p := os.Getenv("JUPITOR_CONFIG"); p != "" {
		cfgPath = p
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	wrote, err := us.GenerateStockTrades(context.Background(), cfg.Storage.DataDir, *n, logger)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	if wrote == 0 {
		slog.Info("no new stock-trades files to generate")
	} else {
		slog.Info("stock trades generation complete", "files_written", wrote)
	}

	rollingWrote, err := us.GenerateRollingBars(context.Background(), cfg.Storage.DataDir, *n, logger)
	if err != nil {
		log.Fatalf("rolling bars error: %v", err)
	}

	if rollingWrote == 0 {
		slog.Info("no new rolling-bar files to generate")
	} else {
		slog.Info("rolling bars generation complete", "files_written", rollingWrote)
	}
}
