// One-shot tool: backfill daily summary parquets from existing
// stock-trades-index + stock-trades-ex-index files.
//
// Usage:
//
//	go run cmd/us-daily-summary/main.go
package main

import (
	"context"
	"log"
	"log/slog"
	"os"

	"jupitor/internal/config"
	"jupitor/internal/gather/us"
)

func main() {
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

	wrote, err := us.GenerateDailySummaries(context.Background(), cfg.Storage.DataDir, 0, logger)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	if wrote == 0 {
		slog.Info("no new daily summaries to generate (all up to date)")
	} else {
		slog.Info("daily summary backfill complete", "wrote", wrote)
	}
}
