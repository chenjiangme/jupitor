// One-shot tool: generate trade-universe CSVs for all complete dates.
//
// Usage:
//
//	go run cmd/us-trade-universe/main.go
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

	ref := us.LoadReferenceData("reference/us")

	wrote, err := us.GenerateTradeUniverse(context.Background(), cfg.Storage.DataDir, ref, logger)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	if !wrote {
		slog.Info("no new trade-universe CSVs to generate (all up to date or trades incomplete)")
	}
}
