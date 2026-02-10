package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"jupitor/internal/config"
	"jupitor/internal/gather/us"
	"jupitor/internal/store"
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

	pstore := store.NewParquetStore(cfg.Storage.DataDir)

	gatherer := us.NewStreamGatherer(
		cfg.Alpaca.APIKey,
		cfg.Alpaca.APISecret,
		cfg.Alpaca.StreamURL,
		pstore,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("starting %s gatherer\n", gatherer.Name())
	if err := gatherer.Run(ctx); err != nil {
		log.Fatalf("gatherer error: %v", err)
	}
}
