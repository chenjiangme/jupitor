package main

import (
	"fmt"
	"log"
	"os"

	"jupitor/internal/config"
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

	fmt.Printf("jupitor-trader starting (paper_mode=%v)...\n", cfg.Trading.PaperMode)

	// TODO: Initialize ParquetStore and SQLiteStore from cfg.Storage.
	// TODO: Initialize broker client (Alpaca) from cfg.Alpaca.
	// TODO: Initialize trading engine with strategy registry and risk limits.
	// TODO: Start engine event loop and block until signal.
}
