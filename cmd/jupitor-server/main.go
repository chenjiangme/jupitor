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

	fmt.Printf("jupitor-server starting on %s:%d...\n", cfg.Server.Host, cfg.Server.Port)

	// TODO: Initialize ParquetStore and SQLiteStore from cfg.Storage.
	// TODO: Initialize API server (REST + gRPC) with store dependencies.
	// TODO: Initialize WebSocket hub for streaming data.
	// TODO: Start HTTP/gRPC listeners and block until signal.
}
