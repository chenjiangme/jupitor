package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"jupitor/internal/config"
	"jupitor/internal/gather/cn"
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

	// TODO: Add baostock_host and baostock_port to GatherJobConfig so these
	// can be read from cfg.Gather.CNDaily instead of being hardcoded.
	// The config YAML has these fields but they are not mapped into the struct.
	bsHost := "www.baostock.com"
	bsPort := 10001

	client := cn.NewBaoStockClient(bsHost, bsPort)

	gatherer := cn.NewDailyBarGatherer(
		client,
		pstore,
		cfg.Gather.CNDaily.StartDate,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("starting %s gatherer\n", gatherer.Name())
	if err := gatherer.Run(ctx); err != nil {
		log.Fatalf("gatherer error: %v", err)
	}
}
