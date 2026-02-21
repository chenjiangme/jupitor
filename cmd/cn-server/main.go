package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"jupitor/internal/cnapi"
	"jupitor/internal/config"
	"jupitor/internal/store"
)

func main() {
	// Load config.
	cfgPath := "config/jupitor.yaml"
	if p := os.Getenv("JUPITOR_CONFIG"); p != "" {
		cfgPath = p
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("loading config: %v", err)
	}

	// Setup logging.
	logFileName := fmt.Sprintf("/tmp/cn-server-%s.log", time.Now().Format("2006-01-02"))
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("opening log file: %v", err)
	}
	defer logFile.Close()

	w := io.MultiWriter(os.Stdout, logFile)
	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Create store and server.
	ps := store.NewParquetStore(cfg.Storage.DataDir)
	srv := cnapi.NewCNServer(cfg.Storage.DataDir, ps, logger)

	if err := srv.Init(); err != nil {
		log.Fatalf("initializing CN server: %v", err)
	}

	// Start HTTP server.
	httpServer := &http.Server{
		Addr:    ":8081",
		Handler: srv.Handler(),
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		logger.Info("CN server listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down CN server")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "error", err)
	}
}
