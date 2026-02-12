package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"jupitor/internal/config"
	"jupitor/internal/gather/us"
	"jupitor/internal/live"
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

	// Dual logger: stdout + /tmp log file.
	logFileName := fmt.Sprintf("/tmp/us-stream-%s.log", time.Now().Format("2006-01-02"))
	logFile, err := os.Create(logFileName)
	if err != nil {
		log.Fatalf("failed to create log file: %v", err)
	}
	defer logFile.Close()

	w := io.MultiWriter(os.Stdout, logFile)
	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	gatherer := us.NewStreamGatherer(
		cfg.Alpaca.APIKey,
		cfg.Alpaca.APISecret,
		cfg.Alpaca.BaseURL,
		cfg.Storage.DataDir,
		"reference/us/symbol_5_chars.csv",
		"reference/us",
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Start the gatherer in a goroutine so we can also start the gRPC server.
	errCh := make(chan error, 1)
	go func() {
		errCh <- gatherer.Run(ctx)
	}()

	// Wait for the model to be ready (symbols loaded, WebSocket connected).
	select {
	case <-gatherer.Ready():
	case err := <-errCh:
		log.Fatalf("gatherer failed during startup: %v", err)
	}

	model := gatherer.Model()

	// Start gRPC server.
	grpcAddr := ":50051"
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", grpcAddr, err)
	}

	gs := grpc.NewServer()
	srv := live.NewServer(model, logger)
	srv.RegisterGRPC(gs)

	go func() {
		slog.Info("gRPC server listening", "addr", grpcAddr)
		if err := gs.Serve(lis); err != nil {
			slog.Error("gRPC server error", "error", err)
		}
	}()

	// Wait for gatherer to finish.
	if err := <-errCh; err != nil {
		slog.Error("gatherer error", "error", err)
	}

	gs.GracefulStop()
	slog.Info("shutdown complete", "logFile", logFileName)
}
