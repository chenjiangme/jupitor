package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	alpacaapi "github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"google.golang.org/grpc"

	"jupitor/internal/config"
	"jupitor/internal/dashboard"
	"jupitor/internal/gather/us"
	"jupitor/internal/httpapi"
	"jupitor/internal/live"
	"jupitor/internal/tradeparams"
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

	// Load tier map and history dates for HTTP API.
	tierMap, err := dashboard.LoadTierMap(cfg.Storage.DataDir)
	if err != nil {
		slog.Warn("loading tier map for HTTP API", "error", err)
		tierMap = make(map[string]string)
	}

	histDates, err := dashboard.ListHistoryDates(cfg.Storage.DataDir)
	if err != nil {
		slog.Warn("listing history dates for HTTP API", "error", err)
	}

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatalf("loading timezone: %v", err)
	}

	// Optional Alpaca clients for watchlist and live news support.
	var alpacaClient *alpacaapi.Client
	var mdClient *marketdata.Client
	if cfg.Alpaca.APIKey != "" {
		alpacaClient = alpacaapi.NewClient(alpacaapi.ClientOpts{
			APIKey:    cfg.Alpaca.APIKey,
			APISecret: cfg.Alpaca.APISecret,
		})
		mdClient = marketdata.NewClient(marketdata.ClientOpts{
			APIKey:    cfg.Alpaca.APIKey,
			APISecret: cfg.Alpaca.APISecret,
		})
	}

	// Create trade params store.
	targetFile := filepath.Join(cfg.Storage.DataDir, "us", "targets.json")
	tpStore := tradeparams.NewStore(targetFile, logger)

	// Start HTTP API server.
	httpAddr := ":8080"
	dashSrv := httpapi.NewDashboardServer(model, cfg.Storage.DataDir, loc, logger, tierMap, histDates, alpacaClient, mdClient, tpStore)
	dashSrv.Start(ctx)
	httpServer := &http.Server{
		Addr:    httpAddr,
		Handler: dashSrv.Handler(),
	}
	go func() {
		slog.Info("HTTP API server listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
		}
	}()

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

	// Graceful shutdown.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	httpServer.Shutdown(shutdownCtx)
	gs.GracefulStop()
	slog.Info("shutdown complete", "logFile", logFileName)
}
