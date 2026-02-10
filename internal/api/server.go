// Package api provides the HTTP and gRPC server for the jupitor platform,
// exposing market data, trading, and strategy management endpoints.
package api

import (
	"context"

	"jupitor/internal/config"
)

// Server is the main API server that hosts HTTP and gRPC endpoints.
type Server struct {
	cfg      *config.Config
	httpAddr string
	grpcAddr string
}

// NewServer creates a new Server configured from the given Config.
func NewServer(cfg *config.Config) *Server {
	return &Server{
		cfg: cfg,
	}
}

// ListenAndServe starts the HTTP and gRPC listeners and blocks until the
// context is cancelled or a fatal error occurs.
func (s *Server) ListenAndServe(_ context.Context) error {
	// TODO: configure HTTP router with handlers and middleware
	// TODO: start HTTP listener on s.httpAddr
	// TODO: start gRPC listener on s.grpcAddr
	return nil
}

// Shutdown performs a graceful shutdown of the HTTP and gRPC servers.
func (s *Server) Shutdown(_ context.Context) error {
	// TODO: signal HTTP and gRPC servers to stop accepting new connections
	// TODO: wait for in-flight requests to complete
	return nil
}
