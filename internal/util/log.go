// Package util provides shared utility functions for logging, retries, rate
// limiting, and trading calendar operations.
package util

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger creates a structured logger using log/slog at the specified
// level. Supported levels: "debug", "info", "warn", "error". Defaults to
// "info" if the level string is not recognised.
func NewLogger(level string) *slog.Logger {
	var slevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		slevel = slog.LevelDebug
	case "info":
		slevel = slog.LevelInfo
	case "warn":
		slevel = slog.LevelWarn
	case "error":
		slevel = slog.LevelError
	default:
		slevel = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slevel,
	})

	return slog.New(handler)
}

// SetDefault configures the provided logger as the default slog logger.
func SetDefault(logger *slog.Logger) {
	slog.SetDefault(logger)
}
