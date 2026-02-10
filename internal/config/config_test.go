package config

import (
	"os"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	// Create a temporary YAML config file.
	yamlContent := []byte(`
storage:
  data_dir: "/tmp/jupitor/data"
  sqlite_path: "/tmp/jupitor/jupitor.db"
server:
  host: "0.0.0.0"
  port: 8080
  grpc_port: 9090
alpaca:
  api_key: "test-key"
  api_secret: "test-secret"
  base_url: "https://paper-api.alpaca.markets"
  data_url: "https://data.alpaca.markets"
  stream_url: "wss://stream.data.alpaca.markets"
logging:
  level: "info"
  format: "json"
gather:
  us_daily:
    start_date: "2020-01-01"
    batch_size: 500
    rate_limit_per_min: 200
  us_trade:
    start_date: "2024-01-01"
    batch_size: 1000
    rate_limit_per_min: 100
  cn_daily:
    start_date: "2020-01-01"
    batch_size: 300
    rate_limit_per_min: 60
trading:
  max_position_pct: 0.1
  max_daily_loss_pct: 0.02
  paper_mode: true
`)

	tmpFile, err := os.CreateTemp("", "jupitor-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(yamlContent); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("failed to close temp file: %v", err)
	}

	// Clear any environment overrides that might interfere.
	os.Unsetenv("ALPACA_API_KEY")
	os.Unsetenv("ALPACA_API_SECRET")
	os.Unsetenv("APCA_API_KEY_ID")
	os.Unsetenv("APCA_API_SECRET_KEY")
	os.Unsetenv("DATA_DIR")

	cfg, err := Load(tmpFile.Name())
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	// -- Storage --
	if cfg.Storage.DataDir != "/tmp/jupitor/data" {
		t.Errorf("Storage.DataDir = %q, want %q", cfg.Storage.DataDir, "/tmp/jupitor/data")
	}
	if cfg.Storage.SQLitePath != "/tmp/jupitor/jupitor.db" {
		t.Errorf("Storage.SQLitePath = %q, want %q", cfg.Storage.SQLitePath, "/tmp/jupitor/jupitor.db")
	}

	// -- Server --
	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("Server.Host = %q, want %q", cfg.Server.Host, "0.0.0.0")
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("Server.Port = %d, want %d", cfg.Server.Port, 8080)
	}
	if cfg.Server.GRPCPort != 9090 {
		t.Errorf("Server.GRPCPort = %d, want %d", cfg.Server.GRPCPort, 9090)
	}

	// -- Alpaca --
	if cfg.Alpaca.APIKey != "test-key" {
		t.Errorf("Alpaca.APIKey = %q, want %q", cfg.Alpaca.APIKey, "test-key")
	}
	if cfg.Alpaca.APISecret != "test-secret" {
		t.Errorf("Alpaca.APISecret = %q, want %q", cfg.Alpaca.APISecret, "test-secret")
	}
	if cfg.Alpaca.BaseURL != "https://paper-api.alpaca.markets" {
		t.Errorf("Alpaca.BaseURL = %q, want %q", cfg.Alpaca.BaseURL, "https://paper-api.alpaca.markets")
	}

	// -- Logging --
	if cfg.Logging.Level != "info" {
		t.Errorf("Logging.Level = %q, want %q", cfg.Logging.Level, "info")
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("Logging.Format = %q, want %q", cfg.Logging.Format, "json")
	}

	// -- Gather --
	if cfg.Gather.USDaily.BatchSize != 500 {
		t.Errorf("Gather.USDaily.BatchSize = %d, want %d", cfg.Gather.USDaily.BatchSize, 500)
	}
	if cfg.Gather.USTrade.RateLimitPerMin != 100 {
		t.Errorf("Gather.USTrade.RateLimitPerMin = %d, want %d", cfg.Gather.USTrade.RateLimitPerMin, 100)
	}
	if cfg.Gather.CNDaily.StartDate != "2020-01-01" {
		t.Errorf("Gather.CNDaily.StartDate = %q, want %q", cfg.Gather.CNDaily.StartDate, "2020-01-01")
	}

	// -- Trading --
	if cfg.Trading.MaxPositionPct != 0.1 {
		t.Errorf("Trading.MaxPositionPct = %f, want %f", cfg.Trading.MaxPositionPct, 0.1)
	}
	if cfg.Trading.MaxDailyLossPct != 0.02 {
		t.Errorf("Trading.MaxDailyLossPct = %f, want %f", cfg.Trading.MaxDailyLossPct, 0.02)
	}
	if !cfg.Trading.PaperMode {
		t.Error("Trading.PaperMode = false, want true")
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	yamlContent := []byte(`
alpaca:
  api_key: "yaml-key"
  api_secret: "yaml-secret"
storage:
  data_dir: "/original/data"
`)

	tmpFile, err := os.CreateTemp("", "jupitor-config-env-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(yamlContent); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	tmpFile.Close()

	// Set environment overrides.
	os.Setenv("ALPACA_API_KEY", "env-key")
	os.Setenv("DATA_DIR", "/env/data")
	os.Unsetenv("APCA_API_KEY_ID")
	os.Unsetenv("APCA_API_SECRET_KEY")
	defer os.Unsetenv("ALPACA_API_KEY")
	defer os.Unsetenv("DATA_DIR")

	cfg, err := Load(tmpFile.Name())
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.Alpaca.APIKey != "env-key" {
		t.Errorf("Alpaca.APIKey = %q, want %q (env override)", cfg.Alpaca.APIKey, "env-key")
	}
	// api_secret should remain from YAML since no env override was set.
	if cfg.Alpaca.APISecret != "yaml-secret" {
		t.Errorf("Alpaca.APISecret = %q, want %q (from YAML)", cfg.Alpaca.APISecret, "yaml-secret")
	}
	if cfg.Storage.DataDir != "/env/data" {
		t.Errorf("Storage.DataDir = %q, want %q (env override)", cfg.Storage.DataDir, "/env/data")
	}
}
