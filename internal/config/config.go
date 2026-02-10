package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// Configuration structs
// ---------------------------------------------------------------------------

// Config is the top-level configuration for the jupitor platform.
type Config struct {
	Storage Storage       `yaml:"storage"`
	Server  Server        `yaml:"server"`
	Alpaca  Alpaca        `yaml:"alpaca"`
	Logging Logging       `yaml:"logging"`
	Gather  GatherConfig  `yaml:"gather"`
	Trading TradingConfig `yaml:"trading"`
}

// Storage holds paths for data persistence.
type Storage struct {
	DataDir    string `yaml:"data_dir"`
	SQLitePath string `yaml:"sqlite_path"`
}

// Server holds network listener configuration.
type Server struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	GRPCPort int    `yaml:"grpc_port"`
}

// Alpaca holds credentials and endpoints for the Alpaca broker API.
type Alpaca struct {
	APIKey    string `yaml:"api_key"`
	APISecret string `yaml:"api_secret"`
	BaseURL   string `yaml:"base_url"`
	DataURL   string `yaml:"data_url"`
	StreamURL string `yaml:"stream_url"`
}

// Logging configures the application logger.
type Logging struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// GatherConfig controls data gathering behaviour for different markets and
// data types.
type GatherConfig struct {
	USDaily GatherJobConfig `yaml:"us_daily"`
	USTrade GatherJobConfig `yaml:"us_trade"`
	CNDaily GatherJobConfig `yaml:"cn_daily"`
}

// GatherJobConfig holds parameters for a single data gathering job.
type GatherJobConfig struct {
	StartDate       string `yaml:"start_date"`
	BatchSize       int    `yaml:"batch_size"`
	MaxWorkers      int    `yaml:"max_workers"`
	RateLimitPerMin int    `yaml:"rate_limit_per_min"`
}

// TradingConfig defines risk and execution parameters.
type TradingConfig struct {
	MaxPositionPct float64 `yaml:"max_position_pct"`
	MaxDailyLossPct float64 `yaml:"max_daily_loss_pct"`
	PaperMode       bool    `yaml:"paper_mode"`
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

// Load reads the YAML configuration file at the given path, parses it into a
// Config struct, and then applies environment variable overrides.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	applyEnvOverrides(cfg)

	return cfg, nil
}

// applyEnvOverrides checks well-known environment variables and overrides the
// corresponding configuration fields when they are set.
func applyEnvOverrides(cfg *Config) {
	// TODO: expand the set of supported environment variable overrides as the
	// configuration surface grows.

	if v := os.Getenv("DATA_1"); v != "" {
		cfg.Storage.DataDir = v
	}
	if v := os.Getenv("DATA_DIR"); v != "" {
		cfg.Storage.DataDir = v
	}

	if v := os.Getenv("SQLITE_PATH"); v != "" {
		cfg.Storage.SQLitePath = v
	}

	if v := os.Getenv("ALPACA_API_KEY"); v != "" {
		cfg.Alpaca.APIKey = v
	}

	if v := os.Getenv("ALPACA_API_SECRET"); v != "" {
		cfg.Alpaca.APISecret = v
	}

	if v := os.Getenv("ALPACA_BASE_URL"); v != "" {
		cfg.Alpaca.BaseURL = v
	}

	if v := os.Getenv("ALPACA_DATA_URL"); v != "" {
		cfg.Alpaca.DataURL = v
	}

	if v := os.Getenv("ALPACA_STREAM_URL"); v != "" {
		cfg.Alpaca.StreamURL = v
	}

	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.Logging.Level = v
	}

	// Standard Alpaca env vars (highest priority — canonical names used by SDK).
	if v := os.Getenv("APCA_API_KEY_ID"); v != "" {
		cfg.Alpaca.APIKey = v
	}
	if v := os.Getenv("APCA_API_SECRET_KEY"); v != "" {
		cfg.Alpaca.APISecret = v
	}
}
