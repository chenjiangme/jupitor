# Jupitor

A unified financial data platform for US equities and China A-shares.

## Overview

Jupitor collects, stores, and visualizes market data across two markets:

- **US equities** — real-time and historical trades via Alpaca, with pre/post-market coverage for ex-index stocks
- **China A-shares** — daily, 30-min, and 5-min bars plus quarterly fundamentals via BaoStock

Go handles all core services (data collection, storage, streaming, APIs). Python handles analysis scripts and notebooks.

## Architecture

| Component | Description |
|-----------|-------------|
| `us-alpaca-data` | Long-running daemon — daily bar updates + historical trade backfill via Alpaca API |
| `us-stream` | Live trade streaming via WebSocket + REST backfill, gRPC + HTTP API |
| `us-client` | Terminal dashboard (bubbletea TUI) — live and historical ex-index trade view |
| `us-stock-trades` | Consolidates per-symbol trade files into per-date parquet files |
| iOS app | SwiftUI dashboard connected to the HTTP API |
| Python scripts | Analysis notebooks, index constituent builders, A-share data collection |

## Project Structure

```
cmd/            Go binaries (each runs as a separate daemon/tool)
internal/       Private Go packages (gather, live, dashboard, store, config, httpapi)
pkg/            Public Go client SDK
proto/          Protobuf definitions (gRPC services)
ios/            SwiftUI iPhone app
python/         Python analysis scripts and notebooks
config/         YAML configuration templates
migrations/     SQL migrations
reference/      Static reference data (CSVs)
```

## Prerequisites

- Go 1.24+
- Python 3.9+ (for analysis scripts)
- [Alpaca](https://alpaca.markets/) API account (for US market data)
- External data volume mounted at `$DATA_1` (e.g. `/Volumes/DATA 4TB 1`)

## Quick Start

```bash
# Build all binaries
make build

# Set environment variables
export DATA_1="/path/to/data/volume"
export APCA_API_KEY_ID="your-key"
export APCA_API_SECRET_KEY="your-secret"

# Run the daily bar + trade backfill daemon
bin/us-alpaca-data

# Run the live stream server (gRPC :50051, HTTP :8080)
bin/us-stream

# Connect the TUI dashboard
bin/us-client
```

## HTTP API

The `us-stream` server exposes a REST API on port 8080:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/dashboard` | Live dashboard data |
| GET | `/api/dashboard/history/{date}` | Historical dashboard for a date |
| GET | `/api/dates` | List available history dates |
| GET | `/api/watchlist` | Get watchlist symbols |
| PUT | `/api/watchlist/{symbol}` | Add symbol to watchlist |
| DELETE | `/api/watchlist/{symbol}` | Remove symbol from watchlist |
| GET | `/api/news/{symbol}` | News articles for a symbol |

## Storage

All time-series data is stored as Parquet files under `$DATA_1`:

```
$DATA_1/us/daily/<SYMBOL>/<YYYY>.parquet           # daily bars
$DATA_1/us/trades/<SYMBOL>/<YYYY-MM-DD>.parquet     # per-symbol trades
$DATA_1/us/stock-trades-ex-index/<YYYY-MM-DD>.parquet  # consolidated ex-index trades
$DATA_1/cn/daily/<SYMBOL>/<YYYY>.parquet            # CN daily bars
$DATA_1/cn/30min/<SYMBOL>/<YYYY>.parquet            # CN 30-min bars
$DATA_1/cn/5min/<SYMBOL>/<YYYY>.parquet             # CN 5-min bars
```

## License

[MIT](LICENSE)
