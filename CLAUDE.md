# Jupitor

Unified financial platform for US equities and China A-shares. Go handles all core services; Python handles analysis scripts and notebooks.

## Project Structure

- `cmd/` — Go binaries (each runs as a separate process)
- `internal/` — Private Go packages (domain, config, store, gather, engine, strategy, api, broker, util)
- `pkg/jupitor/` — Public Go client SDK
- `proto/` — Protobuf definitions
- `config/` — YAML config templates
- `python/` — Python subsystem (analysis, notebooks, CLI)
- `migrations/` — SQL migrations
- `reference/` — Static reference data (CSVs)
- `data/` — Local data dir (gitignored, symlink to $DATA_1)

## Build & Test

```bash
make build       # go build ./...
make test        # go test ./... -v
make vet         # go vet ./...
make python-test # pytest
```

## Key Conventions

- Go module: `module jupitor`
- Config: YAML files in `config/` with env var overrides (secrets via env vars)
- Storage: Parquet files for time-series, SQLite for transactional data
- Data path: `$DATA_1/us/daily/AAPL/2024.parquet`
- Streaming: gRPC between services
- Client API: REST + gRPC + WebSocket
- Python reads Parquet files and calls jupitor-server API (no direct data gathering)

## us-alpaca-data Daemon Architecture

The `DailyBarGatherer` in `internal/gather/us/alpaca.go` is a long-running daemon (`cmd/us-alpaca-data/`) that alternates between daily bar updates and trade backfill.

### Main Loop

```
Run(ctx):
  loop forever:
    1. If 8:05 PM ET on a trading day AND bars not yet fetched → runDailyUpdate (phases 1-3)
    2. Pick next trade backfill date (latest unfilled first) → ProcessTradeDay
    3. If no work → sleep 1 min, re-check
    4. On ctx cancellation → graceful exit
```

### Daily Bar Update (phases 1-3)

1. **Phase 1 — Update known**: `ListSymbols("us")` → fetch only `[lastCompleted+1, endDate]` for ~18K known symbols
2. **Phase 2 — Discover**: Brute-force all A-Z 1-4 char + CSV symbols, minus known & tried-empty → fetch same narrow window
3. **Phase 3 — Backfill**: Newly discovered symbols only → fetch full `[startDate, endDate]` history

Bar progress: `.tried-empty` + `.last-completed` in `<DataDir>/us/daily/`

### Trade Backfill

- Processes one universe date per loop iteration (latest unfilled first), then yields to daily update check
- `buildTradeBatches` groups symbols targeting ~500K trades per batch based on bar `trade_count`
- Worker pool with rate limiting (300ms ticker, ~200 req/min)
- Trade filter: `size > 100 AND price * size >= 100`
- Trade progress: per-date `.done` marker files in `<DataDir>/us/trades/.done/`

### Storage Layout

- Bars: `$DATA_1/us/daily/<SYMBOL>/<YYYY>.parquet` — immutable, merge-on-write dedup
- Trades: `$DATA_1/us/trades/<SYMBOL>/<YYYY-MM-DD>.parquet` — fields: symbol, timestamp, price, size, exchange, id, conditions, update
- Universe: `$DATA_1/us/universe/<YYYY-MM-DD>.txt` — sorted, deduped symbol lists per trading day

### Key Functions

- `processBatches()` — shared bar worker pool for all three phases
- `ProcessTradeDay()` — trade worker pool (exported for trial scripts)
- `buildTradeBatches()` — groups symbols by trade_count into ~500K-trade batches
- `fetchMultiBars()` / `fetchMultiTrades()` — single Alpaca multi-symbol API calls
- `progressTracker` — `.tried-empty` and `.last-completed` for bar crash recovery
- `universeWriter` / `ReadUniverseFile` / `ListUniverseDates` — per-date symbol lists

## Dependencies

Go: alpaca-trade-api-go, parquet-go, grpc, protobuf, yaml.v3, modernc.org/sqlite
Python: pandas, pyarrow, numpy, matplotlib, typer, grpcio, requests, jupyterlab
