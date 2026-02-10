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

## US Daily Gatherer Architecture

The `DailyBarGatherer` in `internal/gather/us/alpaca.go` uses a three-phase approach:

1. **Phase 1 — Update known**: `ListSymbols("us")` → fetch only `[lastCompleted+1, endDate]` for ~18K known symbols (~4 API calls, ~1 bar each)
2. **Phase 2 — Discover**: Brute-force all A-Z 1-4 char + CSV symbols, minus known & tried-empty → fetch same narrow window (~92 API calls, mostly empty responses)
3. **Phase 3 — Backfill**: Newly discovered symbols only → fetch full `[startDate, endDate]` history (typically 0-5 symbols/day)

Key helpers:
- `processBatches()` — shared worker pool for all three phases (batch splitting, concurrent fetch, WriteBars, universe tracking, optional MarkEmpty)
- `fetchMultiBars()` — single Alpaca multi-symbol API call
- `progressTracker` — `.tried-empty` and `.last-completed` files for crash recovery
- `universeWriter` — per-date symbol lists in `us/universe/YYYY-MM-DD.txt`

Historical bar data is immutable — once written, it never changes. `WriteBars` uses merge-on-write deduplication.

## Dependencies

Go: alpaca-trade-api-go, parquet-go, grpc, protobuf, yaml.v3, modernc.org/sqlite
Python: pandas, pyarrow, numpy, matplotlib, typer, grpcio, requests, jupyterlab
