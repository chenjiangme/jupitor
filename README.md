# Jupitor

A unified financial data platform for US equities and China A-shares. Go handles all core services (data collection, storage, streaming, APIs). Python handles analysis scripts and notebooks. A native iOS app and terminal TUI provide real-time dashboards.

## Architecture

```
                    Alpaca API                     BaoStock API
                   (WebSocket + REST)              (REST)
                        │                              │
          ┌─────────────┼──────────────┐               │
          ▼             ▼              ▼               ▼
    us-alpaca-data   us-stream    us-news-history   cn_baostock_data.py
    (bars + trades)  (live trades) (news archive)   (bars + fundamentals)
          │             │              │               │
          ▼             ▼              ▼               ▼
    ┌─────────────────────────────────────────────────────────┐
    │              Parquet Files ($DATA_1)                     │
    │  daily bars / trades / consolidated / news / index      │
    └──────────────────────┬──────────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         us-client     iOS App     Python notebooks
         (TUI)         (SwiftUI)   (analysis)
              ▲            ▲
              │            │
              └──── gRPC / HTTP API (us-stream) ────┘
```

## Demo

<p align="center">
  <img src="docs/demo/ios-demo.gif" alt="Jupitor iOS Demo" width="300">
</p>

| Bubble Chart | Session Modes | Date Navigation |
|:---:|:---:|:---:|
| ![Bubbles](docs/demo/01-bubbles.gif) | ![Sessions](docs/demo/02-sessions.gif) | ![Dates](docs/demo/03-dates.gif) |

| Watchlist | Detail View |
|:---:|:---:|
| ![Watchlist](docs/demo/04-watchlist.gif) | ![Detail](docs/demo/05-detail.gif) |

## Components

### Production Daemons

| Binary | Description |
|--------|-------------|
| `us-alpaca-data` | Daily bar updates (3-phase: update known, discover new, backfill) at 8:05 PM ET + continuous historical trade backfill via Alpaca API |
| `us-stream` | Live WebSocket trade streaming + REST backfill for ex-index stocks. Serves gRPC (:50051) and HTTP (:8080) APIs. Day switching at 3:50 AM ET |

### Interactive Clients

| Binary | Description |
|--------|-------------|
| `us-client` | Bubbletea TUI dashboard with live + history modes, watchlist, news, sort modes, mouse/keyboard navigation |
| `us-stream-console` | Lightweight console client with auto-refresh |
| iOS app | Native SwiftUI iPhone app with bubble chart visualization, swipe navigation, watchlist, news |

### One-Shot Tools

| Binary | Description |
|--------|-------------|
| `us-stock-trades` | Consolidates per-symbol trade files into per-date parquet (ex-index, index, rolling 5m bars) |
| `us-trade-universe` | Generates trade-universe CSVs with tier classification (ACTIVE/MODERATE/SPORADIC) from daily bar VWAP x Volume |
| `us-daily-summary` | Backfills daily summary parquets from existing stock-trades files |
| `us-news-history` | Fetches historical news from Alpaca, Google News RSS, GlobeNewswire, and StockTwits |

### Python Scripts

| Script | Description |
|--------|-------------|
| `cn_baostock_data.py` | China A-share daemon: CSI 300/500 constituents, daily/30min/5min bars, quarterly fundamentals (4 workers) |
| `us_index_data.py` | SPX/NDX per-date constituent file builder from Dropbox CSVs + GitHub/Wikipedia |
| `us_ex_index_trades.py` | Ex-index trade analysis |
| `us_daily_movers.py` | Daily movers analysis |

## Project Structure

```
cmd/                    Go binaries (each runs as a separate daemon/tool)
  us-alpaca-data/         Daily bar + trade backfill daemon
  us-stream/              Live streaming + API server
  us-client/              TUI dashboard
  us-stream-console/      Console client
  us-stock-trades/        Trade file consolidator
  us-trade-universe/      Tier classification generator
  us-daily-summary/       Daily summary backfiller
  us-news-history/        News archive builder
internal/               Private Go packages
  gather/us/              Data collection (bars, trades, universe, symbols, calendar)
  live/                   In-memory LiveModel (today/next buckets, dedup, pub/sub)
  dashboard/              Stats aggregation, sorting, filtering, formatting
  httpapi/                HTTP REST API server
  api/                    gRPC service + WebSocket hub
  store/                  ParquetStore (bars + trades) + SQLiteStore
  config/                 YAML config loader with env var overrides
  domain/                 Core types (Bar, Trade, Order, Position, Signal)
  broker/                 Broker abstraction (Alpaca + simulator)
  engine/                 Strategy execution engine
  strategy/               Trading strategies
  util/                   Logging, rate limiting
pkg/jupitor/            Public Go client SDK
proto/                  Protobuf definitions (marketdata, trading, strategy)
ios/Jupitor/            SwiftUI iPhone app (iOS 17+)
python/                 Python analysis scripts and notebooks
config/                 YAML configuration templates
migrations/             SQL migrations
reference/              Static reference data (CSVs)
```

## Prerequisites

- Go 1.24+
- Python 3.9+ (for analysis scripts)
- [Alpaca](https://alpaca.markets/) API account (for US market data)
- Xcode 15+ (for iOS app)
- External data volume mounted at `$DATA_1`

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

## Build & Test

```bash
make build          # go build ./...
make test           # go test ./... -v
make vet            # go vet ./...
make lint           # golangci-lint run ./...
make proto          # regenerate protobuf code
make python-test    # cd python && pytest tests/ -v
make python-lint    # cd python && ruff check src/ tests/
make clean          # go clean ./... && rm -rf bin/
```

## HTTP API

The `us-stream` server exposes a REST API on port 8080:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/dashboard` | Live dashboard data (today + next day, all tiers) |
| GET | `/api/dashboard/history/{date}` | Historical dashboard for a specific date |
| GET | `/api/dates` | List available history dates |
| GET | `/api/watchlist?date=YYYY-MM-DD` | Get watchlist symbols for a date |
| PUT | `/api/watchlist/{symbol}?date=YYYY-MM-DD` | Add symbol to date-scoped watchlist |
| DELETE | `/api/watchlist/{symbol}?date=YYYY-MM-DD` | Remove symbol from date-scoped watchlist |
| GET | `/api/news/{symbol}?date=YYYY-MM-DD` | News articles for a symbol on a date |
| GET | `/api/symbol-history/{symbol}` | Historical stats across dates |

Watchlists are per-date (`jupitor-YYYY-MM-DD`) on Alpaca, created on demand with automatic pruning when the 200-watchlist limit is reached.

## gRPC Services

Defined in `proto/`:

- **MarketData** — StreamTrades, StreamBars, GetDailyBars, GetLatestQuote, StreamLiveTrades
- **Trading** — SubmitOrder, CancelOrder, GetPositions, StreamOrders, GetAccount
- **Strategy** — ListStrategies, GetStrategy, StartStrategy, StopStrategy, RunBacktest, StreamSignals

## iOS App

Native SwiftUI app (iOS 17+) connected to the us-stream HTTP API.

- **Bubble chart**: Physics-based visualization with area-proportional sizing by turnover, tier-colored rings (green/yellow/red), close-position dial needle, vertical sorting by close fraction
- **Navigation**: Swipe left/right to navigate dates, swipe up/down to cycle session modes (PRE/REG/DAY/NEXT)
- **Watchlist**: Per-date watchlists with tap to star, shake to clear, pinch to filter. Watchlist bubbles shown as rounded squares with price labels
- **Detail view**: Push navigation with OHLC session cards, inline news, historical chart
- **Sort**: Menu dropdown grouped by session (Pre-Market / Regular / News) with 7 sort modes
- **Settings**: Server URL configuration, Day Mode toggle
- **Session backgrounds**: Distinct dark backgrounds per session mode for visual orientation

## TUI Client

Bubbletea terminal dashboard with:

- **Live mode**: gRPC stream subscription, 5s refresh, TODAY + NEXT DAY sections
- **History mode**: Left/right arrows navigate dates, all dates preloaded in background
- **Display filtering**: gain >= 10% AND trades >= 500, top-N per tier (ACTIVE=5, MODERATE=8, SPORADIC=8)
- **Watchlist**: Space to toggle, per-date (`jupitor-YYYY-MM-DD`), orange highlighting
- **Sort modes**: 4-mode cycle (PRE:TRD, PRE:GAIN, REG:TRD, REG:GAIN)
- **Selection**: Up/down highlight bar with auto-scroll, mouse click support

## Data Pipeline

### US Market

1. **us-alpaca-data** collects daily bars + per-symbol trades → Parquet files
2. **us-trade-universe** classifies symbols into tiers (ACTIVE/MODERATE/SPORADIC) → CSV
3. **us-stock-trades** consolidates per-symbol trades into per-date files → Parquet
4. **us-news-history** fetches news from multiple sources → Parquet
5. **us-stream** streams live trades via WebSocket → in-memory LiveModel → gRPC/HTTP APIs
6. Clients (TUI + iOS) consume APIs and read historical Parquet files

### China A-Shares

1. **cn_baostock_data.py** collects CSI 300/500 constituents, multi-timeframe bars, quarterly fundamentals → Parquet
2. Python notebooks perform analysis

### Key Design Decisions

- **Timestamps**: ET-shifted milliseconds everywhere (ET clock treated as-if-UTC)
- **Trading day**: 4AM–8PM ET window (pre-market 4AM–9:30AM, regular 9:30AM–4PM, post-market 4PM–8PM)
- **Trade filter**: `size > 100 AND price * size >= 100` plus exchange/condition filtering
- **Deduplication**: By `(trade_id, exchange)` in LiveModel; merge-on-write for bars
- **Tier classification**: Based on VWAP x Volume from daily bar data
- **Ex-index stocks**: Active US equities excluding ETFs and SPX/NDX constituents

## Storage Layout

```
$DATA_1/
├── us/
│   ├── daily/<SYMBOL>/<YYYY>.parquet                       # Daily bars
│   ├── trades/<SYMBOL>/<YYYY-MM-DD>.parquet                # Per-symbol trades
│   ├── universe/<YYYY-MM-DD>.txt                           # Per-date symbol lists
│   ├── trade-universe/<YYYY-MM-DD>.csv                     # symbol,type,spx,ndx,tier
│   ├── stock-trades-ex-index/<YYYY-MM-DD>.parquet          # Consolidated ex-index trades
│   ├── stock-trades-index/<YYYY-MM-DD>.parquet             # Consolidated index trades
│   ├── stock-trades-ex-index-rolling/<YYYY-MM-DD>.parquet  # Rolling 5m bars
│   ├── news/<YYYY-MM-DD>.parquet                           # News articles
│   └── index/
│       ├── spx/<YYYY-MM-DD>.txt                            # SPX constituents
│       └── ndx/<YYYY-MM-DD>.txt                            # NDX constituents
├── cn/
│   ├── daily/<SYMBOL>/<YYYY>.parquet                       # CN daily bars
│   ├── 30min/<SYMBOL>/<YYYY>.parquet                       # CN 30-min bars
│   ├── 5min/<SYMBOL>/<YYYY>.parquet                        # CN 5-min bars
│   ├── fundamentals/{type}/<SYMBOL>.parquet                # Quarterly fundamentals
│   └── index/
│       ├── csi300/<YYYY-MM-DD>.txt                         # CSI 300 constituents
│       └── csi500/<YYYY-MM-DD>.txt                         # CSI 500 constituents
└── jupitor.db                                              # SQLite transactional data

/tmp/us-stream/<YYYY-MM-DD>/backfill/<SYMBOL>.parquet       # Stream backfill cache
reference/us/us_stock_YYYY-MM-DD.csv                        # Stock reference (Dropbox)
reference/us/us_etf_YYYY-MM-DD.csv                          # ETF reference (Dropbox)
```

## Configuration

Config file at `config/jupitor.yaml` with env var overrides:

| Env Variable | Description |
|--------------|-------------|
| `DATA_1` | Path to data volume |
| `APCA_API_KEY_ID` | Alpaca API key |
| `APCA_API_SECRET_KEY` | Alpaca API secret |

## Dependencies

**Go**: alpaca-trade-api-go, parquet-go, grpc, protobuf, bubbletea, lipgloss, yaml.v3, modernc.org/sqlite

**Python**: pandas, pyarrow, numpy, matplotlib, typer, grpcio, requests, jupyterlab, baostock

## License

[MIT](LICENSE)
