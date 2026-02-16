# Jupitor

Unified financial platform for US equities and China A-shares. Go handles all core services; Python handles analysis scripts and notebooks.

## Project Structure

- `cmd/` — Go binaries (each runs as a separate process)
- `bin/` — Compiled Go binaries (all `go build` output goes here, e.g. `go build -o bin/us-stream ./cmd/us-stream/`)
- `internal/` — Private Go packages (gather, live, dashboard, httpapi, api, store, config, domain, broker, engine, strategy, util)
- `pkg/jupitor/` — Public Go client SDK
- `proto/` — Protobuf definitions (marketdata, trading, strategy)
- `config/` — YAML config templates
- `python/` — Python subsystem (analysis, notebooks, CLI)
- `migrations/` — SQL migrations
- `reference/` — Static reference data (CSVs)
- `ios/` — SwiftUI iOS app (Xcode project)
- `data/` — Local data dir (gitignored, symlink to $DATA_1)

## Build & Test

```bash
make build       # go build ./...
make test        # go test ./... -v
make vet         # go vet ./...
make lint        # golangci-lint run ./...
make proto       # regenerate protobuf code
make python-test # cd python && pytest tests/ -v
make python-lint # cd python && ruff check src/ tests/
make clean       # go clean ./... && rm -rf bin/
```

## Key Conventions

- Go module: `module jupitor`
- Config: YAML files in `config/` with env var overrides (secrets via env vars, never hardcoded)
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

## us-stream Daemon Architecture

The `StreamGatherer` in `internal/gather/us/alpaca.go` is a long-running daemon (`cmd/us-stream/`) that streams live trades via WebSocket and backfills via REST, maintaining a shared `LiveModel` (`internal/live/model.go`).

### LiveModel

In-memory trade store with today/next-day buckets, dedup by `(trade_id, exchange)`, and pub/sub for gRPC streaming.

- Trades with `timestamp <= todayCutoff` (D 4PM ET) → today bucket; `> cutoff` → next bucket
- Each bucket has index and ex-index slices
- `SwitchDay(newCutoff)` — promotes next→today, rebuilds seen map, updates cutoff

### Main Loop

```
Run(ctx):
  1. Determine today/prevDate, load symbols from Alpaca API
  2. Create LiveModel with todayCutoff = today 4PM ET
  3. Load backfill cache from /tmp (resume from earlier run)
  4. Connect WebSocket stream (captures from NOW)
  5. Start goroutines: runBackfill, runDaySwitch, logStatus
  6. Wait for ctx cancellation or stream termination
```

### Day Switching

- `runDaySwitch` fires at 3:50 AM ET daily
- On trading days (Alpaca Calendar API check): calls `model.SwitchDay()`, updates date fields under `dateMu`, cleans old cache dir
- On non-trading days: skips (next bucket accumulates weekend trades)
- `dateMu sync.RWMutex` protects `today`, `prevDate`, `prevCloseUTC`

### Backfill

- `runBackfill` — 4 workers fetch per-symbol trades from `prevDate 4PM ET → now`
- Snapshots date fields per scan (thread-safe with day switching)
- Per-symbol cache files in `/tmp/us-stream/<YYYY-MM-DD>/backfill/<SYMBOL>.parquet`
- Rescans every 5 min (stream fills the gap)

### HTTP API

REST API on port 8080:

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

### Per-Date Watchlists

Watchlists are scoped per trading date as `jupitor-YYYY-MM-DD` on Alpaca:
- `resolveWatchlistID(date)` lazily creates watchlists on demand, caches ID mapping
- `pruneOldestWatchlists()` removes oldest when 200-watchlist Alpaca limit is hit
- All handlers default to today ET if no `?date=` param

### Key Functions

- `loadSymbolsFromAPI()` — fetches active US equities, filters to ex-index stocks
- `handleStreamTrade()` — processes WebSocket trades with size/exchange filter
- `backfillSymbol()` — per-symbol REST fetch with incremental cache resume
- `isTradingDay()` — Alpaca Calendar API check
- `runDaySwitch()` — 3:50 AM ET day-switch goroutine

## us-client TUI Dashboard

The `cmd/us-client/` is a bubbletea TUI that connects to the us-stream gRPC server and shows a live ex-index trade dashboard.

### Architecture

- **Live mode**: Subscribes to gRPC stream, refreshes every 5s. Shows TODAY and NEXT DAY sections.
- **History mode**: Reads consolidated `stock-trades-ex-index` parquet files. Left/right arrows navigate dates.
- **Preloading**: All history dates preloaded in background on startup. Progress shown in header bar (`hist: N/M`).
- **Caching**: `historyCacheEntry` stores computed `DayData` + sort mode. Navigation between cached dates skips recomputation.

### Display Filtering

- Per-symbol: `gain >= 10% AND trades >= 500` in either pre-market or regular session
- Per-tier top-N: ACTIVE keeps top 5, MODERATE/SPORADIC keep top 8 (by trades, turnover, or gain% in either session)
- Dim styling: trades < 1K, turnover < $1M, gain < 10%, loss < 10%

### Stock Selection

- **Highlight bar**: Up/down arrows navigate a visible highlight bar across all stocks in both day sections
- **Selection tracking**: By `(dayIndex, symbol)` pair — handles duplicates across TODAY/NEXT DAY
- **Default**: First symbol in MODERATE tier of primary day; resets on history navigation
- **Auto-scroll**: Viewport scrolls to keep the selected row visible
- **Styles**: Dark grey background (`236`) preserving per-column colors; brighter blue (`75`) for symbol on highlight

### Watchlist (Alpaca)

- **Toggle**: Space key adds/removes selected symbol from per-date `jupitor-YYYY-MM-DD` watchlist
- **Visual**: Watchlist symbols shown in orange (`208`/`214`), with `*` marker before row number
- **Async**: Optimistic UI update with revert on API error; reloads when navigating dates
- **Optional**: Requires `APCA_API_KEY_ID` / `APCA_API_SECRET_KEY` env vars; gracefully disabled if not set

### Sort Modes

4-mode cycle via `s` key: PRE:TRD → PRE:GAIN → REG:TRD → REG:GAIN. Sort persists across history navigation.

### Next-Day in History

For the latest history date, next-day data comes from the live model's `TodaySnapshot()` (filtered to post-market window). For other dates, it reads the next date's ex-index file filtered to post-market (4PM–8PM ET).

### Key Files

- `cmd/us-client/main.go` — TUI client
- `internal/dashboard/stats.go` — Aggregation, sorting, filtering, session splitting
- `internal/dashboard/format.go` — Price/count/turnover/gain/loss formatting
- `internal/dashboard/history.go` — History file loading, tier map loading
- `internal/dashboard/tiermap.go` — Tier map from trade-universe CSV

## iOS App (SwiftUI)

The `ios/Jupitor/` directory contains a native SwiftUI iPhone app that connects to the us-stream HTTP REST API. Requires iOS 17+.

### Navigation

- **RootTabView**: Single NavigationStack with BubbleChartView as main content
- **Swipe left/right**: Navigate between dates (history + live)
- **Swipe up/down**: Cycle session modes (PRE → REG → DAY → NEXT)
- **Toolbar**: Date + session label (principal), LIVE indicator (leading), settings gear (trailing)
- **History tab**: `HistoryDateListView` → pick date → `HistoryDayView` with same swipe navigation

### Bubble Chart (BubbleChartView)

Physics-based bubble visualization:
- **Sizing**: Area-proportional to turnover (sqrt-weighted). Min 14pt, max bounded by canvas.
- **Rings**: Tier-colored (green=ACTIVE, yellow=MODERATE, red=SPORADIC). Dual rings in DAY mode (outer=REG, inner=PRE, scaled by turnover ratio).
- **Watchlist bubbles**: Rounded squares instead of circles, purple color, price labels (open/high/close).
- **Close dial**: Needle from center to outer ring edge showing close position in [low, high]. Full 360° sweep, 12 o'clock = both 0% and 100%. Red (hue 0) at low, green (hue 0.33) at high.
- **Vertical sorting**: Bubbles with close near high float to top, close near low sink to bottom (spring force on target Y).
- **Physics**: Collision avoidance + boundary forces + vertical bias + progressive damping. Force-settles after 300 frames.
- **Interactions**: Tap to toggle watchlist, double-tap for symbol history, long-press for detail view, shake to clear watchlist, pinch to filter watchlist-only.

### Session Modes

- **PRE**: Pre-market (4AM–9:30AM ET) — deep indigo background
- **REG**: Regular session (9:30AM–4PM ET) — deep forest background
- **DAY**: Combined pre+reg (togglable in Settings, default off) — near black background
- **NEXT**: Next trading day's post-market data — deep maroon background

### Per-Date Watchlists

- Watchlists scoped per date (`jupitor-YYYY-MM-DD`), created on demand
- `watchlistCache: [String: Set<String>]` — in-memory cache by date
- `updateDisplayDate(_ date:)` reloads watchlist when navigating dates
- `toggleWatchlist(symbol:date:)` and `removeWatchlistSymbols(_:date:)` update both cache and optimistic UI

### Key Files

- `ios/Jupitor/Jupitor/JupitorApp.swift` — App entry, creates ViewModel, injects environment
- `ios/Jupitor/Jupitor/ViewModels/DashboardViewModel.swift` — Shared state: live refresh, history, watchlist (per-date), sort, news
- `ios/Jupitor/Jupitor/Views/RootTabView.swift` — Main view: NavigationStack + BubbleChartView + swipe gestures
- `ios/Jupitor/Jupitor/Views/Live/BubbleChartView.swift` — Physics-based bubble chart with rings, dial, vertical sorting
- `ios/Jupitor/Jupitor/Views/Live/SymbolCardView.swift` — Two-line symbol card
- `ios/Jupitor/Jupitor/Views/Live/SymbolListView.swift` — Reusable tier-sectioned list
- `ios/Jupitor/Jupitor/Views/History/HistoryDateListView.swift` — Date list + HistoryDayView with swipe navigation
- `ios/Jupitor/Jupitor/Views/Watchlist/WatchlistView.swift` — Filtered watchlist symbols
- `ios/Jupitor/Jupitor/Views/Detail/SymbolDetailView.swift` — Full detail with SessionCards + news + star toggle
- `ios/Jupitor/Jupitor/Views/Detail/SessionCard.swift` — OHLC grid + trades/turnover/gain/loss
- `ios/Jupitor/Jupitor/Views/Detail/SymbolHistoryView.swift` — Historical stats chart for a symbol
- `ios/Jupitor/Jupitor/Views/TierSectionView.swift` — Tier header + SymbolCardView list with NavigationLinks
- `ios/Jupitor/Jupitor/Views/Settings/SettingsView.swift` — Server URL configuration + Day Mode toggle
- `ios/Jupitor/Jupitor/Models/DashboardModels.swift` — API response types, SortMode, SessionView, SessionMode enums
- `ios/Jupitor/Jupitor/Services/APIService.swift` — REST API client (actor-based)
- `ios/Jupitor/Jupitor/Utilities/Formatters.swift` — Fmt enum for count/turnover/price/gain/loss
- `ios/Jupitor/Jupitor/Utilities/Colors.swift` — Tier/gain/loss/watchlist/session colors + PulseModifier + shake detection

## One-Shot Tools

### us-stock-trades

`cmd/us-stock-trades/main.go` generates per-date consolidated parquet files from per-symbol trade files.

- **stock-trades-ex-index**: `$DATA_1/us/stock-trades-ex-index/<YYYY-MM-DD>.parquet` — all ex-index stock trades for (P 4PM, D 4PM] window
- **stock-trades-index**: `$DATA_1/us/stock-trades-index/<YYYY-MM-DD>.parquet` — same for index stocks
- **stock-trades-ex-index-rolling**: Rolling 5m bars from ex-index trades
- **Filter**: exchange != "D", conditions in {" ", "@", "T", "F"}
- **Requires**: consecutive trade-universe CSV pairs (P, D) + per-symbol trade files

### us-trade-universe

`cmd/us-trade-universe/main.go` generates trade-universe CSVs with tier classification (ACTIVE/MODERATE/SPORADIC) from daily bar VWAP x Volume.

### us-news-history

`cmd/us-news-history/main.go` fetches historical news from Alpaca, Google News RSS, GlobeNewswire, and StockTwits for top symbols per tier. Deep pagination for top 20 MODERATE+SPORADIC on StockTwits.

### us-daily-summary

`cmd/us-daily-summary/main.go` backfills daily summary parquets from existing stock-trades files.

## Dependencies

Go: alpaca-trade-api-go, parquet-go, grpc, protobuf, bubbletea, lipgloss, yaml.v3, modernc.org/sqlite
Python: pandas, pyarrow, numpy, matplotlib, typer, grpcio, requests, jupyterlab, baostock
