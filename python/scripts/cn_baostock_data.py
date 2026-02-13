#!/usr/bin/env python3
from __future__ import annotations

"""cn-baostock-data: daemon for China A-share data collection via BaoStock.

Collects CSI 300/500 constituent history, daily bars, 30-minute bars, and
quarterly fundamentals. Uses 4 worker processes for parallel BaoStock queries.

Priority:
  1. Build CSI 300 + CSI 500 constituent history (as far back as BaoStock allows)
  2. Daily: update today's bars + detect new index additions → backfill new symbols
  3. Backfill full daily bar history for all unique symbols ever in either index
  4. Backfill 30-minute bars (from 2019 onwards)
  5. Backfill quarterly fundamentals (from 2007 onwards)

Storage layout:
  $DATA_1/cn/index/csi300/YYYY-MM-DD.txt            (sorted symbol list)
  $DATA_1/cn/index/csi500/YYYY-MM-DD.txt
  $DATA_1/cn/daily/<SYMBOL>/YYYY.parquet             (all 18 BaoStock fields)
  $DATA_1/cn/daily/.last-completed                   (date of last daily update)
  $DATA_1/cn/30min/<SYMBOL>/YYYY.parquet             (30-min bars, from 2019)
  $DATA_1/cn/fundamentals/{type}/<SYMBOL>.parquet    (quarterly, from 2007)

Usage:
  python python/scripts/cn_baostock_data.py [--config config/jupitor.yaml]
"""

import argparse
import logging
import multiprocessing
import os
import signal
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import baostock as bs
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

log = logging.getLogger("cn-baostock-data")

NUM_WORKERS = 4

# ---------------------------------------------------------------------------
# Parquet schema (all 18 BaoStock daily bar fields)
# ---------------------------------------------------------------------------

BAR_SCHEMA = pa.schema([
    ("symbol",      pa.string()),
    ("date",        pa.string()),        # "2024-01-15"
    ("open",        pa.float64()),
    ("high",        pa.float64()),
    ("low",         pa.float64()),
    ("close",       pa.float64()),
    ("preclose",    pa.float64()),
    ("volume",      pa.int64()),
    ("amount",      pa.float64()),
    ("adjustflag",  pa.string()),
    ("turn",        pa.float64()),
    ("tradestatus", pa.string()),        # "1" or "0"
    ("pctChg",      pa.float64()),
    ("peTTM",       pa.float64()),
    ("psTTM",       pa.float64()),
    ("pcfNcfTTM",   pa.float64()),
    ("pbMRQ",       pa.float64()),
    ("isST",        pa.string()),        # "1" or "0"
])

BAR_FIELDS = (
    "date,code,open,high,low,close,preclose,volume,amount,"
    "adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST"
)

# Float fields in BAR_SCHEMA (for safe parsing)
_FLOAT_COLUMNS = {
    "open", "high", "low", "close", "preclose", "amount",
    "turn", "pctChg", "peTTM", "psTTM", "pcfNcfTTM", "pbMRQ",
}

# Index names recognized by BaoStock
_INDEXES = {
    "csi300": "query_hs300_stocks",
    "csi500": "query_zz500_stocks",
}

# ---------------------------------------------------------------------------
# 30-minute bar schema (BaoStock provides 30-min data from 2019 onwards)
# ---------------------------------------------------------------------------

BAR_30MIN_SCHEMA = pa.schema([
    ("symbol",     pa.string()),      # "sh.600000"
    ("date",       pa.string()),      # "2026-02-10"
    ("time",       pa.string()),      # "20260210100000000"
    ("open",       pa.float64()),
    ("high",       pa.float64()),
    ("low",        pa.float64()),
    ("close",      pa.float64()),
    ("volume",     pa.int64()),
    ("amount",     pa.float64()),
    ("adjustflag", pa.string()),
])

BAR_30MIN_FIELDS = "date,time,code,open,high,low,close,volume,amount,adjustflag"
BAR_30MIN_START = "2019-01-01"

_30MIN_FLOAT_COLUMNS = {"open", "high", "low", "close", "amount"}

# ---------------------------------------------------------------------------
# Quarterly fundamentals (6 types, BaoStock data from 2007 onwards)
# ---------------------------------------------------------------------------

FUNDAMENTAL_TYPES = [
    ("profit",    "query_profit_data"),
    ("operation", "query_operation_data"),
    ("growth",    "query_growth_data"),
    ("balance",   "query_balance_data"),
    ("cashflow",  "query_cash_flow_data"),
    ("dupont",    "query_dupont_data"),
]

FUNDAMENTAL_START_YEAR = 2007

# ---------------------------------------------------------------------------
# Shutdown handling
# ---------------------------------------------------------------------------

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("shutdown signal received (sig=%d)", signum)
    _shutdown = True


# ---------------------------------------------------------------------------
# Worker process init (each worker gets its own BaoStock login)
# ---------------------------------------------------------------------------

def _worker_init():
    """Initialize BaoStock connection in worker process."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # parent handles signals
    bs.login()


# ---------------------------------------------------------------------------
# BaoStock helpers
# ---------------------------------------------------------------------------

def _bs_login():
    """Login to BaoStock, raising on failure."""
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"BaoStock login failed: {lg.error_msg}")
    log.info("BaoStock login OK")


def _bs_logout():
    """Logout from BaoStock."""
    bs.logout()
    log.info("BaoStock logout")


def _query_trading_days(start_date: str, end_date: str) -> list[str]:
    """Query trading calendar from BaoStock. Returns sorted list of trading day strings."""
    rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
    if rs.error_code != "0":
        raise RuntimeError(f"trade calendar query failed: {rs.error_msg}")
    days = []
    while rs.next():
        row = rs.get_row_data()
        if row[1] == "1":  # is_trading_day
            days.append(row[0])
    return days


def _query_index_constituents(index_name: str, query_date: str) -> list[tuple[str, str]]:
    """Query constituents for a given index on a given date.

    Returns a sorted list of (code, name) tuples,
    e.g. [("sh.600000", "浦发银行"), ("sh.600016", "民生银行"), ...].
    Returns empty list if no data for that date (holiday / weekend).
    """
    fn = getattr(bs, _INDEXES[index_name])
    rs = fn(date=query_date)
    if rs.error_code != "0":
        return []

    seen = set()
    results = []
    while rs.next():
        row = rs.get_row_data()
        if len(row) >= 3 and row[1] and row[1] not in seen:
            seen.add(row[1])
            results.append((row[1], row[2]))
    results.sort(key=lambda x: x[0])
    return results


def _query_daily_bars(symbol: str, start_date: str, end_date: str) -> list[dict]:
    """Query daily bars for a single symbol from BaoStock.

    Returns list of dicts matching BAR_SCHEMA field names.
    """
    rs = bs.query_history_k_data_plus(
        code=symbol,
        fields=BAR_FIELDS,
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="3",
    )
    if rs.error_code != "0":
        return []

    field_names = BAR_FIELDS.split(",")
    rows = []
    while rs.next():
        raw = rs.get_row_data()
        if len(raw) != len(field_names):
            continue
        row = {}
        for name, val in zip(field_names, raw):
            if name == "code":
                row["symbol"] = val
            elif name == "volume":
                row["volume"] = int(val) if val else 0
            elif name in _FLOAT_COLUMNS:
                row[name] = float(val) if val else 0.0
            else:
                row[name] = val
        rows.append(row)
    return rows


def _query_30min_bars(symbol: str, start_date: str, end_date: str) -> list[dict]:
    """Query 30-minute bars for a single symbol from BaoStock."""
    rs = bs.query_history_k_data_plus(
        code=symbol,
        fields=BAR_30MIN_FIELDS,
        start_date=start_date,
        end_date=end_date,
        frequency="30",
        adjustflag="3",
    )
    if rs.error_code != "0":
        return []

    field_names = BAR_30MIN_FIELDS.split(",")
    rows = []
    while rs.next():
        raw = rs.get_row_data()
        if len(raw) != len(field_names):
            continue
        row = {}
        for name, val in zip(field_names, raw):
            if name == "code":
                row["symbol"] = val
            elif name == "volume":
                row["volume"] = int(val) if val else 0
            elif name in _30MIN_FLOAT_COLUMNS:
                row[name] = float(val) if val else 0.0
            else:
                row[name] = val
        rows.append(row)
    return rows


def _rs_to_rows(rs) -> list[list[str]]:
    """Convert BaoStock result set to list of row lists."""
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    return rows


# ---------------------------------------------------------------------------
# Worker tasks (module-level functions for multiprocessing pickling)
# ---------------------------------------------------------------------------

def _task_fetch_index(args: tuple) -> tuple | None:
    """Worker task: fetch index constituents for one (date, index_name) pair.

    Returns (index_name, date_str, count) on success, None if skipped/empty.
    """
    date_str, index_name, index_dir_str = args
    file_path = Path(index_dir_str) / f"{date_str}.txt"
    if file_path.exists():
        return None

    constituents = _query_index_constituents(index_name, date_str)
    if constituents:
        _write_index_file(file_path, constituents)
        return (index_name, date_str, len(constituents))
    return None


def _task_fetch_bars(args: tuple) -> tuple:
    """Worker task: fetch and write bars for one symbol.

    Returns (symbol, num_bars_written).
    """
    symbol, fetch_start, fetch_end, daily_dir_str = args
    daily_dir = Path(daily_dir_str)

    rows = _query_daily_bars(symbol, fetch_start, fetch_end)
    if rows:
        # Group by year and write with merge-on-write
        by_year: dict[str, list[dict]] = {}
        for r in rows:
            year = r["date"][:4]
            by_year.setdefault(year, []).append(r)
        for year, year_rows in by_year.items():
            path = daily_dir / symbol / f"{year}.parquet"
            _write_bars_parquet(path, year_rows)
        return (symbol, len(rows))
    return (symbol, 0)


def _task_fetch_30min(args: tuple) -> tuple:
    """Worker task: fetch and write 30-min bars for one symbol.

    Returns (symbol, num_bars_written).
    """
    symbol, fetch_start, fetch_end, bar_dir_str = args
    bar_dir = Path(bar_dir_str)

    rows = _query_30min_bars(symbol, fetch_start, fetch_end)
    if rows:
        by_year: dict[str, list[dict]] = {}
        for r in rows:
            year = r["date"][:4]
            by_year.setdefault(year, []).append(r)
        for year, year_rows in by_year.items():
            path = bar_dir / symbol / f"{year}.parquet"
            _write_30min_parquet(path, year_rows)
        return (symbol, len(rows))
    return (symbol, 0)


def _task_fetch_fundamentals(args: tuple) -> tuple:
    """Worker task: fetch all 6 fundamental types for one symbol.

    Returns (symbol, total_rows, errors).
    """
    symbol, fund_dir_str, start_year = args
    fund_dir = Path(fund_dir_str)
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1

    total_rows = 0
    errors = []

    for ftype, query_fn_name in FUNDAMENTAL_TYPES:
        query_fn = getattr(bs, query_fn_name)
        output_path = fund_dir / ftype / f"{symbol}.parquet"

        existing = _get_existing_quarters(output_path)
        target_quarters = []
        for year in range(start_year, current_year + 1):
            q_end = current_quarter if year == current_year else 4
            for q in range(1, q_end + 1):
                target_quarters.append((year, q))

        missing = [(y, q) for y, q in target_quarters if (y, q) not in existing]
        if not missing:
            continue

        all_rows = []
        fields = None
        for year, quarter in missing:
            try:
                rs = query_fn(symbol, year, quarter)
                if rs.error_code != "0":
                    continue
                if fields is None:
                    fields = rs.fields
                raw_rows = _rs_to_rows(rs)
                for raw in raw_rows:
                    if len(raw) != len(fields):
                        continue
                    row = {}
                    for fname, val in zip(fields, raw):
                        if fname in ("code", "pubDate", "statDate"):
                            row[fname] = val
                        else:
                            row[fname] = float(val) if val else None
                    all_rows.append(row)
            except Exception as e:
                errors.append(f"{ftype} {year}Q{quarter}: {e}")

        if all_rows and fields:
            _write_fundamental_parquet(output_path, all_rows, fields)
            total_rows += len(all_rows)

    return (symbol, total_rows, errors)


# ---------------------------------------------------------------------------
# Parquet I/O
# ---------------------------------------------------------------------------

def _write_bars_parquet(path: Path, rows: list[dict]):
    """Write bars to a parquet file with merge-on-write dedup.

    Reads existing file if present, merges new rows, deduplicates by
    (symbol, date), sorts by date, and writes back.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    existing_rows = []
    if path.exists():
        try:
            table = pq.read_table(path, schema=BAR_SCHEMA)
            existing_rows = table.to_pylist()
        except Exception:
            pass

    # Merge: new rows overwrite existing rows with same (symbol, date)
    by_key = {}
    for r in existing_rows:
        by_key[(r["symbol"], r["date"])] = r
    for r in rows:
        by_key[(r["symbol"], r["date"])] = r

    merged = sorted(by_key.values(), key=lambda r: r["date"])

    arrays = []
    for field in BAR_SCHEMA:
        col = [r.get(field.name) for r in merged]
        arrays.append(pa.array(col, type=field.type))

    table = pa.table(arrays, schema=BAR_SCHEMA)
    pq.write_table(table, path)


def _read_latest_bar_date(symbol: str, daily_dir: Path) -> str | None:
    """Find the latest bar date for a symbol from its parquet files."""
    sym_dir = daily_dir / symbol
    if not sym_dir.exists():
        return None

    for pf in sorted(sym_dir.glob("*.parquet"), reverse=True):
        try:
            table = pq.read_table(pf, columns=["date"])
            dates = table.column("date").to_pylist()
            if dates:
                return max(dates)
        except Exception:
            continue
    return None


def _write_30min_parquet(path: Path, rows: list[dict]):
    """Write 30-min bars to parquet with merge-on-write dedup on (symbol, date, time)."""
    path.parent.mkdir(parents=True, exist_ok=True)

    existing_rows = []
    if path.exists():
        try:
            table = pq.read_table(path, schema=BAR_30MIN_SCHEMA)
            existing_rows = table.to_pylist()
        except Exception:
            pass

    by_key = {}
    for r in existing_rows:
        by_key[(r["symbol"], r["date"], r["time"])] = r
    for r in rows:
        by_key[(r["symbol"], r["date"], r["time"])] = r

    merged = sorted(by_key.values(), key=lambda r: (r["date"], r["time"]))

    arrays = []
    for field in BAR_30MIN_SCHEMA:
        col = [r.get(field.name) for r in merged]
        arrays.append(pa.array(col, type=field.type))

    table = pa.table(arrays, schema=BAR_30MIN_SCHEMA)
    pq.write_table(table, path)


def _read_latest_30min_date(symbol: str, bar_dir: Path) -> str | None:
    """Find the latest 30-min bar date for a symbol from its parquet files."""
    sym_dir = bar_dir / symbol
    if not sym_dir.exists():
        return None

    for pf in sorted(sym_dir.glob("*.parquet"), reverse=True):
        try:
            table = pq.read_table(pf, columns=["date"])
            dates = table.column("date").to_pylist()
            if dates:
                return max(dates)
        except Exception:
            continue
    return None


def _get_existing_quarters(path: Path) -> set[tuple[int, int]]:
    """Get set of (year, quarter) already in a fundamental parquet file."""
    if not path.exists():
        return set()
    try:
        table = pq.read_table(path, columns=["statDate"])
        quarters = set()
        for val in table.column("statDate").to_pylist():
            if val is None:
                continue
            if hasattr(val, 'strftime'):
                val = val.strftime("%Y-%m-%d")
            parts = val.split("-")
            year = int(parts[0])
            month = int(parts[1])
            quarter = (month - 1) // 3 + 1
            quarters.add((year, quarter))
        return quarters
    except Exception:
        return set()


def _write_fundamental_parquet(path: Path, new_rows: list[dict], fields: list[str]):
    """Write fundamental data with merge-on-write dedup on (code, statDate)."""
    path.parent.mkdir(parents=True, exist_ok=True)

    existing_rows = []
    if path.exists():
        try:
            table = pq.read_table(path)
            existing_rows = table.to_pylist()
            for r in existing_rows:
                for k, v in r.items():
                    if hasattr(v, 'strftime'):
                        r[k] = v.strftime("%Y-%m-%d")
        except Exception:
            pass

    by_key = {}
    for r in existing_rows:
        by_key[(r.get("code", ""), r.get("statDate", ""))] = r
    for r in new_rows:
        by_key[(r.get("code", ""), r.get("statDate", ""))] = r

    merged = sorted(by_key.values(), key=lambda r: r.get("statDate", ""))
    if not merged:
        return

    schema_fields = []
    for f in fields:
        if f in ("code", "pubDate", "statDate"):
            schema_fields.append(pa.field(f, pa.string()))
        else:
            schema_fields.append(pa.field(f, pa.float64()))
    schema = pa.schema(schema_fields)

    arrays = []
    for field in schema:
        col = [r.get(field.name) for r in merged]
        arrays.append(pa.array(col, type=field.type))

    table = pa.table(arrays, schema=schema)
    pq.write_table(table, path)


# ---------------------------------------------------------------------------
# Index history file I/O
# ---------------------------------------------------------------------------

def _write_index_file(path: Path, constituents: list[tuple[str, str]]):
    """Write sorted constituent list to a text file, one per line as 'code,name'."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = sorted(set(f"{code},{name}" for code, name in constituents))
    path.write_text("\n".join(lines) + "\n")


def _read_index_file(path: Path) -> list[str]:
    """Read symbol codes from an index file. Handles both 'code,name' and plain 'code' formats."""
    if not path.exists():
        return []
    symbols = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            symbols.append(line.split(",")[0])
    return symbols


def _all_index_symbols(data_dir: Path) -> list[str]:
    """Union of all symbols ever in any index constituent file."""
    symbols = set()
    for index_name in _INDEXES:
        index_dir = data_dir / "cn" / "index" / index_name
        for f in index_dir.glob("*.txt"):
            symbols.update(_read_index_file(f))
    return sorted(symbols)


# ---------------------------------------------------------------------------
# Daemon
# ---------------------------------------------------------------------------

class CNBaoStockDaemon:
    def __init__(self, data_dir: str, start_date: str):
        self.data_dir = Path(data_dir)
        self.start_date = start_date
        self.daily_dir = self.data_dir / "cn" / "daily"
        self.bar_30min_dir = self.data_dir / "cn" / "30min"
        self.fund_dir = self.data_dir / "cn" / "fundamentals"
        self._daily_update_done_today = None

    def run(self):
        """Main daemon loop."""
        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        _bs_login()
        try:
            while not _shutdown:
                # 1. Build index history
                if not self._index_history_complete():
                    self._build_index_history()
                    continue

                # 2. Daily update trigger
                if self._should_run_daily_update():
                    self._run_daily_update()
                    continue

                # 3. Daily bar backfill
                did_work = self._bar_backfill()

                # 4. 30-minute bar backfill
                if not did_work:
                    did_work = self._30min_backfill()

                # 5. Fundamentals backfill
                if not did_work:
                    did_work = self._fundamentals_backfill()

                if not did_work:
                    log.info("no work available, sleeping 60s")
                    self._sleep(60)
        finally:
            _bs_logout()

        log.info("daemon stopped")

    # -------------------------------------------------------------------
    # Phase 1: Build index constituent history (4 workers)
    # -------------------------------------------------------------------

    def _index_scan_progress_path(self) -> Path:
        return self.data_dir / "cn" / "index" / ".scan-progress"

    def _read_scan_progress(self) -> str | None:
        p = self._index_scan_progress_path()
        if p.exists():
            return p.read_text().strip() or None
        return None

    def _write_scan_progress(self, date_str: str):
        p = self._index_scan_progress_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(date_str + "\n")

    def _index_history_complete(self) -> bool:
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        progress = self._read_scan_progress()
        return progress is not None and progress >= yesterday

    def _build_index_history(self):
        """Build CSI 300 + CSI 500 constituent history with 4 workers.

        Uses BaoStock trading calendar. Resumes from .scan-progress.
        Skips dates with existing files.
        """
        progress = self._read_scan_progress()
        if progress:
            scan_start = (date.fromisoformat(progress) + timedelta(days=1)).isoformat()
            log.info("resuming index history from %s", scan_start)
        else:
            scan_start = self.start_date
            log.info("building index constituent history from %s", scan_start)

        end_str = date.today().isoformat()
        if scan_start > end_str:
            return

        trading_days = _query_trading_days(scan_start, end_str)
        log.info("trading calendar: %d trading days in [%s, %s]", len(trading_days), scan_start, end_str)

        # Build work items: (date_str, index_name, index_dir) for each missing file
        work = []
        for date_str in trading_days:
            for index_name in _INDEXES:
                index_dir = self.data_dir / "cn" / "index" / index_name
                file_path = index_dir / f"{date_str}.txt"
                if not file_path.exists():
                    work.append((date_str, index_name, str(index_dir)))

        if not work:
            log.info("index history: all files exist, nothing to do")
            if trading_days:
                self._write_scan_progress(trading_days[-1])
            return

        log.info("index history: %d items to fetch with %d workers", len(work), NUM_WORKERS)

        completed = True
        with multiprocessing.Pool(NUM_WORKERS, initializer=_worker_init) as pool:
            done = 0
            for result in pool.imap_unordered(_task_fetch_index, work, chunksize=4):
                if _shutdown:
                    pool.terminate()
                    completed = False
                    break
                done += 1
                if result:
                    log.info("index %s %s: %d symbols", *result)
                if done % 200 == 0:
                    log.info("index scan: %d/%d items done", done, len(work))

        if completed and trading_days:
            self._write_scan_progress(trading_days[-1])
            log.info("index history build complete (%d items)", len(work))

    # -------------------------------------------------------------------
    # Phase 2: Bar backfill (4 workers)
    # -------------------------------------------------------------------

    def _bar_backfill(self) -> bool:
        """Backfill bars for all symbols missing data, using 4 workers.

        Returns True if work was done.
        """
        all_symbols = _all_index_symbols(self.data_dir)
        if not all_symbols:
            return False

        today_str = date.today().isoformat()

        # Build work list: symbols needing bar data
        work = []
        for symbol in all_symbols:
            latest = _read_latest_bar_date(symbol, self.daily_dir)
            if latest is not None and latest >= today_str:
                continue
            if latest is None:
                fetch_start = self.start_date
            else:
                fetch_start = (date.fromisoformat(latest) + timedelta(days=1)).isoformat()
            if fetch_start > today_str:
                continue
            work.append((symbol, fetch_start, today_str, str(self.daily_dir)))

        if not work:
            return False

        log.info("bar backfill: %d symbols to process with %d workers", len(work), NUM_WORKERS)

        total_bars = 0
        with multiprocessing.Pool(NUM_WORKERS, initializer=_worker_init) as pool:
            done = 0
            for result in pool.imap_unordered(_task_fetch_bars, work, chunksize=1):
                if _shutdown:
                    pool.terminate()
                    break
                done += 1
                sym, count = result
                total_bars += count
                if count > 0:
                    log.info("backfill %s: %d bars (%d/%d)", sym, count, done, len(work))
                if done % 50 == 0 and count == 0:
                    log.info("bar backfill progress: %d/%d symbols done", done, len(work))

        return total_bars > 0

    # -------------------------------------------------------------------
    # Phase 3: 30-minute bar backfill (4 workers)
    # -------------------------------------------------------------------

    def _30min_backfill(self) -> bool:
        """Backfill 30-minute bars for all symbols, using 4 workers.

        Returns True if work was done.
        """
        all_symbols = _all_index_symbols(self.data_dir)
        if not all_symbols:
            return False

        today_str = date.today().isoformat()

        work = []
        for symbol in all_symbols:
            latest = _read_latest_30min_date(symbol, self.bar_30min_dir)
            if latest is not None and latest >= today_str:
                continue
            if latest is None:
                fetch_start = BAR_30MIN_START
            else:
                fetch_start = (date.fromisoformat(latest) + timedelta(days=1)).isoformat()
            if fetch_start > today_str:
                continue
            work.append((symbol, fetch_start, today_str, str(self.bar_30min_dir)))

        if not work:
            return False

        log.info("30min backfill: %d symbols to process with %d workers", len(work), NUM_WORKERS)

        total_bars = 0
        with multiprocessing.Pool(NUM_WORKERS, initializer=_worker_init) as pool:
            done = 0
            for result in pool.imap_unordered(_task_fetch_30min, work, chunksize=1):
                if _shutdown:
                    pool.terminate()
                    break
                done += 1
                sym, count = result
                total_bars += count
                if count > 0:
                    log.info("30min %s: %d bars (%d/%d)", sym, count, done, len(work))
                if done % 50 == 0 and count == 0:
                    log.info("30min backfill progress: %d/%d symbols done", done, len(work))

        return total_bars > 0

    # -------------------------------------------------------------------
    # Phase 4: Fundamentals backfill (4 workers)
    # -------------------------------------------------------------------

    def _fundamentals_tried_empty_path(self) -> Path:
        return self.fund_dir / ".tried-empty"

    def _load_fundamentals_tried_empty(self) -> set[str]:
        p = self._fundamentals_tried_empty_path()
        if not p.exists():
            return set()
        return set(line.strip() for line in p.read_text().splitlines() if line.strip())

    def _save_fundamentals_tried_empty(self, symbols: set[str]):
        p = self._fundamentals_tried_empty_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("\n".join(sorted(symbols)) + "\n")

    def _fundamentals_backfill(self) -> bool:
        """Backfill quarterly fundamentals for all symbols, using 4 workers.

        Returns True if work was done.
        """
        all_symbols = _all_index_symbols(self.data_dir)
        if not all_symbols:
            return False

        tried_empty = self._load_fundamentals_tried_empty()

        # Check if any symbol has missing quarters
        work = []
        current_year = datetime.now().year
        current_quarter = (datetime.now().month - 1) // 3 + 1
        target_quarters = []
        for year in range(FUNDAMENTAL_START_YEAR, current_year + 1):
            q_end = current_quarter if year == current_year else 4
            for q in range(1, q_end + 1):
                target_quarters.append((year, q))

        for symbol in all_symbols:
            if symbol in tried_empty:
                continue
            needs_work = False
            for ftype, _ in FUNDAMENTAL_TYPES:
                path = self.fund_dir / ftype / f"{symbol}.parquet"
                existing = _get_existing_quarters(path)
                if any((y, q) not in existing for y, q in target_quarters):
                    needs_work = True
                    break
            if needs_work:
                work.append((symbol, str(self.fund_dir), FUNDAMENTAL_START_YEAR))

        if not work:
            return False

        log.info("fundamentals backfill: %d symbols to process (%d tried-empty skipped)",
                 len(work), len(tried_empty))

        total_rows = 0
        with multiprocessing.Pool(NUM_WORKERS, initializer=_worker_init) as pool:
            done = 0
            for result in pool.imap_unordered(_task_fetch_fundamentals, work, chunksize=1):
                if _shutdown:
                    pool.terminate()
                    break
                done += 1
                sym, rows, errors = result
                total_rows += rows
                if rows > 0:
                    log.info("fundamentals %s: %d rows (%d/%d)", sym, rows, done, len(work))
                else:
                    # No data at all — mark as tried-empty
                    tried_empty.add(sym)
                    if done % 100 == 0:
                        log.info("fundamentals progress: %d/%d symbols done", done, len(work))
                if errors:
                    for e in errors[:2]:
                        log.warning("fundamentals %s: %s", sym, e)

        self._save_fundamentals_tried_empty(tried_empty)
        return total_rows > 0

    # -------------------------------------------------------------------
    # Phase 5: Daily update
    # -------------------------------------------------------------------

    def _should_run_daily_update(self) -> bool:
        """Check if daily update should run (after 4:30 PM CST, once per day)."""
        now = datetime.now()
        today = now.date()

        if self._daily_update_done_today == today:
            return False

        lc_path = self.daily_dir / ".last-completed"
        if lc_path.exists():
            lc = lc_path.read_text().strip()
            if lc >= today.isoformat():
                self._daily_update_done_today = today
                return False

        cutoff_hour, cutoff_min = 16, 30
        if now.hour < cutoff_hour or (now.hour == cutoff_hour and now.minute < cutoff_min):
            return False

        return True

    def _run_daily_update(self):
        """Daily update: refresh constituents, detect new symbols, update bars."""
        today_str = date.today().isoformat()
        log.info("daily update started for %s", today_str)

        # 1. Refresh today's index constituents (just 2 queries, serial)
        new_symbols = set()
        known_symbols = set(_all_index_symbols(self.data_dir))

        for index_name in _INDEXES:
            if _shutdown:
                return
            constituents = _query_index_constituents(index_name, today_str)
            if constituents:
                index_dir = self.data_dir / "cn" / "index" / index_name
                _write_index_file(index_dir / f"{today_str}.txt", constituents)
                log.info("daily index %s: %d symbols", index_name, len(constituents))
                for code, _name in constituents:
                    if code not in known_symbols:
                        new_symbols.add(code)

        # 2. Backfill new symbols fully (parallel)
        if new_symbols and not _shutdown:
            log.info("detected %d new symbols, backfilling", len(new_symbols))
            work = [(sym, self.start_date, today_str, str(self.daily_dir))
                    for sym in sorted(new_symbols)]
            with multiprocessing.Pool(NUM_WORKERS, initializer=_worker_init) as pool:
                for result in pool.imap_unordered(_task_fetch_bars, work, chunksize=1):
                    if _shutdown:
                        pool.terminate()
                        break
                    sym, count = result
                    if count > 0:
                        log.info("new symbol backfill %s: %d bars", sym, count)

        # 3. Update bars for all current constituents (parallel)
        current_symbols = set()
        for index_name in _INDEXES:
            index_dir = self.data_dir / "cn" / "index" / index_name
            file_path = index_dir / f"{today_str}.txt"
            current_symbols.update(_read_index_file(file_path))

        if current_symbols and not _shutdown:
            log.info("updating bars for %d current constituents", len(current_symbols))
            work = [(sym, today_str, today_str, str(self.daily_dir))
                    for sym in sorted(current_symbols)]
            count = 0
            with multiprocessing.Pool(NUM_WORKERS, initializer=_worker_init) as pool:
                for result in pool.imap_unordered(_task_fetch_bars, work, chunksize=4):
                    if _shutdown:
                        pool.terminate()
                        break
                    if result[1] > 0:
                        count += 1
            log.info("daily bar update: %d/%d symbols had data", count, len(current_symbols))

        # 4. Mark completed
        if not _shutdown:
            self.daily_dir.mkdir(parents=True, exist_ok=True)
            (self.daily_dir / ".last-completed").write_text(today_str + "\n")
            self._daily_update_done_today = date.today()
            log.info("daily update complete for %s", today_str)

    # -------------------------------------------------------------------
    # Utility
    # -------------------------------------------------------------------

    def _sleep(self, seconds: int):
        """Sleep in 1-second increments, checking shutdown flag."""
        for _ in range(seconds):
            if _shutdown:
                break
            time.sleep(1)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> tuple[str, str]:
    """Load data_dir and start_date from config YAML + env overrides."""
    data_dir = os.environ.get("DATA_1", "")
    start_date = "2005-01-01"

    if os.path.exists(config_path):
        with open(config_path) as f:
            cfg = yaml.safe_load(f) or {}

        storage = cfg.get("storage", {})
        if not data_dir:
            raw = storage.get("data_dir", "")
            data_dir = os.path.expandvars(raw)

        gather = cfg.get("gather", {})
        cn = gather.get("cn_daily", {})
        if cn.get("start_date"):
            start_date = cn["start_date"]

    if not data_dir:
        print("ERROR: DATA_1 env var or storage.data_dir config required", file=sys.stderr)
        sys.exit(1)

    return data_dir, start_date


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="China A-share BaoStock data daemon")
    parser.add_argument("--config", default="config/jupitor.yaml", help="Config file path")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    data_dir, start_date = load_config(args.config)
    log.info("data_dir=%s start_date=%s workers=%d", data_dir, start_date, NUM_WORKERS)

    daemon = CNBaoStockDaemon(data_dir=data_dir, start_date=start_date)
    daemon.run()


if __name__ == "__main__":
    main()
