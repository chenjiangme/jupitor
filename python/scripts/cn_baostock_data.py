#!/usr/bin/env python3
"""cn-baostock-data: daemon for China A-share daily bar collection via BaoStock.

Collects CSI 300 and CSI 500 constituent history and daily bars.

Priority:
  1. Build CSI 300 + CSI 500 constituent history (as far back as BaoStock allows)
  2. Backfill full daily bar history for all unique symbols ever in either index
  3. Daily: update today's bars + detect new index additions → backfill new symbols

Storage layout:
  $DATA_1/cn/index/csi300/YYYY-MM-DD.txt   (sorted symbol list)
  $DATA_1/cn/index/csi500/YYYY-MM-DD.txt
  $DATA_1/cn/daily/<SYMBOL>/YYYY.parquet    (all 18 BaoStock fields)
  $DATA_1/cn/daily/.last-completed          (date of last daily update)

Usage:
  python python/scripts/cn_baostock_data.py [--config config/jupitor.yaml]
"""

import argparse
import logging
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
# Shutdown handling
# ---------------------------------------------------------------------------

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("shutdown signal received (sig=%d)", signum)
    _shutdown = True


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


def _query_index_constituents(index_name: str, query_date: str) -> list[str]:
    """Query constituents for a given index on a given date.

    Returns a sorted list of symbol codes (e.g. ["sh.600000", "sh.600016", ...]).
    Returns empty list if no data for that date (holiday / weekend).
    """
    fn = getattr(bs, _INDEXES[index_name])
    rs = fn(date=query_date)
    if rs.error_code != "0":
        log.warning("index query error: %s %s → %s", index_name, query_date, rs.error_msg)
        return []

    symbols = []
    while rs.next():
        row = rs.get_row_data()
        # row[1] is the stock code (e.g. "sh.600000")
        if len(row) >= 2 and row[1]:
            symbols.append(row[1])
    return sorted(set(symbols))


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
        log.warning("bar query error: %s [%s, %s] → %s", symbol, start_date, end_date, rs.error_msg)
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
        except Exception as e:
            log.warning("reading existing parquet %s: %s", path, e)

    # Merge: new rows overwrite existing rows with same (symbol, date)
    by_key = {}
    for r in existing_rows:
        by_key[(r["symbol"], r["date"])] = r
    for r in rows:
        by_key[(r["symbol"], r["date"])] = r

    merged = sorted(by_key.values(), key=lambda r: r["date"])

    # Build columnar arrays
    arrays = []
    for field in BAR_SCHEMA:
        col = [r.get(field.name) for r in merged]
        arrays.append(pa.array(col, type=field.type))

    table = pa.table(arrays, schema=BAR_SCHEMA)
    pq.write_table(table, path)


def _read_latest_bar_date(symbol: str, daily_dir: Path) -> str | None:
    """Find the latest bar date for a symbol from its parquet files.

    Returns date string like "2024-12-31" or None if no data.
    """
    sym_dir = daily_dir / symbol
    if not sym_dir.exists():
        return None

    latest = None
    for pf in sorted(sym_dir.glob("*.parquet"), reverse=True):
        try:
            table = pq.read_table(pf, columns=["date"])
            dates = table.column("date").to_pylist()
            if dates:
                max_date = max(dates)
                if latest is None or max_date > latest:
                    latest = max_date
                break  # files are sorted descending by year, first hit is enough
        except Exception:
            continue
    return latest


# ---------------------------------------------------------------------------
# Index history file I/O
# ---------------------------------------------------------------------------

def _write_index_file(path: Path, symbols: list[str]):
    """Write sorted symbol list to a text file, one per line."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(sorted(set(symbols))) + "\n")


def _read_index_file(path: Path) -> list[str]:
    """Read symbol list from a text file."""
    if not path.exists():
        return []
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


def _list_index_dates(index_dir: Path) -> list[str]:
    """List all dates that have index constituent files, sorted ascending."""
    if not index_dir.exists():
        return []
    dates = []
    for f in index_dir.iterdir():
        if f.suffix == ".txt" and len(f.stem) == 10:
            dates.append(f.stem)
    return sorted(dates)


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
        self._daily_update_done_today = None  # date when last daily update ran

    def run(self):
        """Main daemon loop."""
        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        _bs_login()
        try:
            while not _shutdown:
                # 1. Build index history (runs to completion)
                if not self._index_history_complete():
                    self._build_index_history()
                    continue

                # 2. Daily update trigger
                if self._should_run_daily_update():
                    self._run_daily_update()
                    continue

                # 3. Bar backfill (one symbol per iteration)
                did_work = self._bar_backfill_step()

                if not did_work:
                    log.info("no work available, sleeping 60s")
                    self._sleep(60)
        finally:
            _bs_logout()

        log.info("daemon stopped")

    # -------------------------------------------------------------------
    # Phase 1: Build index constituent history
    # -------------------------------------------------------------------

    def _index_history_complete(self) -> bool:
        """Check if index history is built up to yesterday."""
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        for index_name in _INDEXES:
            index_dir = self.data_dir / "cn" / "index" / index_name
            dates = _list_index_dates(index_dir)
            if not dates or dates[-1] < yesterday:
                return False
        return True

    def _build_index_history(self):
        """Build CSI 300 + CSI 500 constituent history from start_date to today.

        Iterates over every date, querying BaoStock for constituents.
        Skips dates that already have files. Writes only when data is returned
        (i.e. trading days).
        """
        log.info("building index constituent history from %s", self.start_date)
        start = date.fromisoformat(self.start_date)
        end = date.today()

        current = start
        while current <= end and not _shutdown:
            date_str = current.isoformat()

            for index_name in _INDEXES:
                if _shutdown:
                    break
                index_dir = self.data_dir / "cn" / "index" / index_name
                file_path = index_dir / f"{date_str}.txt"

                if file_path.exists():
                    continue

                symbols = _query_index_constituents(index_name, date_str)

                if symbols:
                    _write_index_file(file_path, symbols)
                    log.info("index %s %s: %d symbols", index_name, date_str, len(symbols))

            current += timedelta(days=1)

        if not _shutdown:
            log.info("index history build complete")

    # -------------------------------------------------------------------
    # Phase 2: Bar backfill
    # -------------------------------------------------------------------

    def _bar_backfill_step(self) -> bool:
        """Backfill bars for the next symbol missing data.

        Returns True if work was done.
        """
        all_symbols = _all_index_symbols(self.data_dir)
        if not all_symbols:
            return False

        today_str = date.today().isoformat()

        for symbol in all_symbols:
            if _shutdown:
                return False

            latest = _read_latest_bar_date(symbol, self.daily_dir)

            if latest is not None and latest >= today_str:
                continue  # up to date

            # Determine fetch range
            if latest is None:
                fetch_start = self.start_date
            else:
                # Start from the day after the latest bar
                next_day = date.fromisoformat(latest) + timedelta(days=1)
                fetch_start = next_day.isoformat()

            if fetch_start > today_str:
                continue

            log.info("backfill %s from %s", symbol, fetch_start)
            rows = _query_daily_bars(symbol, fetch_start, today_str)

            if rows:
                self._write_bars(symbol, rows)
                log.info("backfill %s: wrote %d bars", symbol, len(rows))
            else:
                log.debug("backfill %s: no data in [%s, %s]", symbol, fetch_start, today_str)

            return True  # did work (even if no data, we queried)

        return False  # all symbols up to date

    # -------------------------------------------------------------------
    # Phase 3: Daily update
    # -------------------------------------------------------------------

    def _should_run_daily_update(self) -> bool:
        """Check if daily update should run (after 4:30 PM CST, once per day)."""
        now = datetime.now()
        today = now.date()

        # Already ran today
        if self._daily_update_done_today == today:
            return False

        # Check .last-completed
        lc_path = self.daily_dir / ".last-completed"
        if lc_path.exists():
            lc = lc_path.read_text().strip()
            if lc >= today.isoformat():
                self._daily_update_done_today = today
                return False

        # Only run after 16:30 CST (UTC+8)
        # We approximate by checking local time — assumes running in CST timezone
        cutoff_hour, cutoff_min = 16, 30
        if now.hour < cutoff_hour or (now.hour == cutoff_hour and now.minute < cutoff_min):
            return False

        return True

    def _run_daily_update(self):
        """Daily update: refresh today's constituents, detect new symbols, update bars."""
        today_str = date.today().isoformat()
        log.info("daily update started for %s", today_str)

        # 1. Refresh today's index constituents
        new_symbols = set()
        known_symbols = set(_all_index_symbols(self.data_dir))

        for index_name in _INDEXES:
            if _shutdown:
                return
            symbols = _query_index_constituents(index_name, today_str)
            if symbols:
                index_dir = self.data_dir / "cn" / "index" / index_name
                _write_index_file(index_dir / f"{today_str}.txt", symbols)
                log.info("daily index %s: %d symbols", index_name, len(symbols))
                for s in symbols:
                    if s not in known_symbols:
                        new_symbols.add(s)

        # 2. Backfill new symbols fully
        if new_symbols:
            log.info("detected %d new symbols, backfilling", len(new_symbols))
            for symbol in sorted(new_symbols):
                if _shutdown:
                    return
                rows = _query_daily_bars(symbol, self.start_date, today_str)
                if rows:
                    self._write_bars(symbol, rows)
                    log.info("new symbol backfill %s: %d bars", symbol, len(rows))

        # 3. Update bars for all current constituents
        current_symbols = set()
        for index_name in _INDEXES:
            index_dir = self.data_dir / "cn" / "index" / index_name
            file_path = index_dir / f"{today_str}.txt"
            current_symbols.update(_read_index_file(file_path))

        if current_symbols:
            log.info("updating bars for %d current constituents", len(current_symbols))
            count = 0
            for symbol in sorted(current_symbols):
                if _shutdown:
                    return
                rows = _query_daily_bars(symbol, today_str, today_str)
                if rows:
                    self._write_bars(symbol, rows)
                    count += 1
            log.info("daily bar update: %d/%d symbols had data", count, len(current_symbols))

        # 4. Mark completed
        self.daily_dir.mkdir(parents=True, exist_ok=True)
        (self.daily_dir / ".last-completed").write_text(today_str + "\n")
        self._daily_update_done_today = date.today()
        log.info("daily update complete for %s", today_str)

    # -------------------------------------------------------------------
    # Bar writing helper
    # -------------------------------------------------------------------

    def _write_bars(self, symbol: str, rows: list[dict]):
        """Write bar rows to per-year parquet files with merge-on-write."""
        # Group by year
        by_year: dict[str, list[dict]] = {}
        for r in rows:
            year = r["date"][:4]
            by_year.setdefault(year, []).append(r)

        for year, year_rows in by_year.items():
            path = self.daily_dir / symbol / f"{year}.parquet"
            _write_bars_parquet(path, year_rows)

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
    """Load data_dir and start_date from config YAML + env overrides.

    Returns (data_dir, start_date).
    """
    data_dir = os.environ.get("DATA_1", "")
    start_date = "2005-01-01"

    if os.path.exists(config_path):
        with open(config_path) as f:
            cfg = yaml.safe_load(f) or {}

        storage = cfg.get("storage", {})
        if not data_dir:
            raw = storage.get("data_dir", "")
            # Expand ${DATA_1} style references
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

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    data_dir, start_date = load_config(args.config)
    log.info("data_dir=%s start_date=%s", data_dir, start_date)

    daemon = CNBaoStockDaemon(data_dir=data_dir, start_date=start_date)
    daemon.run()


if __name__ == "__main__":
    main()
