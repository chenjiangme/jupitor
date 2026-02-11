#!/usr/bin/env python3
from __future__ import annotations

"""us_index_data: build per-date SPX and NDX constituent files.

Downloads S&P 500 historical components from GitHub (fja05680/sp500) and
reconstructs NASDAQ-100 membership from Wikipedia. Writes one text file per
trading day containing sorted member symbols.

One-shot build (not a daemon). Re-run to update; existing dates are skipped.

Output:
  $DATA_1/us/index/spx/<YYYY-MM-DD>.txt   (~500 symbols per file)
  $DATA_1/us/index/ndx/<YYYY-MM-DD>.txt   (~100 symbols per file)

Usage:
  python python/scripts/us_index_data.py [--config config/jupitor.yaml]
"""

import argparse
import csv
import io
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
import yaml

log = logging.getLogger("us-index-data")

# US federal holidays (fixed dates + observed rules are complex; use a simple
# set of known holidays by year-month-day pattern). For production accuracy we
# generate weekday dates and accept minor over-generation — the per-date files
# that correspond to non-trading days will simply list the same constituents.

_US_HOLIDAYS_FIXED = {
    (1, 1),   # New Year's Day
    (7, 4),   # Independence Day
    (12, 25), # Christmas Day
}


def _is_weekday(d: date) -> bool:
    return d.weekday() < 5


def _trading_days(start: date, end: date) -> list[date]:
    """Generate weekday dates in [start, end]. Not holiday-aware (good enough
    for constituent file generation — extra dates are harmless)."""
    days = []
    d = start
    while d <= end:
        if _is_weekday(d):
            days.append(d)
        d += timedelta(days=1)
    return days


# ---------------------------------------------------------------------------
# S&P 500 from GitHub (fja05680/sp500)
# ---------------------------------------------------------------------------

_SP500_CSV_URL = (
    "https://raw.githubusercontent.com/fja05680/sp500/"
    "master/S%26P%20500%20Historical%20Components%20%26%20Changes(01-17-2026).csv"
)


def _download_sp500_csv() -> str:
    """Download the S&P 500 historical components CSV from GitHub."""
    log.info("downloading S&P 500 CSV from GitHub...")
    resp = requests.get(_SP500_CSV_URL, timeout=60)
    resp.raise_for_status()
    log.info("downloaded %d bytes", len(resp.content))
    return resp.text


def _parse_sp500_csv(text: str) -> list[tuple[date, list[str]]]:
    """Parse the CSV into (date, [symbols]) pairs.

    Format: date,"TMC-200006,AAPL,MSFT,..."
    Tickers with -YYYYMM suffix are historical names; strip the suffix.
    """
    rows = []
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)
    if not header:
        return rows

    for line in reader:
        if len(line) < 2:
            continue
        date_str = line[0].strip()
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        # Tickers are in a single comma-separated field (the second column)
        raw_tickers = line[1] if len(line) == 2 else ",".join(line[1:])
        symbols = []
        for ticker in raw_tickers.split(","):
            ticker = ticker.strip()
            if not ticker:
                continue
            # Strip -YYYYMM suffix (historical name marker)
            ticker = re.sub(r"-\d{6}$", "", ticker)
            # Normalize: uppercase, replace . with .
            ticker = ticker.upper().strip()
            if ticker:
                symbols.append(ticker)

        if symbols:
            rows.append((d, sorted(set(symbols))))

    # Sort by date ascending
    rows.sort(key=lambda x: x[0])
    return rows


def _build_spx_files(data_dir: Path, rows: list[tuple[date, list[str]]]):
    """Write per-date SPX constituent files.

    The CSV provides snapshots at change dates. For each snapshot, we write
    all trading days from that snapshot date until the next snapshot date.
    """
    spx_dir = data_dir / "us" / "index" / "spx"
    spx_dir.mkdir(parents=True, exist_ok=True)

    if not rows:
        log.warning("no S&P 500 data to process")
        return

    written = 0
    skipped = 0

    # For each pair of consecutive snapshots, fill trading days with the earlier
    # snapshot's constituent list.
    for i, (snap_date, symbols) in enumerate(rows):
        if i + 1 < len(rows):
            next_date = rows[i + 1][0]
        else:
            next_date = date.today()

        # Generate trading days for [snap_date, next_date)
        end = next_date - timedelta(days=1) if i + 1 < len(rows) else next_date
        for d in _trading_days(snap_date, end):
            path = spx_dir / f"{d.isoformat()}.txt"
            if path.exists():
                skipped += 1
                continue
            path.write_text("\n".join(symbols) + "\n")
            written += 1

    log.info("SPX: wrote %d files, skipped %d existing", written, skipped)


# ---------------------------------------------------------------------------
# NASDAQ-100 from Wikipedia
# ---------------------------------------------------------------------------

_NDX_CURRENT_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"


def _fetch_wikipedia_page(url: str) -> str:
    """Fetch a Wikipedia page's HTML."""
    resp = requests.get(url, headers={"User-Agent": "jupitor-index-builder/1.0"}, timeout=60)
    resp.raise_for_status()
    return resp.text


def _parse_ndx_current_members(html: str) -> list[str]:
    """Extract current NASDAQ-100 member tickers from the Wikipedia page.

    Looks for the constituents table with 'Ticker' column.
    """
    # Use simple regex to find table rows with ticker symbols
    # The constituents table typically has columns: Company, Ticker, GICS Sector/Sub-Industry
    symbols = []

    # Find all tables
    tables = re.findall(r"<table[^>]*>.*?</table>", html, re.DOTALL)

    for table in tables:
        # Look for a table that has "Ticker" in the header
        if "Ticker" not in table and "ticker" not in table.lower():
            continue

        # Find header row to identify Ticker column index
        header_match = re.search(r"<tr>(.*?)</tr>", table, re.DOTALL)
        if not header_match:
            continue

        headers = re.findall(r"<th[^>]*>(.*?)</th>", header_match.group(1), re.DOTALL)
        headers = [re.sub(r"<[^>]+>", "", h).strip() for h in headers]

        ticker_idx = None
        for idx, h in enumerate(headers):
            if "ticker" in h.lower():
                ticker_idx = idx
                break

        if ticker_idx is None:
            continue

        # Extract data rows
        data_rows = re.findall(r"<tr>(.*?)</tr>", table, re.DOTALL)
        for row in data_rows[1:]:  # skip header
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
            if len(cells) > ticker_idx:
                raw = re.sub(r"<[^>]+>", "", cells[ticker_idx]).strip()
                if raw and re.match(r"^[A-Z]{1,5}$", raw):
                    symbols.append(raw)

    return sorted(set(symbols))


def _parse_ndx_changes(html: str) -> list[tuple[date, str, str]]:
    """Parse NASDAQ-100 changes from Wikipedia.

    Returns list of (effective_date, action, ticker) where action is 'added' or 'removed'.
    Sorted by date descending (most recent first).
    """
    changes = []

    # Find the "Changes to the composition" or similar section
    # Look for tables after a heading about changes
    # The changes table typically has: Date, Added, Removed columns

    tables = re.findall(r"<table[^>]*class=\"wikitable[^\"]*\"[^>]*>.*?</table>", html, re.DOTALL)

    for table in tables:
        # Look for tables with date, added/removed pattern
        header_match = re.search(r"<tr>(.*?)</tr>", table, re.DOTALL)
        if not header_match:
            continue

        headers = re.findall(r"<th[^>]*>(.*?)</th>", header_match.group(1), re.DOTALL)
        headers = [re.sub(r"<[^>]+>", "", h).strip().lower() for h in headers]

        # We need Date, Added, Removed columns
        date_idx = None
        added_idx = None
        removed_idx = None

        for idx, h in enumerate(headers):
            if "date" in h:
                date_idx = idx
            elif "added" in h:
                added_idx = idx
            elif "removed" in h:
                removed_idx = idx

        if date_idx is None or (added_idx is None and removed_idx is None):
            continue

        data_rows = re.findall(r"<tr>(.*?)</tr>", table, re.DOTALL)
        current_date = None

        for row in data_rows[1:]:
            cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.DOTALL)
            if not cells:
                continue

            # Handle rowspan: if we have fewer cells, the date cell is spanning
            if len(cells) > date_idx:
                raw_date = re.sub(r"<[^>]+>", "", cells[date_idx]).strip()
                # Try to parse various date formats
                parsed = _try_parse_date(raw_date)
                if parsed:
                    current_date = parsed

            if current_date is None:
                continue

            # Extract added ticker
            if added_idx is not None and len(cells) > added_idx:
                raw = re.sub(r"<[^>]+>", "", cells[added_idx]).strip()
                tickers = _extract_tickers(raw)
                for t in tickers:
                    changes.append((current_date, "added", t))

            # Extract removed ticker
            if removed_idx is not None and len(cells) > removed_idx:
                raw = re.sub(r"<[^>]+>", "", cells[removed_idx]).strip()
                tickers = _extract_tickers(raw)
                for t in tickers:
                    changes.append((current_date, "removed", t))

    # Sort by date descending
    changes.sort(key=lambda x: x[0], reverse=True)
    return changes


def _try_parse_date(s: str) -> date | None:
    """Try parsing a date string from various Wikipedia formats."""
    s = s.strip()
    # Remove references like [1], [2]
    s = re.sub(r"\[.*?\]", "", s).strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _extract_tickers(raw: str) -> list[str]:
    """Extract ticker symbols from a cell that may contain names and tickers."""
    # Common patterns: "AAPL", "Apple Inc. (AAPL)", just text
    tickers = []
    # Look for explicit ticker patterns (all caps, 1-5 chars)
    for match in re.finditer(r"\b([A-Z]{1,5})\b", raw):
        candidate = match.group(1)
        # Filter out common non-ticker words
        if candidate not in {"THE", "AND", "FOR", "INC", "LTD", "LLC", "CO",
                             "ETF", "LP", "NV", "SA", "SE", "PLC", "AG",
                             "NA", "DE", "NEW", "OLD", "AS", "AB", "IN",
                             "IT", "ON", "AT", "BY", "TO", "OR", "AN",
                             "IS", "IF", "UP", "NO", "SO", "DO", "BE"}:
            tickers.append(candidate)
    return tickers


def _reconstruct_ndx_history(
    current_members: list[str],
    changes: list[tuple[date, str, str]],
    start_date: date,
) -> list[tuple[date, list[str]]]:
    """Reconstruct NDX membership history by working backwards from current.

    Returns (date, [symbols]) pairs for each change date, sorted ascending.
    """
    # Start from current membership
    members = set(current_members)
    snapshots = [(date.today(), sorted(members))]

    # Work backwards through changes (already sorted descending)
    for change_date, action, ticker in changes:
        if change_date < start_date:
            break
        if action == "added":
            # Was added on this date, so before this date it wasn't there
            members.discard(ticker)
        elif action == "removed":
            # Was removed on this date, so before this date it was there
            members.add(ticker)
        snapshots.append((change_date, sorted(members)))

    # Sort ascending by date
    snapshots.sort(key=lambda x: x[0])
    return snapshots


def _build_ndx_files(data_dir: Path, snapshots: list[tuple[date, list[str]]]):
    """Write per-date NDX constituent files from snapshots."""
    ndx_dir = data_dir / "us" / "index" / "ndx"
    ndx_dir.mkdir(parents=True, exist_ok=True)

    if not snapshots:
        log.warning("no NASDAQ-100 data to process")
        return

    written = 0
    skipped = 0

    for i, (snap_date, symbols) in enumerate(snapshots):
        if i + 1 < len(snapshots):
            next_date = snapshots[i + 1][0]
        else:
            next_date = date.today()

        end = next_date - timedelta(days=1) if i + 1 < len(snapshots) else next_date
        for d in _trading_days(snap_date, end):
            path = ndx_dir / f"{d.isoformat()}.txt"
            if path.exists():
                skipped += 1
                continue
            path.write_text("\n".join(symbols) + "\n")
            written += 1

    log.info("NDX: wrote %d files, skipped %d existing", written, skipped)


# ---------------------------------------------------------------------------
# Main build functions
# ---------------------------------------------------------------------------

def build_spx(data_dir: Path):
    """Download and build S&P 500 per-date constituent files."""
    log.info("=== Building S&P 500 constituent history ===")
    csv_text = _download_sp500_csv()
    rows = _parse_sp500_csv(csv_text)
    log.info("parsed %d S&P 500 snapshots (%s to %s)",
             len(rows),
             rows[0][0].isoformat() if rows else "?",
             rows[-1][0].isoformat() if rows else "?")
    _build_spx_files(data_dir, rows)


def build_ndx(data_dir: Path):
    """Scrape Wikipedia and build NASDAQ-100 per-date constituent files."""
    log.info("=== Building NASDAQ-100 constituent history ===")
    html = _fetch_wikipedia_page(_NDX_CURRENT_URL)

    current = _parse_ndx_current_members(html)
    log.info("current NASDAQ-100 members: %d", len(current))
    if not current:
        log.error("failed to parse current NDX members from Wikipedia")
        return

    changes = _parse_ndx_changes(html)
    log.info("parsed %d NDX changes from Wikipedia", len(changes))

    # Reconstruct from earliest change date
    earliest = min((c[0] for c in changes), default=date.today()) if changes else date.today()
    log.info("reconstructing NDX history from %s", earliest.isoformat())

    snapshots = _reconstruct_ndx_history(current, changes, earliest)
    log.info("generated %d NDX snapshots", len(snapshots))

    _build_ndx_files(data_dir, snapshots)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> str:
    """Load data_dir from config YAML + env overrides."""
    data_dir = os.environ.get("DATA_1", "")

    if os.path.exists(config_path):
        with open(config_path) as f:
            cfg = yaml.safe_load(f) or {}
        storage = cfg.get("storage", {})
        if not data_dir:
            raw = storage.get("data_dir", "")
            data_dir = os.path.expandvars(raw)

    if not data_dir:
        print("ERROR: DATA_1 env var or storage.data_dir config required", file=sys.stderr)
        sys.exit(1)

    return data_dir


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build US index constituent history (SPX + NDX)")
    parser.add_argument("--config", default="config/jupitor.yaml", help="Config file path")
    parser.add_argument("--spx-only", action="store_true", help="Only build S&P 500")
    parser.add_argument("--ndx-only", action="store_true", help="Only build NASDAQ-100")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    data_dir = Path(load_config(args.config))
    log.info("data_dir=%s", data_dir)

    if not args.ndx_only:
        build_spx(data_dir)
    if not args.spx_only:
        build_ndx(data_dir)

    log.info("done")


if __name__ == "__main__":
    main()
