#!/usr/bin/env python3
from __future__ import annotations

"""us_index_data: download reference CSVs and build per-date SPX/NDX files.

Steps (in order):
  1. Download US stock/ETF reference CSVs from Dropbox (date-stamped)
  2. Build S&P 500 per-date constituent files from GitHub (fja05680/sp500)
  3. Build NASDAQ-100 per-date constituent files from Wikipedia
  4. Validate Dropbox index column against GitHub/Wikipedia files

One-shot build (not a daemon). Re-run to update; existing dates are skipped.

Output:
  reference/us/us_stock_YYYY-MM-DD.csv   (from Dropbox)
  reference/us/us_etf_YYYY-MM-DD.csv     (from Dropbox)
  $DATA_1/us/index/spx/<YYYY-MM-DD>.txt  (~500 symbols per file)
  $DATA_1/us/index/ndx/<YYYY-MM-DD>.txt  (~100 symbols per file)

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
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
import yaml

log = logging.getLogger("us-index-data")


def _is_weekday(d: date) -> bool:
    return d.weekday() < 5


def _trading_days(start: date, end: date) -> list[date]:
    """Generate weekday dates in [start, end]."""
    days = []
    d = start
    while d <= end:
        if _is_weekday(d):
            days.append(d)
        d += timedelta(days=1)
    return days


# ---------------------------------------------------------------------------
# Step 1: Dropbox reference download
# ---------------------------------------------------------------------------

_DROPBOX_URL_ENV = "DROPBOX_INFO_FOLDER"


def _download_dropbox_zip(url: str) -> bytes:
    """Download a Dropbox shared folder as a zip."""
    dl_url = url.replace("dl=0", "dl=1")
    log.info("downloading from Dropbox...")
    resp = requests.get(dl_url, timeout=120)
    resp.raise_for_status()
    log.info("downloaded %d bytes", len(resp.content))
    return resp.content


def _extract_csv_from_zip(zip_data: bytes, prefix: str) -> bytes:
    """Extract a CSV file matching the given prefix from a zip archive."""
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        for name in zf.namelist():
            basename = name.rsplit("/", 1)[-1]
            if basename.startswith(prefix) and basename.endswith(".csv"):
                return zf.read(name)
    raise FileNotFoundError(f"no file matching '{prefix}*.csv' in zip")


def _find_latest_ref_file(ref_dir: Path, prefix: str) -> Path | None:
    """Find the latest date-stamped file matching prefix_YYYY-MM-DD.csv."""
    matches = sorted(ref_dir.glob(f"{prefix}_????-??-??.csv"))
    return matches[-1] if matches else None


def download_reference(ref_dir: Path):
    """Download US stock/ETF reference CSVs from Dropbox with date stamps."""
    log.info("=== Step 1: Download reference CSVs from Dropbox ===")

    dropbox_url = os.environ.get(_DROPBOX_URL_ENV)
    if not dropbox_url:
        log.warning("%s env var not set, skipping Dropbox download", _DROPBOX_URL_ENV)
        return

    ref_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()

    stock_path = ref_dir / f"us_stock_{today}.csv"
    etf_path = ref_dir / f"us_etf_{today}.csv"
    if stock_path.exists() and etf_path.exists():
        log.info("today's files already exist, skipping download")
        return

    zip_data = _download_dropbox_zip(dropbox_url)

    stock_data = _extract_csv_from_zip(zip_data, "us_stock")
    stock_path.write_bytes(stock_data)
    log.info("saved %s (%d bytes)", stock_path, len(stock_data))

    etf_data = _extract_csv_from_zip(zip_data, "us_etf")
    etf_path.write_bytes(etf_data)
    log.info("saved %s (%d bytes)", etf_path, len(etf_data))


# ---------------------------------------------------------------------------
# Step 2: S&P 500 from GitHub (fja05680/sp500)
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

        raw_tickers = line[1] if len(line) == 2 else ",".join(line[1:])
        symbols = []
        for ticker in raw_tickers.split(","):
            ticker = ticker.strip()
            if not ticker:
                continue
            ticker = re.sub(r"-\d{6}$", "", ticker)
            ticker = ticker.upper().strip()
            if ticker:
                symbols.append(ticker)

        if symbols:
            rows.append((d, sorted(set(symbols))))

    rows.sort(key=lambda x: x[0])
    return rows


def _build_spx_files(data_dir: Path, rows: list[tuple[date, list[str]]]):
    """Write per-date SPX constituent files."""
    spx_dir = data_dir / "us" / "index" / "spx"
    spx_dir.mkdir(parents=True, exist_ok=True)

    if not rows:
        log.warning("no S&P 500 data to process")
        return

    written = 0
    skipped = 0

    for i, (snap_date, symbols) in enumerate(rows):
        if i + 1 < len(rows):
            next_date = rows[i + 1][0]
        else:
            next_date = date.today()

        end = next_date - timedelta(days=1) if i + 1 < len(rows) else next_date
        for d in _trading_days(snap_date, end):
            path = spx_dir / f"{d.isoformat()}.txt"
            if path.exists():
                skipped += 1
                continue
            path.write_text("\n".join(symbols) + "\n")
            written += 1

    log.info("SPX: wrote %d files, skipped %d existing", written, skipped)


def build_spx(data_dir: Path):
    """Download and build S&P 500 per-date constituent files."""
    log.info("=== Step 2: Build S&P 500 constituent history ===")
    csv_text = _download_sp500_csv()
    rows = _parse_sp500_csv(csv_text)
    log.info("parsed %d S&P 500 snapshots (%s to %s)",
             len(rows),
             rows[0][0].isoformat() if rows else "?",
             rows[-1][0].isoformat() if rows else "?")
    _build_spx_files(data_dir, rows)


# ---------------------------------------------------------------------------
# Step 3: NASDAQ-100 from Wikipedia
# ---------------------------------------------------------------------------

_NDX_CURRENT_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"


def _fetch_wikipedia_page(url: str) -> str:
    """Fetch a Wikipedia page's HTML."""
    resp = requests.get(url, headers={"User-Agent": "jupitor-index-builder/1.0"}, timeout=60)
    resp.raise_for_status()
    return resp.text


def _parse_ndx_current_members(html: str) -> list[str]:
    """Extract current NASDAQ-100 member tickers from the Wikipedia page."""
    symbols = []
    tables = re.findall(r"<table[^>]*>.*?</table>", html, re.DOTALL)

    for table in tables:
        if "Ticker" not in table and "ticker" not in table.lower():
            continue

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

        data_rows = re.findall(r"<tr>(.*?)</tr>", table, re.DOTALL)
        for row in data_rows[1:]:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
            if len(cells) > ticker_idx:
                raw = re.sub(r"<[^>]+>", "", cells[ticker_idx]).strip()
                if raw and re.match(r"^[A-Z]{1,5}$", raw):
                    symbols.append(raw)

    return sorted(set(symbols))


def _parse_ndx_changes(html: str) -> list[tuple[date, str, str]]:
    """Parse NASDAQ-100 changes from Wikipedia."""
    changes = []
    tables = re.findall(r"<table[^>]*class=\"wikitable[^\"]*\"[^>]*>.*?</table>", html, re.DOTALL)

    for table in tables:
        header_match = re.search(r"<tr>(.*?)</tr>", table, re.DOTALL)
        if not header_match:
            continue

        headers = re.findall(r"<th[^>]*>(.*?)</th>", header_match.group(1), re.DOTALL)
        headers = [re.sub(r"<[^>]+>", "", h).strip().lower() for h in headers]

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

            if len(cells) > date_idx:
                raw_date = re.sub(r"<[^>]+>", "", cells[date_idx]).strip()
                parsed = _try_parse_date(raw_date)
                if parsed:
                    current_date = parsed

            if current_date is None:
                continue

            if added_idx is not None and len(cells) > added_idx:
                raw = re.sub(r"<[^>]+>", "", cells[added_idx]).strip()
                for t in _extract_tickers(raw):
                    changes.append((current_date, "added", t))

            if removed_idx is not None and len(cells) > removed_idx:
                raw = re.sub(r"<[^>]+>", "", cells[removed_idx]).strip()
                for t in _extract_tickers(raw):
                    changes.append((current_date, "removed", t))

    changes.sort(key=lambda x: x[0], reverse=True)
    return changes


def _try_parse_date(s: str) -> date | None:
    s = s.strip()
    s = re.sub(r"\[.*?\]", "", s).strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _extract_tickers(raw: str) -> list[str]:
    tickers = []
    for match in re.finditer(r"\b([A-Z]{1,5})\b", raw):
        candidate = match.group(1)
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
    members = set(current_members)
    snapshots = [(date.today(), sorted(members))]

    for change_date, action, ticker in changes:
        if change_date < start_date:
            break
        if action == "added":
            members.discard(ticker)
        elif action == "removed":
            members.add(ticker)
        snapshots.append((change_date, sorted(members)))

    snapshots.sort(key=lambda x: x[0])
    return snapshots


def _build_ndx_files(data_dir: Path, snapshots: list[tuple[date, list[str]]]):
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


def build_ndx(data_dir: Path):
    """Scrape Wikipedia and build NASDAQ-100 per-date constituent files."""
    log.info("=== Step 3: Build NASDAQ-100 constituent history ===")
    html = _fetch_wikipedia_page(_NDX_CURRENT_URL)

    current = _parse_ndx_current_members(html)
    log.info("current NASDAQ-100 members: %d", len(current))
    if not current:
        log.error("failed to parse current NDX members from Wikipedia")
        return

    changes = _parse_ndx_changes(html)
    log.info("parsed %d NDX changes from Wikipedia", len(changes))

    earliest = min((c[0] for c in changes), default=date.today()) if changes else date.today()
    log.info("reconstructing NDX history from %s", earliest.isoformat())

    snapshots = _reconstruct_ndx_history(current, changes, earliest)
    log.info("generated %d NDX snapshots", len(snapshots))

    _build_ndx_files(data_dir, snapshots)


# ---------------------------------------------------------------------------
# Step 4: Validate Dropbox index column against GitHub/Wikipedia
# ---------------------------------------------------------------------------

def _parse_spx_ndx_from_stock_csv(path: Path) -> tuple[set[str], set[str]]:
    """Parse the index column from us_stock CSV to extract SPX and NDX members."""
    spx = set()
    ndx = set()

    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = row.get("\u5546\u54c1\u4ee3\u7801", "").strip().upper()  # 商品代码
            indexes = row.get("\u6307\u6570", "")  # 指数
            if not symbol:
                continue
            if "S&P 500" in indexes:
                spx.add(symbol)
            if "NASDAQ 100" in indexes:
                ndx.add(symbol)

    return spx, ndx


def _read_index_file_set(path: Path) -> set[str]:
    """Read symbols from an index file."""
    if not path.exists():
        return set()
    symbols = set()
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            symbols.add(line.split(",")[0].upper())
    return symbols


def validate_index(data_dir: Path, ref_dir: Path):
    """Compare Dropbox-sourced SPX/NDX membership against index files."""
    log.info("=== Step 4: Validate SPX/NDX against Dropbox ===")

    latest_stock = _find_latest_ref_file(ref_dir, "us_stock")
    if not latest_stock:
        log.warning("no us_stock reference file found, skipping validation")
        return

    dropbox_spx, dropbox_ndx = _parse_spx_ndx_from_stock_csv(latest_stock)
    log.info("dropbox SPX: %d members, NDX: %d members", len(dropbox_spx), len(dropbox_ndx))

    spx_dir = data_dir / "us" / "index" / "spx"
    ndx_dir = data_dir / "us" / "index" / "ndx"

    for label, index_dir, dropbox_set in [
        ("SPX", spx_dir, dropbox_spx),
        ("NDX", ndx_dir, dropbox_ndx),
    ]:
        files = sorted(index_dir.glob("*.txt"))
        if not files:
            log.warning("no %s index files found for validation", label)
            continue

        latest_path = files[-1]
        index_set = _read_index_file_set(latest_path)
        only_index = index_set - dropbox_set
        only_dropbox = dropbox_set - index_set

        if not only_index and not only_dropbox:
            log.info("%s validation OK: %d members match (index date: %s)",
                     label, len(index_set), latest_path.stem)
        else:
            log.warning("%s validation MISMATCH (index date: %s):", label, latest_path.stem)
            log.warning("  index: %d, dropbox: %d", len(index_set), len(dropbox_set))
            if only_index:
                log.warning("  only in index file: %s", ", ".join(sorted(only_index)))
            if only_dropbox:
                log.warning("  only in dropbox: %s", ", ".join(sorted(only_dropbox)))


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
    parser = argparse.ArgumentParser(
        description="Download reference CSVs and build US index constituent history")
    parser.add_argument("--config", default="config/jupitor.yaml", help="Config file path")
    parser.add_argument("--ref-dir", default="reference/us", help="Reference directory")
    parser.add_argument("--spx-only", action="store_true", help="Only build S&P 500")
    parser.add_argument("--ndx-only", action="store_true", help="Only build NASDAQ-100")
    parser.add_argument("--skip-download", action="store_true", help="Skip Dropbox download")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    data_dir = Path(load_config(args.config))
    ref_dir = Path(args.ref_dir)
    log.info("data_dir=%s ref_dir=%s", data_dir, ref_dir)

    errors = []

    # Step 1: Download reference CSVs
    if not args.skip_download:
        try:
            download_reference(ref_dir)
        except Exception as e:
            log.error("Dropbox download failed: %s", e)
            errors.append(f"Dropbox: {e}")

    # Step 2: Build SPX
    if not args.ndx_only:
        try:
            build_spx(data_dir)
        except Exception as e:
            log.error("SPX build failed: %s", e)
            errors.append(f"SPX: {e}")

    # Step 3: Build NDX
    if not args.spx_only:
        try:
            build_ndx(data_dir)
        except Exception as e:
            log.error("NDX build failed: %s", e)
            errors.append(f"NDX: {e}")

    # Step 4: Validate
    try:
        validate_index(data_dir, ref_dir)
    except Exception as e:
        log.error("validation failed: %s", e)
        errors.append(f"validation: {e}")

    if errors:
        log.error("FAILED — %d error(s): %s", len(errors), "; ".join(errors))
        sys.exit(1)

    log.info("done")


if __name__ == "__main__":
    main()
