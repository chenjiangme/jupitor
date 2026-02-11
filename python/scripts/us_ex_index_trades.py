#!/usr/bin/env python3
"""Analyze the most traded ex-index stocks from stock-trades-ex-index parquet files.

Reads the latest (or specified) date's parquet file, aggregates per symbol, and
prints a ranked table of the top 50 stocks sorted by the chosen metric.

Usage:
  python python/scripts/us_ex_index_trades.py                        # latest date, by dollar vol
  python python/scripts/us_ex_index_trades.py --sort trades          # by trade count
  python python/scripts/us_ex_index_trades.py --sort volume          # by share volume
  python python/scripts/us_ex_index_trades.py --date 2026-02-10
  python python/scripts/us_ex_index_trades.py --top 100
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from jupitor.config import get_data_dir


def find_dates(data_dir: Path) -> list[str]:
    """Return sorted list of available date strings (YYYY-MM-DD)."""
    ex_dir = data_dir / "us" / "stock-trades-ex-index"
    if not ex_dir.is_dir():
        return []
    dates = sorted(p.stem for p in ex_dir.glob("*.parquet"))
    return dates


def load_and_aggregate(path: Path) -> pd.DataFrame:
    """Load a parquet file and return per-symbol aggregation."""
    df = pd.read_parquet(path, columns=["symbol", "price", "size", "exchange"])
    df["dollar"] = df["price"] * df["size"]

    agg = df.groupby("symbol", sort=False).agg(
        trades=("price", "count"),
        volume=("size", "sum"),
        dollar_vol=("dollar", "sum"),
        low=("price", "min"),
        high=("price", "max"),
        exchanges=("exchange", "nunique"),
    )
    agg["vwap"] = agg["dollar_vol"] / agg["volume"]
    return agg


def fmt_dollar(val: float) -> str:
    """Format dollar volume as human-readable string."""
    if val >= 1e9:
        return f"${val / 1e9:.1f}B"
    if val >= 1e6:
        return f"${val / 1e6:.1f}M"
    if val >= 1e3:
        return f"${val / 1e3:.0f}K"
    return f"${val:.0f}"


SORT_LABELS = {
    "dollar_vol": "Dollar Volume",
    "trades": "Trade Count",
    "volume": "Share Volume",
}


def print_table(agg: pd.DataFrame, top_n: int, date_str: str,
                sort_by: str, prev_symbols: set[str] | None) -> None:
    """Print formatted table of top stocks."""
    print(f"\nEx-Index Most Traded Stocks — {date_str}  (sorted by {SORT_LABELS[sort_by]})")
    print("=" * 100)
    header = (f"{'Rank':>4}  {'Symbol':<8} {'Trades':>10} {'Volume':>14} "
              f"{'Dollar Vol':>12} {'VWAP':>9} {'Low':>9} {'High':>9} {'Exch':>4}")
    if prev_symbols is not None:
        header += "  New"
    print(header)
    print("-" * 100)

    top = agg.head(top_n)
    for i, (sym, row) in enumerate(top.iterrows(), 1):
        line = (f"{i:>4}  {sym:<8} {row['trades']:>10,.0f} {row['volume']:>14,.0f} "
                f"{fmt_dollar(row['dollar_vol']):>12} {row['vwap']:>9.2f} "
                f"{row['low']:>9.2f} {row['high']:>9.2f} {row['exchanges']:>4.0f}")
        if prev_symbols is not None:
            line += "    *" if sym not in prev_symbols else ""
        print(line)

    print("-" * 100)
    print(f"Total: {len(agg):,} symbols | {agg['trades'].sum():,.0f} trades | "
          f"{agg['volume'].sum():,.0f} shares | {fmt_dollar(agg['dollar_vol'].sum())} dollar volume")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze most traded ex-index stocks")
    parser.add_argument("--date", help="Date to analyze (YYYY-MM-DD). Default: latest available.")
    parser.add_argument("--top", type=int, default=50, help="Number of top stocks to show (default 50)")
    parser.add_argument("--sort", choices=["dollar_vol", "trades", "volume"], default="dollar_vol",
                        help="Sort metric (default: dollar_vol)")
    args = parser.parse_args()

    data_dir = get_data_dir()
    ex_dir = data_dir / "us" / "stock-trades-ex-index"

    dates = find_dates(data_dir)
    if not dates:
        print(f"No parquet files found in {ex_dir}", file=sys.stderr)
        sys.exit(1)

    date_str = args.date if args.date else dates[-1]
    path = ex_dir / f"{date_str}.parquet"
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        print(f"Available dates: {dates[0]} .. {dates[-1]}", file=sys.stderr)
        sys.exit(1)

    agg = load_and_aggregate(path)
    agg.sort_values(args.sort, ascending=False, inplace=True)

    # Try loading previous day for newcomer detection
    prev_symbols: set[str] | None = None
    idx = dates.index(date_str) if date_str in dates else -1
    if idx > 0:
        prev_date = dates[idx - 1]
        prev_path = ex_dir / f"{prev_date}.parquet"
        prev_agg = load_and_aggregate(prev_path)
        prev_agg.sort_values(args.sort, ascending=False, inplace=True)
        prev_symbols = set(prev_agg.head(args.top).index)

    print_table(agg, args.top, date_str, args.sort, prev_symbols)


if __name__ == "__main__":
    main()
