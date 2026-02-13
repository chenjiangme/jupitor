#!/usr/bin/env python3
"""Analyze the most traded ex-index stocks from stock-trades-ex-index parquet files.

Reads the latest (or specified) date's parquet file, aggregates per symbol, and
prints a ranked table of the top 50 stocks sorted by the chosen metric.

Supports a --session mode that splits trades into PM (previous post-market +
today's pre-market) and regular (9:30 AM - 4:00 PM ET) sessions, showing
open→high gain% and trade counts for each.

Note: timestamps in the parquet files are ET stored as UTC (Go code converts
before writing). The window is (prev_date 4PM ET, date 4PM ET].

Usage:
  python python/scripts/us_ex_index_trades.py                        # by dollar vol
  python python/scripts/us_ex_index_trades.py --sort trades          # by trade count
  python python/scripts/us_ex_index_trades.py --session              # PM vs regular session
  python python/scripts/us_ex_index_trades.py --session --sort pm_gain
  python python/scripts/us_ex_index_trades.py --date 2026-02-10 --top 100
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
    return sorted(p.stem for p in ex_dir.glob("*.parquet"))


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


def _agg_session(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate a session DataFrame: open (first trade), high, trades, dollar_vol, gain%."""
    if df.empty:
        return pd.DataFrame(columns=["open", "high", "trades", "dollar_vol", "gain_pct"])
    df = df.sort_values("timestamp")
    first = df.groupby("symbol", sort=False).first()["price"].rename("open")
    agg = df.groupby("symbol", sort=False).agg(
        high=("price", "max"),
        trades=("price", "count"),
        dollar_vol=("dollar", "sum"),
    )
    agg["open"] = first
    agg["gain_pct"] = (agg["high"] - agg["open"]) / agg["open"] * 100
    return agg


def load_session_data(path: Path, date_str: str) -> pd.DataFrame:
    """Load parquet, split into PM and regular sessions, return merged per-symbol stats."""
    df = pd.read_parquet(path, columns=["symbol", "timestamp", "price", "size"])
    df["dollar"] = df["price"] * df["size"]

    # Timestamps are ET stored as UTC (Go converts before writing)
    regular_open = pd.Timestamp(f"{date_str} 09:30:00", tz="UTC")

    pm_agg = _agg_session(df[df["timestamp"] < regular_open])
    reg_agg = _agg_session(df[df["timestamp"] >= regular_open])

    merged = pm_agg[["trades", "gain_pct", "open", "high", "dollar_vol"]].rename(columns={
        "trades": "pm_trades", "gain_pct": "pm_gain", "open": "pm_open",
        "high": "pm_high", "dollar_vol": "pm_dvol",
    }).join(
        reg_agg[["trades", "gain_pct", "open", "high", "dollar_vol"]].rename(columns={
            "trades": "reg_trades", "gain_pct": "reg_gain", "open": "reg_open",
            "high": "reg_high", "dollar_vol": "reg_dvol",
        }),
        how="outer",
    ).fillna(0)

    merged["total_trades"] = merged["pm_trades"] + merged["reg_trades"]
    merged["total_dvol"] = merged["pm_dvol"] + merged["reg_dvol"]
    return merged


def fmt_dollar(val: float) -> str:
    """Format dollar volume as human-readable string."""
    if val >= 1e9:
        return f"${val / 1e9:.1f}B"
    if val >= 1e6:
        return f"${val / 1e6:.1f}M"
    if val >= 1e3:
        return f"${val / 1e3:.0f}K"
    return f"${val:.0f}"


def fmt_pct(val: float) -> str:
    if val == 0:
        return "    -"
    return f"{val:+.1f}%"


# ---------------------------------------------------------------------------
# Standard (non-session) output
# ---------------------------------------------------------------------------

SORT_LABELS = {
    "dollar_vol": "Dollar Volume",
    "trades": "Trade Count",
    "volume": "Share Volume",
}


def print_table(agg: pd.DataFrame, top_n: int, date_str: str,
                sort_by: str, prev_symbols: set[str] | None) -> None:
    print(f"\nEx-Index Most Traded Stocks — {date_str}  (sorted by {SORT_LABELS[sort_by]})")
    print("=" * 100)
    header = (f"{'Rank':>4}  {'Symbol':<8} {'Trades':>10} {'Volume':>14} "
              f"{'Dollar Vol':>12} {'VWAP':>9} {'Low':>9} {'High':>9} {'Exch':>4}")
    if prev_symbols is not None:
        header += "  New"
    print(header)
    print("-" * 100)

    for i, (sym, row) in enumerate(agg.head(top_n).iterrows(), 1):
        line = (f"{i:>4}  {sym:<8} {row['trades']:>10,.0f} {row['volume']:>14,.0f} "
                f"{fmt_dollar(row['dollar_vol']):>12} {row['vwap']:>9.2f} "
                f"{row['low']:>9.2f} {row['high']:>9.2f} {row['exchanges']:>4.0f}")
        if prev_symbols is not None:
            line += "    *" if sym not in prev_symbols else ""
        print(line)

    print("-" * 100)
    print(f"Total: {len(agg):,} symbols | {agg['trades'].sum():,.0f} trades | "
          f"{agg['volume'].sum():,.0f} shares | {fmt_dollar(agg['dollar_vol'].sum())} dollar volume")


# ---------------------------------------------------------------------------
# Session output
# ---------------------------------------------------------------------------

SESSION_SORT_LABELS = {
    "pm_gain": "PM Gain%",
    "reg_gain": "Regular Gain%",
    "pm_trades": "PM Trades",
    "reg_trades": "Regular Trades",
    "total_dvol": "Total Dollar Volume",
}


def print_session_table(merged: pd.DataFrame, top_n: int, date_str: str,
                        sort_by: str) -> None:
    print(f"\nEx-Index Session Analysis — {date_str}  (sorted by {SESSION_SORT_LABELS[sort_by]})")
    print(f"PM = prev post-market + today pre-market | Regular = 9:30 AM–4:00 PM ET")
    print("=" * 120)
    header = (f"{'Rank':>4}  {'Symbol':<8}"
              f" {'PM Trd':>8} {'PM Open':>9} {'PM High':>9} {'PM Gain':>8}"
              f" {'Reg Trd':>8} {'Reg Open':>9} {'Reg High':>9} {'Reg Gain':>8}"
              f" {'Total $Vol':>12}")
    print(header)
    print("-" * 120)

    for i, (sym, r) in enumerate(merged.head(top_n).iterrows(), 1):
        pm_open = f"{r['pm_open']:>9.2f}" if r['pm_trades'] > 0 else "        -"
        pm_high = f"{r['pm_high']:>9.2f}" if r['pm_trades'] > 0 else "        -"
        reg_open = f"{r['reg_open']:>9.2f}" if r['reg_trades'] > 0 else "        -"
        reg_high = f"{r['reg_high']:>9.2f}" if r['reg_trades'] > 0 else "        -"
        print(f"{i:>4}  {sym:<8}"
              f" {r['pm_trades']:>8,.0f} {pm_open} {pm_high} {fmt_pct(r['pm_gain']):>8}"
              f" {r['reg_trades']:>8,.0f} {reg_open} {reg_high} {fmt_pct(r['reg_gain']):>8}"
              f" {fmt_dollar(r['total_dvol']):>12}")

    print("-" * 120)
    print(f"Total: {len(merged):,} symbols | "
          f"PM {merged['pm_trades'].sum():,.0f} trades | "
          f"Regular {merged['reg_trades'].sum():,.0f} trades | "
          f"{fmt_dollar(merged['total_dvol'].sum())} dollar volume")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze most traded ex-index stocks")
    parser.add_argument("--date", help="Date to analyze (YYYY-MM-DD). Default: latest available.")
    parser.add_argument("--top", type=int, default=50, help="Number of top stocks to show (default 50)")
    parser.add_argument("--session", action="store_true",
                        help="Show PM vs regular session breakdown with gain%%")
    parser.add_argument("--sort", default=None,
                        help="Sort metric. Standard: dollar_vol, trades, volume. "
                             "Session: pm_gain, reg_gain, pm_trades, reg_trades, total_dvol")
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

    if args.session:
        sort_by = args.sort or "pm_gain"
        if sort_by not in SESSION_SORT_LABELS:
            print(f"Invalid --sort for session mode. Choose from: {', '.join(SESSION_SORT_LABELS)}", file=sys.stderr)
            sys.exit(1)
        merged = load_session_data(path, date_str)
        merged.sort_values(sort_by, ascending=False, inplace=True)
        print_session_table(merged, args.top, date_str, sort_by)
    else:
        sort_by = args.sort or "dollar_vol"
        if sort_by not in SORT_LABELS:
            print(f"Invalid --sort for standard mode. Choose from: {', '.join(SORT_LABELS)}", file=sys.stderr)
            sys.exit(1)
        agg = load_and_aggregate(path)
        agg.sort_values(sort_by, ascending=False, inplace=True)

        prev_symbols: set[str] | None = None
        idx = dates.index(date_str) if date_str in dates else -1
        if idx > 0:
            prev_date = dates[idx - 1]
            prev_path = ex_dir / f"{prev_date}.parquet"
            prev_agg = load_and_aggregate(prev_path)
            prev_agg.sort_values(sort_by, ascending=False, inplace=True)
            prev_symbols = set(prev_agg.head(args.top).index)

        print_table(agg, args.top, date_str, sort_by, prev_symbols)


if __name__ == "__main__":
    main()
