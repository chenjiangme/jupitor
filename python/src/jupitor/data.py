"""Parquet data reader for jupitor market data."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from jupitor.config import get_data_dir


def read_daily_bars(symbol: str, market: str = "us", year: int | None = None) -> pd.DataFrame:
    """Read daily bar data from Parquet files.

    Args:
        symbol: Stock symbol (e.g., "AAPL").
        market: Market identifier ("us" or "cn").
        year: Specific year to read. If None, reads all available years.

    Returns:
        DataFrame with columns: timestamp, open, high, low, close, volume, trade_count, vwap.
    """
    data_dir = get_data_dir()
    symbol_dir = data_dir / market / "daily" / symbol

    if not symbol_dir.exists():
        return pd.DataFrame()

    if year is not None:
        parquet_path = symbol_dir / f"{year}.parquet"
        if not parquet_path.exists():
            return pd.DataFrame()
        return pd.read_parquet(parquet_path)

    # Read all years
    frames = []
    for f in sorted(symbol_dir.glob("*.parquet")):
        frames.append(pd.read_parquet(f))

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def list_symbols(market: str = "us") -> list[str]:
    """List all symbols with available data.

    Args:
        market: Market identifier ("us" or "cn").

    Returns:
        Sorted list of symbol strings.
    """
    data_dir = get_data_dir()
    market_dir = data_dir / market / "daily"

    if not market_dir.exists():
        return []

    return sorted(d.name for d in market_dir.iterdir() if d.is_dir())
