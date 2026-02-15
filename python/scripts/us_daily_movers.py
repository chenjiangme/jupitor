#!/usr/bin/env python3
"""Show top movers by tier from daily summaries and trade-universe CSVs.

Usage:
    python python/scripts/us_daily_movers.py [--days 20] [--top 5]
"""

import argparse
import csv
import os
import sys

import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(description="Top movers by tier")
    parser.add_argument("--days", type=int, default=20, help="Number of trading days (default: 20)")
    parser.add_argument("--top", type=int, default=5, help="Top N per tier per day (default: 5)")
    args = parser.parse_args()

    data_dir = os.environ.get("DATA_1")
    if not data_dir:
        print("ERROR: DATA_1 environment variable not set", file=sys.stderr)
        sys.exit(1)

    tu_dir = os.path.join(data_dir, "us", "trade-universe")
    daily_dir = os.path.join(data_dir, "us", "stock-trades-daily")

    dates = sorted(f[:-4] for f in os.listdir(tu_dir) if f.endswith(".csv"))
    dates = dates[-args.days:]

    tier_configs = [
        ("ACTIVE", 10),
        ("MODERATE", 10),
        ("SPORADIC", 20),
    ]

    # Header
    parts = [f"| {'Date':<10} | #"]
    for tier, thresh in tier_configs:
        parts.append(f"| {tier:<8} | {'Trades':>7} | {'O→H%':>6}")
    print(" ".join(parts) + " |")

    sep_parts = [f"|{'':->12}|---"]
    for _ in tier_configs:
        sep_parts.append(f"|{'':->10}|{'':->9}|{'':->8}")
    print("".join(sep_parts) + "|")

    for date in dates:
        # Read tier assignments from trade-universe CSV.
        tiers_map: dict[str, set[str]] = {}
        csv_path = os.path.join(tu_dir, date + ".csv")
        with open(csv_path) as f:
            for row in csv.DictReader(f):
                tier = row.get("tier", "")
                if tier in ("ACTIVE", "MODERATE", "SPORADIC"):
                    tiers_map.setdefault(tier, set()).add(row["symbol"])

        # Read daily summary.
        daily_path = os.path.join(daily_dir, date + ".parquet")
        if not os.path.exists(daily_path):
            continue
        tbl = pq.read_table(daily_path)
        df = tbl.to_pandas()
        df = df[df["open"] > 0].copy()
        df["o2h"] = (df["high"] - df["open"]) / df["open"] * 100

        # Compute top N for each tier.
        tops: dict[str, list] = {}
        for tier, thresh in tier_configs:
            syms = tiers_map.get(tier, set())
            t = df[df["symbol"].isin(syms) & (df["o2h"] >= thresh)]
            tops[tier] = list(
                t.nlargest(args.top, "trades")[["symbol", "trades", "o2h"]].itertuples(index=False)
            )

        for i in range(args.top):
            d = date if i == 0 else " " * 10
            parts = [f"| {d:<10} | {i + 1}"]
            for tier, _ in tier_configs:
                row = tops[tier][i] if i < len(tops[tier]) else None
                sym = f"{row.symbol:<8}" if row else " " * 8
                tr = f"{row.trades:>7,}" if row else " " * 7
                oh = f"{row.o2h:>5.1f}%" if row else " " * 6
                parts.append(f"| {sym} | {tr} | {oh}")
            print(" ".join(parts) + " |")


if __name__ == "__main__":
    main()
