"""Financial analysis utilities."""

from __future__ import annotations

import numpy as np
import pandas as pd


def daily_returns(prices: pd.Series) -> pd.Series:
    """Calculate daily returns from a price series."""
    return prices.pct_change().dropna()


def cumulative_returns(returns: pd.Series) -> pd.Series:
    """Calculate cumulative returns from a returns series."""
    return (1 + returns).cumprod() - 1


def max_drawdown(prices: pd.Series) -> float:
    """Calculate maximum drawdown from a price series."""
    peak = prices.expanding().max()
    drawdown = (prices - peak) / peak
    return float(drawdown.min())


def sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.0, periods: int = 252) -> float:
    """Calculate annualized Sharpe ratio.

    Args:
        returns: Daily returns series.
        risk_free_rate: Annual risk-free rate.
        periods: Trading periods per year (252 for daily).

    Returns:
        Annualized Sharpe ratio.
    """
    if returns.empty or returns.std() == 0:
        return 0.0

    excess = returns - risk_free_rate / periods
    return float(np.sqrt(periods) * excess.mean() / excess.std())


def volatility(returns: pd.Series, periods: int = 252) -> float:
    """Calculate annualized volatility."""
    if returns.empty:
        return 0.0
    return float(returns.std() * np.sqrt(periods))
