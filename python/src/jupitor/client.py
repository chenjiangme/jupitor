"""HTTP client for communicating with jupitor-server."""

from __future__ import annotations

import requests


class JupitorClient:
    """Client for the jupitor-server REST API."""

    def __init__(self, base_url: str = "http://localhost:8080"):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def get_bars(self, symbol: str, market: str = "us", start: str | None = None, end: str | None = None) -> dict:
        """Fetch daily bars for a symbol."""
        # TODO: implement GET /api/v1/bars
        raise NotImplementedError("get_bars not implemented")

    def get_positions(self) -> list[dict]:
        """Fetch current positions."""
        # TODO: implement GET /api/v1/positions
        raise NotImplementedError("get_positions not implemented")

    def get_account(self) -> dict:
        """Fetch account information."""
        # TODO: implement GET /api/v1/account
        raise NotImplementedError("get_account not implemented")

    def submit_order(self, symbol: str, side: str, qty: float, order_type: str = "market") -> dict:
        """Submit a new order."""
        # TODO: implement POST /api/v1/orders
        raise NotImplementedError("submit_order not implemented")
