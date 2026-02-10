"""Configuration loading for jupitor Python tools."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def get_data_dir() -> Path:
    """Return the data directory from DATA_1 env var or default."""
    data_dir = os.environ.get("DATA_1", "./data")
    return Path(data_dir)


def load_config(path: str | None = None) -> dict[str, Any]:
    """Load jupitor YAML configuration.

    Args:
        path: Path to config file. Defaults to config/jupitor.yaml.

    Returns:
        Configuration dictionary.
    """
    if path is None:
        path = os.environ.get("JUPITOR_CONFIG", "config/jupitor.yaml")

    config_path = Path(path)
    if not config_path.exists():
        return {}

    with open(config_path) as f:
        return yaml.safe_load(f) or {}
