"""Tests for jupitor.config module."""

from jupitor.config import get_data_dir, load_config


def test_get_data_dir_default(monkeypatch):
    """Test default data directory when env var is not set."""
    monkeypatch.delenv("DATA_1", raising=False)
    data_dir = get_data_dir()
    assert str(data_dir) == "data"


def test_get_data_dir_from_env(monkeypatch):
    """Test data directory from environment variable."""
    monkeypatch.setenv("DATA_1", "/mnt/data1")
    data_dir = get_data_dir()
    assert str(data_dir) == "/mnt/data1"


def test_load_config_missing_file():
    """Test loading config from nonexistent file returns empty dict."""
    result = load_config("/nonexistent/path.yaml")
    assert result == {}
