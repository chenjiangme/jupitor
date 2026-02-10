"""Tests for jupitor.data module."""

from jupitor.data import list_symbols, read_daily_bars


def test_read_daily_bars_missing_symbol(tmp_path, monkeypatch):
    """Test reading bars for a symbol that doesn't exist."""
    monkeypatch.setenv("DATA_1", str(tmp_path))
    df = read_daily_bars("NONEXISTENT")
    assert df.empty


def test_list_symbols_empty(tmp_path, monkeypatch):
    """Test listing symbols when no data exists."""
    monkeypatch.setenv("DATA_1", str(tmp_path))
    symbols = list_symbols("us")
    assert symbols == []
