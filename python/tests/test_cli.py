"""Tests for jupitor.cli module."""

from typer.testing import CliRunner

from jupitor.cli import app

runner = CliRunner()


def test_version():
    """Test version command outputs version string."""
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "jupitor" in result.output


def test_bars_todo():
    """Test bars command outputs TODO placeholder."""
    result = runner.invoke(app, ["bars", "AAPL"])
    assert result.exit_code == 0
    assert "TODO" in result.output
