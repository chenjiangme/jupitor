"""Command-line interface for jupitor Python tools."""

import typer

app = typer.Typer(name="jupitor", help="Jupitor financial platform CLI")


@app.command()
def version():
    """Print the jupitor version."""
    from jupitor import __version__
    typer.echo(f"jupitor {__version__}")


@app.command()
def bars(
    symbol: str = typer.Argument(..., help="Stock symbol"),
    market: str = typer.Option("us", help="Market: us or cn"),
    days: int = typer.Option(30, help="Number of days"),
):
    """Display recent daily bars for a symbol."""
    # TODO: Read from Parquet store and display
    typer.echo(f"TODO: show {days} days of bars for {symbol} ({market})")


@app.command()
def analyze(
    symbol: str = typer.Argument(..., help="Stock symbol"),
    market: str = typer.Option("us", help="Market: us or cn"),
):
    """Run basic analysis on a symbol."""
    # TODO: Load data, compute returns, drawdown, etc.
    typer.echo(f"TODO: analyze {symbol} ({market})")


if __name__ == "__main__":
    app()
