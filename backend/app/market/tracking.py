"""Keep the data source's tracked set equal to the desired set."""

from __future__ import annotations

from collections.abc import Iterable

from .interface import MarketDataSource
from .models import normalize_ticker


async def sync_tracked_tickers(
    source: MarketDataSource, desired: Iterable[str]
) -> tuple[list[str], list[str]]:
    """Add missing / remove extra tickers so the source tracks exactly `desired`.

    `desired` should be watchlist ∪ tickers with an open position, so that
    portfolio valuation always has a live price. Returns (added, removed).
    """
    want = {normalize_ticker(t) for t in desired}
    have = set(source.get_tickers())
    added = sorted(want - have)
    removed = sorted(have - want)
    for ticker in added:
        await source.add_ticker(ticker)
    for ticker in removed:
        await source.remove_ticker(ticker)
    return added, removed
