"""Tests for sync_tracked_tickers."""

import pytest

from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource
from app.market.tracking import sync_tracked_tickers


@pytest.mark.asyncio
class TestSyncTrackedTickers:
    async def test_adds_and_removes(self):
        cache = PriceCache()
        source = SimulatorDataSource(cache, update_interval=60, seed=1)
        await source.start(["AAPL", "GOOGL"])
        try:
            added, removed = await sync_tracked_tickers(source, ["aapl", "NVDA"])

            assert added == ["NVDA"]
            assert removed == ["GOOGL"]
            assert set(source.get_tickers()) == {"AAPL", "NVDA"}
            assert "NVDA" in cache and "GOOGL" not in cache
        finally:
            await source.stop()

    async def test_noop_when_in_sync(self):
        source = SimulatorDataSource(PriceCache(), update_interval=60, seed=1)
        await source.start(["AAPL"])
        try:
            assert await sync_tracked_tickers(source, {"AAPL"}) == ([], [])
        finally:
            await source.stop()

    async def test_watchlist_union_positions_keeps_held_ticker(self):
        """Removing NVDA from the watchlist while holding it keeps it tracked."""
        source = SimulatorDataSource(PriceCache(), update_interval=60, seed=1)
        await source.start(["AAPL", "NVDA"])
        try:
            watchlist = {"AAPL"}
            positions = {"NVDA"}
            await sync_tracked_tickers(source, watchlist | positions)
            assert set(source.get_tickers()) == {"AAPL", "NVDA"}
        finally:
            await source.stop()

    async def test_invalid_ticker_raises(self):
        source = SimulatorDataSource(PriceCache(), update_interval=60)
        with pytest.raises(ValueError):
            await sync_tracked_tickers(source, ["$$$"])
