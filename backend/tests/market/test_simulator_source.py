"""Integration tests for SimulatorDataSource."""

import asyncio

import pytest

from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource


@pytest.mark.asyncio
class TestSimulatorDataSource:
    """Integration tests for the SimulatorDataSource."""

    async def test_start_populates_cache(self):
        """Test that start() immediately populates the cache."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "GOOGL"])

        # Cache should have seed prices immediately (before first loop tick)
        assert cache.get("AAPL") is not None
        assert cache.get("GOOGL") is not None

        await source.stop()

    async def test_prices_update_over_time(self):
        """Test that prices are updated periodically."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.05)
        await source.start(["AAPL"])

        initial_version = cache.version
        await asyncio.sleep(0.3)  # Several update cycles

        # Version should have incremented (prices updated)
        assert cache.version > initial_version

        await source.stop()

    async def test_stop_is_clean(self):
        """Test that stop() is clean and idempotent."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])
        await source.stop()
        # Double stop should not raise
        await source.stop()

    async def test_add_ticker(self):
        """Test adding a ticker dynamically."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])

        await source.add_ticker("TSLA")
        assert "TSLA" in source.get_tickers()
        assert cache.get("TSLA") is not None

        await source.stop()

    async def test_remove_ticker(self):
        """Test removing a ticker."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "TSLA"])

        await source.remove_ticker("TSLA")
        assert "TSLA" not in source.get_tickers()
        assert cache.get("TSLA") is None

        await source.stop()

    async def test_get_tickers(self):
        """Test getting the list of active tickers."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "GOOGL"])

        tickers = source.get_tickers()
        assert set(tickers) == {"AAPL", "GOOGL"}

        await source.stop()

    async def test_empty_start(self):
        """Test starting with no tickers."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start([])

        assert len(cache) == 0
        assert source.get_tickers() == []

        await source.stop()

    async def test_exception_resilience(self):
        """Test that simulator continues running after errors."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.05)

        # Start with a valid ticker
        await source.start(["AAPL"])

        # Wait for some updates
        await asyncio.sleep(0.15)

        # Task should still be running
        assert source._task is not None
        assert not source._task.done()

        await source.stop()

    async def test_custom_update_interval(self):
        """Test using a custom update interval."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
        await source.start(["AAPL"])

        initial_version = cache.version
        await asyncio.sleep(0.05)  # Should get ~5 updates

        # Should have multiple updates with fast interval
        assert cache.version > initial_version + 2

        await source.stop()

    async def test_custom_event_probability(self):
        """Test creating source with custom event probability."""
        cache = PriceCache()
        # Very high event probability for testing
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1, event_probability=1.0)
        await source.start(["AAPL"])

        # Just verify it starts and stops cleanly
        await asyncio.sleep(0.2)
        await source.stop()

    async def test_name(self):
        assert SimulatorDataSource(price_cache=PriceCache()).name == "simulator"

    async def test_start_twice_raises(self):
        source = SimulatorDataSource(price_cache=PriceCache(), update_interval=60)
        await source.start(["AAPL"])
        try:
            with pytest.raises(RuntimeError):
                await source.start(["AAPL"])
        finally:
            await source.stop()

    async def test_add_ticker_before_start(self):
        """add_ticker() before start() is remembered, not silently dropped."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=60)
        await source.add_ticker("PYPL")
        assert len(cache) == 0  # nothing written before start

        await source.start(["AAPL"])
        try:
            assert set(source.get_tickers()) == {"AAPL", "PYPL"}
            assert "PYPL" in cache
        finally:
            await source.stop()

    async def test_tickers_normalized(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=60)
        await source.start(["aapl"])
        try:
            await source.add_ticker(" tsla ")
            await source.remove_ticker("aapl")
            assert source.get_tickers() == ["TSLA"]
            assert "TSLA" in cache and "AAPL" not in cache
        finally:
            await source.stop()

    async def test_seed_price_is_session_open(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.01, seed=1)
        await source.start(["AAPL"])
        await asyncio.sleep(0.05)
        await source.stop()

        update = cache.get("AAPL")
        assert update.session_open == 190.0
        assert update.day_change == pytest.approx(update.price - 190.0, abs=1e-6)

    async def test_seeded_sources_are_reproducible(self):
        prices = []
        for _ in range(2):
            source = SimulatorDataSource(price_cache=PriceCache(), seed=123)
            source._sim.add_tickers(["AAPL", "NVDA"])
            prices.append([source._sim.step() for _ in range(20)])
        assert prices[0] == prices[1]

    async def test_removed_ticker_never_rewritten(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.005)
        await source.start(["AAPL", "TSLA"])
        await asyncio.sleep(0.02)
        await source.remove_ticker("TSLA")
        await asyncio.sleep(0.05)
        await source.stop()
        assert "TSLA" not in cache

    async def test_loop_survives_step_exception(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.005)
        await source.start(["AAPL"])
        original_step = source._sim.step
        calls = {"n": 0}

        def flaky_step():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom")
            return original_step()

        source._sim.step = flaky_step
        version = cache.version
        await asyncio.sleep(0.05)
        await source.stop()

        assert calls["n"] > 1
        assert cache.version > version
