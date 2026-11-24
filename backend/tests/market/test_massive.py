"""Tests for MassiveDataSource (API mocked, snapshots built with the real SDK models).

Snapshots are built with massive's own TickerSnapshot.from_dict rather than
MagicMock: MagicMock invents any attribute you ask for, which previously hid a
bug where the client read a `last_trade.timestamp` field that does not exist.
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from massive.exceptions import AuthError, BadResponse
from massive.rest.models import TickerSnapshot

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource

TS_NS = 1_707_580_800_000_000_000  # 2024-02-10T16:00:00Z in nanoseconds


def make_snapshot(ticker: str, price: float, ts_ns: int = TS_NS, prev_close=None) -> TickerSnapshot:
    """A realistic snapshot as the SDK would decode it from the REST response."""
    data = {"ticker": ticker, "lastTrade": {"p": price, "s": 100, "t": ts_ns}}
    if prev_close is not None:
        data["prevDay"] = {"c": prev_close}
    return TickerSnapshot.from_dict(data)


def make_source(cache: PriceCache | None = None, **kwargs) -> MassiveDataSource:
    kwargs.setdefault("poll_interval", 60.0)  # long interval: loop never auto-polls
    return MassiveDataSource(
        api_key="test-key",
        price_cache=cache if cache is not None else PriceCache(),
        client=MagicMock(),
        **kwargs,
    )


@pytest.mark.asyncio
class TestPollOnce:
    """One poll cycle: API response → cache writes."""

    async def test_poll_updates_cache(self):
        cache = PriceCache()
        source = make_source(cache)
        source._tickers = ["AAPL", "GOOGL"]
        source._client.get_snapshot_all.return_value = [
            make_snapshot("AAPL", 190.50),
            make_snapshot("GOOGL", 175.25),
        ]

        assert await source._poll_once() is True

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("GOOGL") == 175.25

    async def test_calls_snapshot_endpoint_with_tracked_tickers(self):
        source = make_source()
        source._tickers = ["AAPL", "MSFT"]
        source._client.get_snapshot_all.return_value = []

        await source._poll_once()

        kwargs = source._client.get_snapshot_all.call_args.kwargs
        assert kwargs["tickers"] == ["AAPL", "MSFT"]
        assert str(kwargs["market_type"].value) == "stocks"

    async def test_timestamp_converted_from_nanoseconds(self):
        """Regression: real trade timestamps are `sip_timestamp` in ns."""
        cache = PriceCache()
        source = make_source(cache)
        source._tickers = ["AAPL"]
        source._client.get_snapshot_all.return_value = [make_snapshot("AAPL", 190.5)]

        await source._poll_once()

        assert cache.get("AAPL").timestamp == pytest.approx(1_707_580_800.0)

    async def test_previous_close_becomes_session_open(self):
        cache = PriceCache()
        source = make_source(cache)
        source._tickers = ["AAPL"]
        source._client.get_snapshot_all.return_value = [
            make_snapshot("AAPL", 191.0, prev_close=190.0)
        ]

        await source._poll_once()

        update = cache.get("AAPL")
        assert update.session_open == 190.0
        assert update.day_change_percent == pytest.approx(0.5263, abs=1e-4)

    async def test_previous_price_tracks_last_poll(self):
        cache = PriceCache()
        source = make_source(cache)
        source._tickers = ["AAPL"]

        source._client.get_snapshot_all.return_value = [make_snapshot("AAPL", 190.0)]
        await source._poll_once()
        source._client.get_snapshot_all.return_value = [make_snapshot("AAPL", 190.4)]
        await source._poll_once()

        update = cache.get("AAPL")
        assert update.previous_price == 190.0
        assert update.direction == "up"

    async def test_unusable_snapshot_skipped(self):
        cache = PriceCache()
        source = make_source(cache)
        source._tickers = ["AAPL", "BAD"]
        source._client.get_snapshot_all.return_value = [
            make_snapshot("AAPL", 190.50),
            TickerSnapshot.from_dict({"ticker": "BAD"}),  # no price anywhere
        ]

        assert await source._poll_once() is True

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("BAD") is None

    async def test_untracked_tickers_in_response_ignored(self):
        cache = PriceCache()
        source = make_source(cache)
        source._tickers = ["AAPL"]
        source._client.get_snapshot_all.return_value = [
            make_snapshot("AAPL", 190.0),
            make_snapshot("ZZZZ", 10.0),
        ]

        await source._poll_once()

        assert "ZZZZ" not in cache

    async def test_removed_ticker_not_rewritten_by_inflight_poll(self):
        """A ticker removed while the API call is in flight must not come back."""
        cache = PriceCache()
        source = make_source(cache)
        source._tickers = ["AAPL", "GOOGL"]

        def fetch(_tickers):
            source._tickers.remove("GOOGL")  # remove_ticker() lands mid-fetch
            return [make_snapshot("AAPL", 100.0), make_snapshot("GOOGL", 100.0)]

        with patch.object(source, "_fetch_snapshots", side_effect=fetch):
            await source._poll_once()

        assert "AAPL" in cache
        assert "GOOGL" not in cache

    async def test_generic_error_returns_false(self):
        cache = PriceCache()
        source = make_source(cache)
        source._tickers = ["AAPL"]
        source._client.get_snapshot_all.side_effect = Exception("network error")

        assert await source._poll_once() is False  # does not raise
        assert len(cache) == 0

    async def test_rate_limit_returns_false(self):
        source = make_source()
        source._tickers = ["AAPL"]
        source._client.get_snapshot_all.side_effect = BadResponse(
            '{"status":"ERROR","error":"You\'ve exceeded the maximum requests per minute"}'
        )

        assert await source._poll_once() is False
        assert source._auth_failed is False

    async def test_auth_error_flagged_and_logged_once(self, caplog):
        source = make_source()
        source._tickers = ["AAPL"]
        source._client.get_snapshot_all.side_effect = BadResponse(
            '{"status":"NOT_AUTHORIZED","request_id":"x","message":"401 Unknown API Key"}'
        )

        with caplog.at_level("ERROR", logger="app.market.massive_client"):
            assert await source._poll_once() is False
            assert await source._poll_once() is False

        assert source._auth_failed is True
        assert sum("auth" in r.message for r in caplog.records) == 1

    async def test_auth_error_exception_type(self):
        source = make_source()
        source._tickers = ["AAPL"]
        source._client.get_snapshot_all.side_effect = AuthError("missing key")

        assert await source._poll_once() is False
        assert source._auth_failed is True

    async def test_success_clears_auth_flag(self):
        source = make_source()
        source._tickers = ["AAPL"]
        source._auth_failed = True
        source._client.get_snapshot_all.return_value = [make_snapshot("AAPL", 1.0)]

        await source._poll_once()

        assert source._auth_failed is False

    async def test_empty_tickers_skips_poll(self):
        source = make_source()
        source._tickers = []

        assert await source._poll_once() is True
        source._client.get_snapshot_all.assert_not_called()

    async def test_no_client_skips_poll(self):
        source = MassiveDataSource(api_key="k", price_cache=PriceCache())
        source._tickers = ["AAPL"]

        with patch.object(source, "_fetch_snapshots") as fetch:
            assert await source._poll_once() is True
            fetch.assert_not_called()


@pytest.mark.asyncio
class TestTrackedSet:
    async def test_add_ticker(self):
        source = make_source()
        await source.add_ticker("AAPL")
        assert source.get_tickers() == ["AAPL"]

    async def test_add_ticker_normalizes(self):
        source = make_source()
        await source.add_ticker("  aapl ")
        assert source.get_tickers() == ["AAPL"]

    async def test_add_duplicate_is_noop(self):
        source = make_source()
        await source.add_ticker("AAPL")
        await source.add_ticker("aapl")
        assert source.get_tickers() == ["AAPL"]

    async def test_add_invalid_ticker_raises(self):
        source = make_source()
        with pytest.raises(ValueError):
            await source.add_ticker("$$$")

    async def test_add_ticker_wakes_poller(self):
        source = make_source()
        await source.add_ticker("PYPL")
        assert source._wake.is_set()

    async def test_remove_ticker_evicts_cache(self):
        cache = PriceCache()
        source = make_source(cache)
        source._tickers = ["AAPL", "GOOGL"]
        cache.update("AAPL", 190.00)

        await source.remove_ticker("aapl")

        assert source.get_tickers() == ["GOOGL"]
        assert cache.get("AAPL") is None

    async def test_get_tickers_returns_copy(self):
        source = make_source()
        source._tickers = ["AAPL"]
        source.get_tickers().append("X")
        assert source.get_tickers() == ["AAPL"]

    async def test_name(self):
        assert make_source().name == "massive"


@pytest.mark.asyncio
class TestLifecycle:
    async def test_start_does_immediate_poll(self):
        cache = PriceCache()
        source = make_source(cache)
        source._client.get_snapshot_all.return_value = [make_snapshot("AAPL", 190.50)]

        await source.start(["aapl"])
        try:
            assert cache.get_price("AAPL") == 190.50
            assert source.get_tickers() == ["AAPL"]
        finally:
            await source.stop()

    async def test_start_survives_failed_first_poll(self):
        source = make_source()
        source._client.get_snapshot_all.side_effect = Exception("network down")

        await source.start(["AAPL"])
        try:
            assert source._task is not None and not source._task.done()
        finally:
            await source.stop()

    async def test_start_creates_client_with_short_timeouts(self):
        source = MassiveDataSource(api_key="test-key", price_cache=PriceCache(), poll_interval=60)
        with patch("app.market.massive_client.RESTClient") as rest_client:
            rest_client.return_value.get_snapshot_all.return_value = []
            await source.start(["AAPL"])
        try:
            kwargs = rest_client.call_args.kwargs
            assert kwargs["api_key"] == "test-key"
            assert kwargs["read_timeout"] <= 5.0
        finally:
            await source.stop()

    async def test_start_twice_raises(self):
        source = make_source()
        source._client.get_snapshot_all.return_value = []
        await source.start([])
        try:
            with pytest.raises(RuntimeError):
                await source.start([])
        finally:
            await source.stop()

    async def test_stop_cancels_task(self):
        source = make_source()
        source._client.get_snapshot_all.return_value = []
        await source.start(["AAPL"])
        task = source._task

        await source.stop()

        assert source._task is None
        assert task.cancelled() or task.done()

    async def test_stop_is_idempotent(self):
        source = make_source()
        await source.stop()
        await source.stop()


@pytest.mark.asyncio
class TestPollLoopScheduling:
    """Timing behaviour of the background loop, with tiny intervals."""

    async def test_polls_on_interval(self):
        source = make_source(poll_interval=0.02, min_poll_gap=0.0)
        source._client.get_snapshot_all.return_value = []
        source._tickers = ["AAPL"]
        source._task = asyncio.create_task(source._poll_loop())

        await asyncio.sleep(0.15)
        await source.stop()

        assert source._client.get_snapshot_all.call_count >= 3

    async def test_backs_off_after_failures(self):
        source = make_source(poll_interval=0.02, min_poll_gap=0.0, max_backoff=10.0)
        source._client.get_snapshot_all.side_effect = Exception("down")
        source._tickers = ["AAPL"]
        source._task = asyncio.create_task(source._poll_loop())

        # Delays: 0.02 (call 1) → 0.04 (call 2) → 0.08 (call 3) → 0.16 ...
        await asyncio.sleep(0.2)
        await source.stop()

        assert 2 <= source._client.get_snapshot_all.call_count <= 4

    async def test_wake_triggers_early_poll(self):
        source = make_source(poll_interval=60.0, min_poll_gap=0.0)
        source._client.get_snapshot_all.return_value = [make_snapshot("PYPL", 60.0)]
        source._task = asyncio.create_task(source._poll_loop())

        await source.add_ticker("PYPL")
        await asyncio.sleep(0.05)
        await source.stop()

        assert source._client.get_snapshot_all.call_count == 1
        assert "PYPL" in source._cache

    async def test_wake_respects_min_poll_gap(self):
        source = make_source(poll_interval=60.0, min_poll_gap=60.0)
        source._client.get_snapshot_all.return_value = []
        source._tickers = ["AAPL"]
        source._last_poll = asyncio.get_running_loop().time()  # just polled
        source._task = asyncio.create_task(source._poll_loop())

        await source.add_ticker("PYPL")
        await asyncio.sleep(0.05)
        await source.stop()

        source._client.get_snapshot_all.assert_not_called()
