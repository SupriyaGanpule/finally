"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

from massive import RESTClient
from massive.exceptions import AuthError, BadResponse
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource
from .models import normalize_ticker

logger = logging.getLogger(__name__)


# --- Snapshot parsing (pure, unit-testable) ---


@dataclass(frozen=True, slots=True)
class ParsedQuote:
    """The fields FinAlly needs from one Massive TickerSnapshot."""

    ticker: str
    price: float
    timestamp: float  # Unix seconds
    session_open: float | None  # previous close: baseline for day change


def to_unix_seconds(ts: float | int | None) -> float | None:
    """Convert a Massive timestamp to Unix seconds.

    Massive mixes units: trade and `updated` timestamps are nanoseconds,
    aggregate bars are milliseconds. Detect the unit by magnitude.
    """
    if isinstance(ts, bool) or not isinstance(ts, (int, float)) or ts <= 0:
        return None
    ts = float(ts)
    if ts > 1e17:  # nanoseconds
        return ts / 1e9
    if ts > 1e14:  # microseconds
        return ts / 1e6
    if ts > 1e11:  # milliseconds
        return ts / 1e3
    return ts  # already seconds


def _first_positive(*values: Any) -> float | None:
    for value in values:
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
            return float(value)
    return None


def parse_snapshot(snap: Any) -> ParsedQuote | None:
    """Extract (ticker, price, timestamp, baseline) from a TickerSnapshot.

    Every TickerSnapshot field is Optional, so each lookup is defensive.
    Note: the SDK's LastTrade has no `timestamp` attribute; the trade time is
    `sip_timestamp` (nanoseconds).

    Price fallback order: last_trade.price → min.close → day.close → prev_day.close
    Returns None if the snapshot has no ticker or no usable price.
    """
    raw_ticker = getattr(snap, "ticker", None)
    if not raw_ticker or not isinstance(raw_ticker, str):
        return None

    last_trade = getattr(snap, "last_trade", None)
    minute = getattr(snap, "min", None)
    day = getattr(snap, "day", None)
    prev_day = getattr(snap, "prev_day", None)

    price = _first_positive(
        getattr(last_trade, "price", None),
        getattr(minute, "close", None),
        getattr(day, "close", None),
        getattr(prev_day, "close", None),
    )
    if price is None:
        return None

    timestamp = (
        to_unix_seconds(getattr(last_trade, "sip_timestamp", None))
        or to_unix_seconds(getattr(snap, "updated", None))
        or time.time()
    )
    session_open = _first_positive(
        getattr(prev_day, "close", None),
        getattr(day, "open", None),
    )
    return ParsedQuote(raw_ticker.strip().upper(), price, timestamp, session_open)


# --- Data source ---


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all tracked
    tickers in one call and writes the results into the PriceCache.

    Scheduling:
      * Normal cadence: every `poll_interval` seconds (15s = free-tier safe).
      * On failure: exponential backoff, capped at `max_backoff`.
      * add_ticker() wakes the loop early so a new ticker gets a price fast,
        but never sooner than `min_poll_gap` after the previous call
        (12s = the free tier's 5 requests/minute).
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
        min_poll_gap: float = 12.0,
        max_backoff: float = 120.0,
        client: RESTClient | None = None,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._min_gap = min(min_poll_gap, poll_interval)
        self._max_backoff = max(max_backoff, poll_interval)
        self._client = client  # injectable for tests
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._last_poll = float("-inf")  # loop.time() of the last API call
        self._auth_failed = False

    @property
    def name(self) -> str:
        return "massive"

    # --- Lifecycle ---

    async def start(self, tickers: list[str]) -> None:
        if self._task is not None:
            raise RuntimeError("MassiveDataSource already started")
        if self._client is None:
            # Short timeouts + one retry: a dead network must not stall startup
            self._client = RESTClient(
                api_key=self._api_key,
                connect_timeout=5.0,
                read_timeout=5.0,
                retries=1,
            )
        for ticker in tickers:
            await self.add_ticker(ticker)

        # Immediate first poll so the cache has data right away (failures tolerated)
        await self._poll_once()
        self._wake.clear()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(self._tickers),
            self._interval,
        )

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("Massive poller stopped")

    # --- Tracked set ---

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            self._wake.set()  # ask the loop for an early poll
            logger.info("Massive: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Polling ---

    async def _poll_loop(self) -> None:
        """Poll on the interval, on wake-ups, and with backoff after failures."""
        loop = asyncio.get_running_loop()
        delay = self._interval
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=delay)
            except TimeoutError:
                pass
            self._wake.clear()

            # Enforce the rate-limit floor even when woken early
            gap = self._min_gap - (loop.time() - self._last_poll)
            if gap > 0:
                await asyncio.sleep(gap)

            ok = await self._poll_once()
            delay = self._interval if ok else min(delay * 2, self._max_backoff)

    async def _poll_once(self) -> bool:
        """One API call → cache writes. Returns False on failure (for backoff)."""
        tickers = list(self._tickers)  # stable copy for the worker thread
        if not tickers or self._client is None:
            return True

        self._last_poll = asyncio.get_running_loop().time()
        try:
            # The Massive RESTClient is synchronous: run it in a worker thread
            snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except (AuthError, BadResponse) as e:
            msg = str(e)
            if isinstance(e, AuthError) or "401" in msg or "403" in msg or "NOT_AUTHORIZED" in msg:
                if not self._auth_failed:  # log loudly once, not every poll
                    logger.error("Massive auth/plan error — check MASSIVE_API_KEY: %s", msg[:200])
                self._auth_failed = True
            else:
                logger.warning("Massive bad response (backing off): %s", msg[:200])
            return False
        except Exception as e:  # network errors, timeouts, bad JSON, ...
            logger.warning("Massive poll failed (backing off): %s", e)
            return False

        self._auth_failed = False
        tracked = set(self._tickers)  # re-read AFTER the await: guards the remove race
        written = 0
        returned: set[str] = set()
        for snap in snapshots or []:
            quote = parse_snapshot(snap)
            if quote is None:
                logger.debug("Skipping unusable snapshot for %s", getattr(snap, "ticker", "?"))
                continue
            returned.add(quote.ticker)
            if quote.ticker not in tracked:
                continue
            self._cache.update(
                ticker=quote.ticker,
                price=quote.price,
                timestamp=quote.timestamp,
                session_open=quote.session_open,
            )
            written += 1

        missing = tracked.intersection(tickers) - returned
        if missing:
            logger.debug("Massive returned no data for: %s", sorted(missing))
        logger.debug("Massive poll: updated %d/%d tickers", written, len(tickers))
        return True

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Blocking call to the Massive REST API. Runs in a worker thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=tickers,
        )
