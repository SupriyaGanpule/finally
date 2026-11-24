"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .models import normalize_ticker
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)

# 252 trading days * 6.5 hours/day * 3600 seconds/hour
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)

    Where Z = L @ z, z ~ N(0, I), and L is the Cholesky factor of a
    sector-based correlation matrix. The tiny dt (~8.5e-8 for 500ms ticks)
    produces sub-cent-to-cents moves per tick that accumulate naturally.

    Pure and synchronous: no I/O, no asyncio. Inject `rng` for determinism.
    """

    TRADING_SECONDS_PER_YEAR = TRADING_SECONDS_PER_YEAR
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str] | None = None,
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
        rng: np.random.Generator | None = None,
    ) -> None:
        self._dt = dt
        self._sqrt_dt = math.sqrt(dt)
        self._event_prob = event_probability
        self._rng = rng if rng is not None else np.random.default_rng()

        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        self.add_tickers(tickers or [])

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: rounded price}.

        Hot path (called every 500ms) — keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z = self._rng.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z

        shocks = self._rng.random(n) < self._event_prob
        result: dict[str, float] = {}

        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu, sigma = params["mu"], params["sigma"]
            drift = (mu - 0.5 * sigma * sigma) * self._dt
            diffusion = sigma * self._sqrt_dt * z[i]
            price = self._prices[ticker] * math.exp(drift + diffusion)

            # Random event: a sudden 2-5% move for drama
            if shocks[i]:
                magnitude = float(self._rng.uniform(0.02, 0.05))
                sign = 1.0 if self._rng.random() < 0.5 else -1.0
                price *= 1.0 + sign * magnitude
                logger.debug("Random event on %s: %+.1f%%", ticker, sign * magnitude * 100)

            self._prices[ticker] = price
            result[ticker] = round(price, 2)

        return result

    def add_tickers(self, tickers: list[str]) -> list[str]:
        """Batch add with a single Cholesky rebuild. Returns tickers actually added."""
        added: list[str] = []
        for raw in tickers:
            ticker = normalize_ticker(raw)
            if ticker in self._prices:
                continue
            self._tickers.append(ticker)
            seed = SEED_PRICES.get(ticker)
            self._prices[ticker] = seed if seed else float(self._rng.uniform(50.0, 300.0))
            self._params[ticker] = dict(TICKER_PARAMS.get(ticker, DEFAULT_PARAMS))
            added.append(ticker)
        if added:
            self._rebuild_cholesky()
        return added

    def add_ticker(self, ticker: str) -> bool:
        """Add one ticker. Returns True if it was newly added."""
        return bool(self.add_tickers([ticker]))

    def remove_ticker(self, ticker: str) -> bool:
        """Remove a ticker. Returns True if it was tracked."""
        ticker = normalize_ticker(ticker)
        if ticker not in self._prices:
            return False
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()
        return True

    def get_price(self, ticker: str) -> float | None:
        """Current price (rounded to cents), or None if not tracked."""
        price = self._prices.get(ticker)
        return round(price, 2) if price is not None else None

    def get_tickers(self) -> list[str]:
        """Currently tracked tickers, in insertion order."""
        return list(self._tickers)

    # --- Internals ---

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky factor of the correlation matrix. n < 50, so µs."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Sector-based correlation.

        - TSLA with anything:  0.3 (it does its own thing)
        - Both tech:           0.6
        - Both finance:        0.5
        - Cross-sector/unknown 0.3
        """
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    A background asyncio task steps the simulator every `update_interval`
    seconds and writes the results into the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
        seed: int | None = None,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        # Built eagerly so add_ticker() works before start()
        self._sim = GBMSimulator(
            event_probability=event_probability,
            rng=np.random.default_rng(seed),
        )
        self._task: asyncio.Task | None = None

    @property
    def name(self) -> str:
        return "simulator"

    async def start(self, tickers: list[str]) -> None:
        if self._task is not None:
            raise RuntimeError("SimulatorDataSource already started")
        self._sim.add_tickers(tickers)
        # Seed the cache so the first SSE push / first trade has a price
        for ticker in self._sim.get_tickers():
            self._seed_cache(ticker)
        self._task = asyncio.create_task(self._run_loop(), name="market-simulator")
        logger.info("Simulator started with %d tickers", len(self._sim.get_tickers()))

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim.add_ticker(ticker):
            if self._task is not None:
                self._seed_cache(ticker)  # price available immediately
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers()

    # --- Internals ---

    def _seed_cache(self, ticker: str) -> None:
        price = self._sim.get_price(ticker)
        if price is not None:
            # The seed price is the "session open" for day-change purposes
            self._cache.update(ticker=ticker, price=price, session_open=price)

    async def _run_loop(self) -> None:
        """Step, write to cache, sleep — on a fixed-rate schedule."""
        loop = asyncio.get_running_loop()
        next_tick = loop.time()
        while True:
            try:
                for ticker, price in self._sim.step().items():
                    self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            next_tick += self._interval
            now = loop.time()
            if next_tick < now:  # fell behind (e.g. event loop stall): don't burst
                next_tick = now
            await asyncio.sleep(next_tick - now)
