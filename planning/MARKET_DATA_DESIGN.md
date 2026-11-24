# Market Data Backend: Detailed Design (v2)

**Status:** Implemented in `backend/app/market/` (191 tests, 99% coverage). It supersedes `planning/archive/MARKET_DATA_DESIGN.md`. Implementation notes: `GBMSimulator._pairwise_correlation` stays private (v1 name), and `_fetch_snapshots` takes the ticker list as an argument.
**Scope:** All market data functionality in `backend/app/market/`: the unified API (models, cache, interface, factory), the GBM simulator, the Massive (Polygon.io) client, SSE streaming, FastAPI wiring, and watchlist/position coordination.
**Inputs:** `PLAN.md` §6 and §10, `MARKET_DATA_SUMMARY.md`, and everything in `planning/archive/`. The design was also checked against the installed `massive` SDK and the current code in `backend/app/market/`.

> **Why a v2?** The v1 subsystem works and its 73 tests pass. Checking it against the real `massive` SDK and against PLAN.md turned up gaps that v1 does not cover. §0 lists them. Everything else keeps the v1 architecture, so downstream code (`from app.market import ...`) keeps working.

---

## Table of Contents

0. [What Changes From v1 (and Why)](#0-what-changes-from-v1-and-why)
1. [Architecture](#1-architecture)
2. [File Layout & Public API](#2-file-layout--public-api)
3. [Data Model: `models.py`](#3-data-model-modelspy)
4. [Price Cache: `cache.py`](#4-price-cache-cachepy)
5. [Unified Interface: `interface.py`](#5-unified-interface-interfacepy)
6. [Seed Data: `seed_prices.py`](#6-seed-data-seed_pricespy)
7. [GBM Simulator: `simulator.py`](#7-gbm-simulator-simulatorpy)
8. [Massive API Client: `massive_client.py`](#8-massive-api-client-massive_clientpy)
9. [Factory & Configuration: `factory.py`](#9-factory--configuration-factorypy)
10. [Ticker Tracking Sync: `tracking.py`](#10-ticker-tracking-sync-trackingpy)
11. [SSE Streaming: `stream.py`](#11-sse-streaming-streampy)
12. [FastAPI Integration](#12-fastapi-integration)
13. [Frontend Contract](#13-frontend-contract)
14. [Testing Plan](#14-testing-plan)
15. [Edge Cases & Failure Modes](#15-edge-cases--failure-modes)
16. [Configuration Reference](#16-configuration-reference)
17. [Implementation Checklist](#17-implementation-checklist)

---

## 0. What Changes From v1 (and Why)

| # | Area | v1 behaviour | Problem | v2 change |
|---|------|--------------|---------|-----------|
| 1 | **Massive timestamp** | Reads `snap.last_trade.timestamp` | **Bug.** The SDK's `LastTrade` has no `timestamp` attribute (its fields are `sip_timestamp`, `participant_timestamp`, `trf_timestamp`, all in **nanoseconds**). With real data, every snapshot raises `AttributeError` and is skipped, so the cache stays empty. The tests pass only because `MagicMock` invents the attribute. | Parse `sip_timestamp`, fall back to `snap.updated`, and detect the unit (ns, µs, ms or s). Tests build snapshots with the real `TickerSnapshot.from_dict`. |
| 2 | **Massive price** | `last_trade.price` only | `last_trade` can be `None`: plan entitlements, pre-market, or a thinly traded symbol. | Fallback chain: `last_trade.price` → `min.close` → `day.close` → `prev_day.close`. |
| 3 | **Daily change %** | `PriceUpdate` only knows the tick-to-tick change | PLAN §10 wants a "daily change %" column. A 500 ms tick change is almost always ~0.00 %. | Add `session_open` to `PriceUpdate` plus derived `day_change` and `day_change_percent`. Simulator baseline = seed price. Massive baseline = previous close. |
| 4 | **Massive resilience** | Fixed 15 s interval forever | A 429 response or an outage gets hammered at the same rate. A new ticker waits up to 15 s for its first price. | Exponential backoff on failure (capped). Adding a ticker wakes the poller early, subject to a rate-limit gap. |
| 5 | **Remove/poll race** | A poll in flight can write a ticker that was just removed | A removed ticker comes back into the cache and the SSE stream. | Drop snapshot rows for tickers that are no longer tracked. |
| 6 | **Ticker normalization** | Massive upper-cases tickers; the simulator does not | `"aapl"` and `"AAPL"` become two simulated tickers. | One `normalize_ticker()` in `models.py`, applied at every public entry point. |
| 7 | **Router footgun** | Module-level `router` mutated by `create_stream_router()` | Calling it twice (e.g. in tests) registers `/prices` twice. | Create the `APIRouter` inside the factory. |
| 8 | **SSE idle** | Nothing is sent while prices are unchanged | In Massive mode (15 s polls, or the market is closed), proxies can drop the idle connection. | Send a `: ping` comment every 15 s while idle. |
| 9 | **`add_ticker` before `start`** | Simulator silently ignores it (`_sim is None`) | Ordering bug waiting to happen. | Build the simulator in `__init__`. `start()` only seeds the tickers and launches the loop. |
| 10 | **Tracked set** | Spread across watchlist routes | If a ticker is removed from the watchlist while a position is held, valuation loses its price. | Tracked = watchlist ∪ open positions. One `sync_tracked_tickers()` helper reconciles them. |
| 11 | **Determinism** | Global `np.random` / `random` | E2E and unit tests can't reproduce a price path. | Inject a `numpy.random.Generator`. Optional `MARKET_SIM_SEED` env var. |
| 12 | **Small fixes** | `version` read without lock; `timestamp or time.time()` treats `0` as missing | Inconsistent; edge-case wrong. | Read under the lock; use `is None`. |

---

## 1. Architecture

```
                    ┌────────────────────────────────────────────┐
                    │  create_market_data_source(cache)          │
                    │  MASSIVE_API_KEY set?  yes → Massive       │
                    │                        no  → Simulator     │
                    └───────────────┬────────────────────────────┘
                                    │ returns
                 ┌──────────────────▼──────────────────┐
                 │      MarketDataSource (ABC)         │
                 │ start / stop / add / remove / get   │
                 └───────┬─────────────────────┬───────┘
                         │                     │
         ┌───────────────▼──────┐   ┌──────────▼──────────────┐
         │ SimulatorDataSource  │   │ MassiveDataSource       │
         │  GBMSimulator.step() │   │  REST snapshot poll     │
         │  every 0.5 s         │   │  every 15 s (+backoff)  │
         └───────────────┬──────┘   └──────────┬──────────────┘
                         │ cache.update(...)   │
                         ▼                     ▼
                 ┌─────────────────────────────────────┐
                 │  PriceCache  (thread-safe, latest   │
                 │  PriceUpdate per ticker, version #) │
                 └───┬──────────────┬──────────────┬───┘
                     │              │              │
          SSE /api/stream/prices  Trade execution  Portfolio valuation,
          (push every 0.5 s       (fill price)     LLM context, snapshots
           when version moves)
```

**Core rule:** producers only **write** to the `PriceCache`, and consumers only **read** from it. Nothing downstream calls a data source to get a price. The data source is touched only to change *which* tickers are tracked.

---

## 2. File Layout & Public API

```
backend/app/market/
├── __init__.py         # Re-exports the public API (below)
├── models.py           # PriceUpdate, normalize_ticker()
├── cache.py            # PriceCache
├── interface.py        # MarketDataSource ABC
├── seed_prices.py      # SEED_PRICES, TICKER_PARAMS, correlation constants
├── simulator.py        # GBMSimulator + SimulatorDataSource
├── massive_client.py   # MassiveDataSource + parse_snapshot()
├── factory.py          # create_market_data_source(), MarketConfig
├── tracking.py         # sync_tracked_tickers()            (new)
└── stream.py           # create_stream_router()
```

```python
# backend/app/market/__init__.py
"""Market data subsystem for FinAlly.

Downstream code imports only from here:

    from app.market import PriceCache, PriceUpdate, MarketDataSource, ...
"""

from .cache import PriceCache
from .factory import MarketConfig, create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate, normalize_ticker
from .stream import create_stream_router
from .tracking import sync_tracked_tickers

__all__ = [
    "MarketConfig",
    "MarketDataSource",
    "PriceCache",
    "PriceUpdate",
    "create_market_data_source",
    "create_stream_router",
    "normalize_ticker",
    "sync_tracked_tickers",
]
```

---

## 3. Data Model: `models.py`

`PriceUpdate` is the only type that leaves the market layer. It is immutable, so a single instance can safely be shared across the event loop, worker threads, and every SSE client.

```python
"""Data models for market data."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field

# 1-10 chars, starts with a letter; allows class shares like BRK.B / BF-B
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def normalize_ticker(raw: str) -> str:
    """Canonical ticker form used everywhere in the backend.

    >>> normalize_ticker("  aapl ")
    'AAPL'
    >>> normalize_ticker("brk.b")
    'BRK.B'
    >>> normalize_ticker("$$$")
    Traceback (most recent call last):
    ValueError: Invalid ticker symbol: '$$$'
    """
    ticker = (raw or "").strip().upper()
    if not _TICKER_RE.fullmatch(ticker):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    return ticker


def _pct(numerator: float, denominator: float | None) -> float:
    if not denominator:
        return 0.0
    return round(numerator / denominator * 100, 4)


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of one ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float                      # price at the previous update
    timestamp: float = field(default_factory=time.time)  # Unix seconds
    session_open: float | None = None          # baseline for "day change"

    # --- tick-to-tick (drives flash animation) ---
    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        return _pct(self.price - self.previous_price, self.previous_price)

    @property
    def direction(self) -> str:
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    # --- vs. session baseline (drives "daily change %" column) ---
    @property
    def day_change(self) -> float:
        if self.session_open is None:
            return 0.0
        return round(self.price - self.session_open, 4)

    @property
    def day_change_percent(self) -> float:
        if self.session_open is None:
            return 0.0
        return _pct(self.price - self.session_open, self.session_open)

    def to_dict(self) -> dict:
        """Single serialization point for SSE and REST responses."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "session_open": self.session_open,
            "day_change": self.day_change,
            "day_change_percent": self.day_change_percent,
        }
```

**Example**

```python
u = PriceUpdate("AAPL", price=191.00, previous_price=190.90, session_open=190.00)
u.direction            # 'up'
u.change               # 0.1
u.day_change_percent   # 0.5263
```

`session_open` defaults to `None`, so existing code that builds `PriceUpdate(ticker, price, previous_price, timestamp)` keeps working.

---

## 4. Price Cache: `cache.py`

The cache stores the latest `PriceUpdate` per ticker plus a monotonically increasing `version`. It is guarded by a `threading.Lock`: Massive does its I/O in a worker thread, and a lock keeps the class correct under free-threaded Python (3.13t) too. The critical sections are a few dict operations, so contention is negligible.

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Latest price per ticker.

    Writers: exactly one MarketDataSource.
    Readers: SSE stream, trade execution, portfolio valuation, LLM context.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        session_open: float | None = None,
    ) -> PriceUpdate:
        """Record a new price and return the resulting PriceUpdate.

        - First update for a ticker: previous_price == price (direction 'flat').
        - session_open: if not given, keep the existing baseline; if there is
          none yet, the first observed price becomes the baseline.
        """
        with self._lock:
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            if session_open is None:
                session_open = prev.session_open if prev and prev.session_open else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=time.time() if timestamp is None else timestamp,
                session_open=round(session_open, 2),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow copy; values are immutable, so this is a safe snapshot."""
        with self._lock:
            return dict(self._prices)

    def snapshot(self) -> tuple[int, dict[str, PriceUpdate]]:
        """Atomically read (version, prices). Used by the SSE loop."""
        with self._lock:
            return self._version, dict(self._prices)

    def remove(self, ticker: str) -> None:
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1  # so SSE clients see the removal

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Design notes**

- `remove()` now bumps `version`. In v1 a removal did not trigger an SSE push, so a deleted ticker lingered in the payload until the next price tick. With Massive polling every 15 s, that delay is visible.
- `snapshot()` returns the version and the prices from one lock acquisition, so the SSE loop never pairs a stale version with newer data.

---

## 5. Unified Interface: `interface.py`

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push PriceUpdates into a shared PriceCache on their own
    schedule; they never return prices to callers.

    Contract (both implementations MUST satisfy):
      * Tickers are normalized with normalize_ticker() at every entry point.
      * start() is called once; a second call raises RuntimeError.
      * add_ticker()/remove_ticker() are idempotent and may be called before
        start() (they just edit the tracked set).
      * remove_ticker() also removes the ticker from the PriceCache, and no
        later write for that ticker may reach the cache.
      * stop() is idempotent; after it returns, the source never writes again.
      * Background failures are logged, never raised into the event loop.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL"])
        await source.add_ticker("PYPL")
        await source.remove_ticker("GOOGL")
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing updates for `tickers` (plus any added earlier)."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task and release resources."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking `ticker`. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking `ticker` and evict it from the cache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers, in insertion order."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier for logs / health endpoint: 'simulator' | 'massive'."""
```

`name` is the only addition to the v1 interface. `/api/health` can report it (`{"status": "ok", "market_data": "simulator"}`), which helps debugging in Docker.

---

## 6. Seed Data: `seed_prices.py`

This module is unchanged from v1 and contains only constants.

```python
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
    "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
    "V": 280.00,    "NFLX": 600.00,
}

# Annualized GBM parameters
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # high vol
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # high vol, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},   # low vol (bank)
    "V": {"sigma": 0.17, "mu": 0.04},     # low vol (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}
INTRA_TECH_CORR = 0.6
INTRA_FINANCE_CORR = 0.5
CROSS_GROUP_CORR = 0.3   # cross-sector and unknown tickers
TSLA_CORR = 0.3          # TSLA "does its own thing"
```

**Positive-definiteness.** Every off-diagonal entry is in {0.3, 0.5, 0.6}, and the groups have a block structure. The resulting matrix is positive definite for any subset of tickers, including any number of unknown tickers at 0.3 (an equicorrelated block with ρ ≥ 0 is PD). So `np.linalg.cholesky` cannot fail. A test (§14) asserts this for the default 10 tickers plus 40 unknown ones.

---

## 7. GBM Simulator: `simulator.py`

### 7.1 The math

At each tick, every ticker evolves under geometric Brownian motion:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )
```

- `dt = 0.5 / (252 · 6.5 · 3600) ≈ 8.48e-8`, i.e. 500 ms expressed as a fraction of a trading year.
- `Z = L · z`, where `z ~ N(0, I)` and `L = cholesky(C)`. This gives correlated shocks: tech names move together.
- **Shock events:** each ticker has probability `p = 0.001` per tick of a 2–5 % jump. With 10 tickers at 2 ticks/s, that is roughly one event every 50 s somewhere on the board.

The per-tick standard deviation is `σ·√dt`. For AAPL that is `0.22 · 2.9e-4 ≈ 0.0064 %`, about 1.2¢ on $190. Over a 6.5 h simulated day the moves add up to a realistic ~1.4 % daily σ.

### 7.2 `GBMSimulator` (pure, synchronous, no I/O)

```python
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
    CORRELATION_GROUPS, CROSS_GROUP_CORR, DEFAULT_PARAMS, INTRA_FINANCE_CORR,
    INTRA_TECH_CORR, SEED_PRICES, TICKER_PARAMS, TSLA_CORR,
)

logger = logging.getLogger(__name__)

TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800


class GBMSimulator:
    """Correlated GBM price paths for a dynamic set of tickers."""

    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR

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
        self._rng = rng or np.random.default_rng()

        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        self.add_tickers(tickers or [])

    # ---------- public ----------

    def step(self) -> dict[str, float]:
        """Advance every ticker by one dt. Returns {ticker: rounded price}."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z = self._rng.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z

        shocks = self._rng.random(n) < self._event_prob  # vectorized event draw
        out: dict[str, float] = {}

        for i, ticker in enumerate(self._tickers):
            mu, sigma = self._params[ticker]["mu"], self._params[ticker]["sigma"]
            drift = (mu - 0.5 * sigma * sigma) * self._dt
            diffusion = sigma * self._sqrt_dt * z[i]
            price = self._prices[ticker] * math.exp(drift + diffusion)

            if shocks[i]:
                magnitude = self._rng.uniform(0.02, 0.05)
                sign = 1.0 if self._rng.random() < 0.5 else -1.0
                price *= 1.0 + sign * magnitude
                logger.debug("Event on %s: %+.1f%%", ticker, sign * magnitude * 100)

            self._prices[ticker] = price
            out[ticker] = round(price, 2)

        return out

    def add_tickers(self, tickers: list[str]) -> list[str]:
        """Batch add (one Cholesky rebuild). Returns the tickers actually added."""
        added = []
        for raw in tickers:
            t = normalize_ticker(raw)
            if t in self._prices:
                continue
            self._tickers.append(t)
            self._prices[t] = SEED_PRICES.get(t) or float(self._rng.uniform(50.0, 300.0))
            self._params[t] = dict(TICKER_PARAMS.get(t, DEFAULT_PARAMS))
            added.append(t)
        if added:
            self._rebuild_cholesky()
        return added

    def add_ticker(self, ticker: str) -> bool:
        return bool(self.add_tickers([ticker]))

    def remove_ticker(self, ticker: str) -> bool:
        t = normalize_ticker(ticker)
        if t not in self._prices:
            return False
        self._tickers.remove(t)
        del self._prices[t], self._params[t]
        self._rebuild_cholesky()
        return True

    def get_price(self, ticker: str) -> float | None:
        p = self._prices.get(ticker)
        return round(p, 2) if p is not None else None

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # ---------- internals ----------

    def _rebuild_cholesky(self) -> None:
        """O(n²) matrix build + O(n³) factorization; n < 50 so this is µs."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                corr[i, j] = corr[j, i] = self.pairwise_correlation(
                    self._tickers[i], self._tickers[j]
                )
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def pairwise_correlation(t1: str, t2: str) -> float:
        if "TSLA" in (t1, t2):
            return TSLA_CORR
        tech, fin = CORRELATION_GROUPS["tech"], CORRELATION_GROUPS["finance"]
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in fin and t2 in fin:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

**Example: standalone use**

```python
sim = GBMSimulator(["AAPL", "NVDA"], rng=np.random.default_rng(42))
sim.step()   # e.g. {'AAPL': 190.01, 'NVDA': 799.97}: sub-cent-to-cents moves per tick
sim.add_ticker("pypl")          # normalized to 'PYPL'; random seed in [50, 300]
sim.get_tickers()               # ['AAPL', 'NVDA', 'PYPL']
```

### 7.3 `SimulatorDataSource` (async wrapper)

```python
class SimulatorDataSource(MarketDataSource):
    """Steps the GBMSimulator every `update_interval` seconds into the cache."""

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
        seed: int | None = None,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
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
        # Seed the cache so the first SSE push / first trade has a price.
        for t in self._sim.get_tickers():
            self._seed_cache(t)
        self._task = asyncio.create_task(self._run_loop(), name="market-simulator")
        logger.info("Simulator started: %d tickers", len(self._sim.get_tickers()))

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
        if self._sim.add_ticker(ticker) and self._task is not None:
            self._seed_cache(normalize_ticker(ticker))  # price available instantly

    async def remove_ticker(self, ticker: str) -> None:
        t = normalize_ticker(ticker)
        self._sim.remove_ticker(t)
        self._cache.remove(t)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers()

    # ---------- internals ----------

    def _seed_cache(self, ticker: str) -> None:
        price = self._sim.get_price(ticker)
        if price is not None:
            # The seed price is the "session open" for day-change purposes.
            self._cache.update(ticker, price, session_open=price)

    async def _run_loop(self) -> None:
        loop = asyncio.get_running_loop()
        next_tick = loop.time()
        while True:
            try:
                for ticker, price in self._sim.step().items():
                    self._cache.update(ticker, price)
            except Exception:
                logger.exception("Simulator step failed")
            # Fixed-rate schedule (no drift from step() runtime)
            next_tick += self._interval
            await asyncio.sleep(max(0.0, next_tick - loop.time()))
```

**Concurrency.** `step()`, `add_ticker()` and `remove_ticker()` all run on the event loop thread, and none of them `await` midway through. So the simulator's internal dicts are never mutated concurrently and need no lock. Only `PriceCache` needs one.

**Remove race.** The loop writes the dict returned by `step()`. Because `remove_ticker` cannot interleave with a `step()` plus its write loop (no `await` between them), a removed ticker can never be written back.

---

## 8. Massive API Client: `massive_client.py`

### 8.1 Endpoint & SDK facts (verified against the installed `massive` SDK)

- One call per poll: `client.get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=[...])` → `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT`. It returns `list[TickerSnapshot]`.
- `TickerSnapshot` fields: `ticker`, `day: Agg`, `prev_day: Agg`, `min: MinuteSnapshot`, `last_trade: LastTrade`, `last_quote: LastQuote`, `todays_change`, `todays_change_percent`, `updated` (ns). **Every field is `Optional`.**
- `LastTrade` fields: `price`, `size`, `sip_timestamp` (ns), `participant_timestamp` (ns), and others. **There is no `timestamp` field.**
- `Agg` fields: `open`, `high`, `low`, `close`, `volume`, `vwap`, `timestamp` (ms).
- Errors: `massive.exceptions.AuthError` (missing key) and `BadResponse` (any non-200, including 401/403/429). urllib3 retries 5xx/429 internally (`retries=3` by default).
- The client is **synchronous** (urllib3), so it runs in `asyncio.to_thread`.
- Free tier allows 5 requests/min, so the minimum spacing between polls is 12 s.

### 8.2 Snapshot parsing (pure function, unit-testable)

```python
"""Massive (Polygon.io) REST poller."""

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


@dataclass(frozen=True, slots=True)
class ParsedQuote:
    ticker: str
    price: float
    timestamp: float            # Unix seconds
    session_open: float | None  # previous close (baseline for day change)


def to_unix_seconds(ts: float | int | None) -> float | None:
    """Massive mixes ns (trades/updated), ms (aggs). Detect by magnitude."""
    if not ts:
        return None
    ts = float(ts)
    if ts > 1e17:   # nanoseconds
        return ts / 1e9
    if ts > 1e14:   # microseconds
        return ts / 1e6
    if ts > 1e11:   # milliseconds
        return ts / 1e3
    return ts       # already seconds


def _first_positive(*values: Any) -> float | None:
    for v in values:
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
    return None


def parse_snapshot(snap: Any) -> ParsedQuote | None:
    """Extract (ticker, price, ts, baseline) from a TickerSnapshot, or None.

    Price fallback order:
      last_trade.price → min.close → day.close → prev_day.close
    getattr(None, "x", None) is None, so missing sub-objects are fine.
    """
    raw_ticker = getattr(snap, "ticker", None)
    if not raw_ticker:
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
    return ParsedQuote(raw_ticker.upper(), price, timestamp, session_open)
```

**Example: what a real response becomes**

```python
from massive.rest.models import TickerSnapshot

snap = TickerSnapshot.from_dict({
    "ticker": "AAPL",
    "lastTrade": {"p": 190.52, "s": 100, "t": 1707580800123456789},
    "day": {"o": 189.10, "c": 190.40},
    "prevDay": {"c": 188.00},
    "updated": 1707580800200000000,
})
parse_snapshot(snap)
# ParsedQuote(ticker='AAPL', price=190.52, timestamp=1707580800.1234567, session_open=188.0)
```

### 8.3 `MassiveDataSource`

```python
class MassiveDataSource(MarketDataSource):
    """Polls the Massive snapshot endpoint for all tracked tickers.

    Scheduling:
      * Normal cadence: every `poll_interval` s (15 s default = free tier safe).
      * On failure: exponential backoff, capped at `max_backoff`.
      * add_ticker() wakes the loop early so new tickers get a price fast,
        but never sooner than `min_poll_gap` after the previous call
        (12 s = 5 req/min free-tier ceiling).
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
        min_poll_gap: float = 12.0,
        max_backoff: float = 120.0,
        client: RESTClient | None = None,  # injectable for tests
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._min_gap = min(min_poll_gap, poll_interval)
        self._max_backoff = max_backoff
        self._client = client
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._last_poll = float("-inf")  # loop.time() of last API call
        self._auth_failed = False

    @property
    def name(self) -> str:
        return "massive"

    # ---------- lifecycle ----------

    async def start(self, tickers: list[str]) -> None:
        if self._task is not None:
            raise RuntimeError("MassiveDataSource already started")
        if self._client is None:
            # Short timeouts + 1 retry: a dead network must not stall app startup
            self._client = RESTClient(
                api_key=self._api_key, connect_timeout=5.0, read_timeout=5.0, retries=1
            )
        for t in tickers:
            await self.add_ticker(t)
        await self._poll_once()  # immediate first fill; failures are tolerated
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller started: %d tickers, %.1fs interval",
                    len(self._tickers), self._interval)

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("Massive poller stopped")

    # ---------- tracked set ----------

    async def add_ticker(self, ticker: str) -> None:
        t = normalize_ticker(ticker)
        if t not in self._tickers:
            self._tickers.append(t)
            self._wake.set()  # ask the loop for an early poll

    async def remove_ticker(self, ticker: str) -> None:
        t = normalize_ticker(ticker)
        self._tickers = [x for x in self._tickers if x != t]
        self._cache.remove(t)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # ---------- polling ----------

    async def _poll_loop(self) -> None:
        loop = asyncio.get_running_loop()
        delay = self._interval
        while True:
            # Sleep until the interval elapses OR add_ticker() wakes us
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
        """One API call → cache writes. Returns False on failure."""
        tickers = list(self._tickers)  # stable copy for the worker thread
        if not tickers or self._client is None:
            return True

        self._last_poll = asyncio.get_running_loop().time()
        try:
            snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except (AuthError, BadResponse) as e:
            msg = str(e)
            if "401" in msg or "403" in msg or isinstance(e, AuthError):
                if not self._auth_failed:  # log loudly once, not every 15 s
                    logger.error("Massive auth/plan error, check MASSIVE_API_KEY: %s", msg)
                self._auth_failed = True
            else:
                logger.warning("Massive bad response (will back off): %s", msg[:200])
            return False
        except Exception as e:  # network, timeout, JSON, ...
            logger.warning("Massive poll failed (will back off): %s", e)
            return False

        self._auth_failed = False
        tracked = set(self._tickers)  # re-read AFTER the await (remove race)
        written = 0
        for snap in snapshots or []:
            quote = parse_snapshot(snap)
            if quote is None or quote.ticker not in tracked:
                continue
            self._cache.update(
                quote.ticker, quote.price,
                timestamp=quote.timestamp, session_open=quote.session_open,
            )
            written += 1

        missing = tracked - {getattr(s, "ticker", None) for s in snapshots or []}
        if missing:
            logger.debug("Massive returned no data for: %s", sorted(missing))
        logger.debug("Massive poll: %d/%d tickers updated", written, len(tickers))
        return True

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Blocking SDK call; runs in a worker thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=tickers,
        )
```

**Timeline example (free tier, `poll_interval=15`, `min_poll_gap=12`):**

```
t=0     start(): poll #1 → 10 tickers in cache
t=15    poll #2
t=20    user adds PYPL → wake
t=27    poll #3 (waited until 12 s after #2) → PYPL priced
t=42    poll #4  (normal cadence resumes)
t=57    poll #5 → 429 → delay 30
t=87    poll #6 → 429 → delay 60
t=147   poll #7 → OK  → delay back to 15
```

**Market closed.** Outside trading hours the snapshot keeps returning the last trade, so prices don't change. `PriceCache.update` still bumps `version` with a `flat` direction, so the frontend shows stable prices and no flashes. That is the correct behaviour.

---

## 9. Factory & Configuration: `factory.py`

```python
"""Select and configure the market data source from the environment."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    try:
        return float(raw) if raw else default
    except ValueError:
        logger.warning("Ignoring invalid %s=%r; using %s", name, raw, default)
        return default


@dataclass(frozen=True)
class MarketConfig:
    massive_api_key: str = ""
    massive_poll_interval: float = 15.0
    sim_update_interval: float = 0.5
    sim_seed: int | None = None

    @classmethod
    def from_env(cls) -> MarketConfig:
        seed = os.environ.get("MARKET_SIM_SEED", "").strip()
        return cls(
            massive_api_key=os.environ.get("MASSIVE_API_KEY", "").strip(),
            massive_poll_interval=max(2.0, _env_float("MASSIVE_POLL_INTERVAL", 15.0)),
            sim_update_interval=_env_float("MARKET_SIM_INTERVAL", 0.5),
            sim_seed=int(seed) if seed.isdigit() else None,
        )


def create_market_data_source(
    price_cache: PriceCache, config: MarketConfig | None = None
) -> MarketDataSource:
    """MASSIVE_API_KEY non-empty → Massive; otherwise → simulator. Unstarted."""
    cfg = config or MarketConfig.from_env()
    if cfg.massive_api_key:
        logger.info("Market data: Massive API (poll every %.1fs)", cfg.massive_poll_interval)
        return MassiveDataSource(
            api_key=cfg.massive_api_key,
            price_cache=price_cache,
            poll_interval=cfg.massive_poll_interval,
        )
    logger.info("Market data: GBM simulator")
    return SimulatorDataSource(
        price_cache=price_cache,
        update_interval=cfg.sim_update_interval,
        seed=cfg.sim_seed,
    )
```

The signature stays compatible with v1: `create_market_data_source(cache)` with no config reads the environment, exactly as before.

---

## 10. Ticker Tracking Sync: `tracking.py`

**Rule:** the source tracks **watchlist ∪ tickers with an open position**. Portfolio valuation needs a live price for every held ticker, even one the user removed from the watchlist.

Instead of sprinkling `add_ticker` and `remove_ticker` calls through the routes, every mutation calls one reconcile function. It is idempotent and makes the right call for every route.

```python
"""Keep the data source's tracked set equal to the desired set."""

from __future__ import annotations

from collections.abc import Iterable

from .interface import MarketDataSource
from .models import normalize_ticker


async def sync_tracked_tickers(
    source: MarketDataSource, desired: Iterable[str]
) -> tuple[list[str], list[str]]:
    """Add missing / remove extra tickers. Returns (added, removed)."""
    want = {normalize_ticker(t) for t in desired}
    have = set(source.get_tickers())
    added = sorted(want - have)
    removed = sorted(have - want)
    for t in added:
        await source.add_ticker(t)
    for t in removed:
        await source.remove_ticker(t)
    return added, removed
```

Call it after: startup, `POST /api/watchlist`, `DELETE /api/watchlist/{t}`, every trade, and every LLM action batch. The `desired` set comes from the DB layer:

```sql
SELECT ticker FROM watchlist WHERE user_id = :uid
UNION
SELECT ticker FROM positions WHERE user_id = :uid AND quantity > 0;
```

| Event | Watchlist | Position | Result |
|---|---|---|---|
| Add PYPL to watchlist | +PYPL | – | PYPL added → simulator prices it instantly; Massive wakes for an early poll |
| Remove GOOGL, no position | −GOOGL | – | GOOGL removed from source + cache |
| Remove NVDA, holding 5 | −NVDA | NVDA | NVDA **stays tracked** (valuation keeps working) |
| Sell all NVDA (not watched) | – | −NVDA | NVDA removed |

---

## 11. SSE Streaming: `stream.py`

```python
"""SSE endpoint: GET /api/stream/prices."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

PUSH_INTERVAL = 0.5        # seconds between cache checks
HEARTBEAT_INTERVAL = 15.0  # idle keep-alive comment
RETRY_MS = 1000            # EventSource reconnect delay


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Router factory. A fresh APIRouter per call (safe to call in tests)."""
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            generate_price_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


def format_prices_event(prices: dict) -> str:
    payload = {t: u.to_dict() for t, u in prices.items()}
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"


async def generate_price_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = PUSH_INTERVAL,
    heartbeat: float = HEARTBEAT_INTERVAL,
) -> AsyncGenerator[str, None]:
    """Yield a full price snapshot whenever the cache version changes."""
    yield f"retry: {RETRY_MS}\n\n"

    loop = asyncio.get_running_loop()
    last_version = -1
    last_sent = loop.time()
    client = request.client.host if request.client else "unknown"
    logger.info("SSE connected: %s", client)

    try:
        while not await request.is_disconnected():
            version, prices = price_cache.snapshot()
            if version != last_version:
                last_version = version
                yield format_prices_event(prices)  # may be {} after last removal
                last_sent = loop.time()
            elif loop.time() - last_sent >= heartbeat:
                yield ": ping\n\n"  # SSE comment, ignored by EventSource
                last_sent = loop.time()
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("SSE disconnected: %s", client)
```

**Why snapshot-on-version instead of per-tick deltas?** Ten tickers at 2 Hz is about 3 KB/s per client, which is negligible. A full snapshot means a reconnecting client is complete after a single event, with no gap-filling logic. Regular 500 ms spacing also gives the frontend evenly spaced sparkline points.

### Wire format

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.52,"previous_price":190.47,"timestamp":1763900000.51,"change":0.05,"change_percent":0.0263,"direction":"up","session_open":190.0,"day_change":0.52,"day_change_percent":0.2737},"GOOGL":{...}}

: ping

```

---

## 12. FastAPI Integration

### 12.1 App factory and lifespan (`backend/app/main.py`)

Create the cache and the source **before** the app is built, so the router can be included at construction time and not from inside `lifespan`.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.db import get_tracked_tickers, init_db   # DB layer (owned by backend agent)
from app.market import (
    MarketDataSource, PriceCache, create_market_data_source,
    create_stream_router, sync_tracked_tickers,
)


def create_app() -> FastAPI:
    price_cache = PriceCache()
    source = create_market_data_source(price_cache)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db()                                           # lazy schema + seed
        await source.start(get_tracked_tickers("default"))  # watchlist ∪ positions
        # ... also start the 30 s portfolio-snapshot task here ...
        try:
            yield
        finally:
            await source.stop()

    app = FastAPI(title="FinAlly", lifespan=lifespan)
    app.state.price_cache = price_cache
    app.state.market_source = source
    app.include_router(create_stream_router(price_cache))
    # app.include_router(portfolio_router), watchlist_router, chat_router ...
    # app.mount("/", StaticFiles(directory="static", html=True))  # LAST
    return app


app = create_app()


# --- dependencies used by other routers ---
def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

### 12.2 Using market data from other routes

```python
from fastapi import APIRouter, Depends, HTTPException
from app.market import normalize_ticker, sync_tracked_tickers

router = APIRouter(prefix="/api")


@router.post("/watchlist", status_code=201)
async def add_watchlist(body: WatchlistAdd,
                        cache: PriceCache = Depends(get_price_cache),
                        source: MarketDataSource = Depends(get_market_source)):
    try:
        ticker = normalize_ticker(body.ticker)
    except ValueError as e:
        raise HTTPException(422, str(e))
    db.add_watchlist(ticker)                       # UNIQUE(user_id, ticker) → 409
    await sync_tracked_tickers(source, db.get_tracked_tickers("default"))
    update = cache.get(ticker)                     # simulator: present; Massive: maybe not yet
    return {"ticker": ticker, "price": update.to_dict() if update else None}


@router.post("/portfolio/trade")
async def trade(body: TradeRequest,
                cache: PriceCache = Depends(get_price_cache),
                source: MarketDataSource = Depends(get_market_source)):
    ticker = normalize_ticker(body.ticker)
    if ticker not in source.get_tickers():
        await source.add_ticker(ticker)            # simulator prices it immediately
    price = cache.get_price(ticker)
    if price is None:
        raise HTTPException(409, f"No price yet for {ticker}; try again in a few seconds")
    result = db.execute_trade(ticker, body.side, body.quantity, price)  # validates cash/shares
    await sync_tracked_tickers(source, db.get_tracked_tickers("default"))
    return result


@router.get("/watchlist")
async def list_watchlist(cache: PriceCache = Depends(get_price_cache)):
    return [
        {"ticker": t, **(u.to_dict() if (u := cache.get(t)) else {"price": None})}
        for t in db.get_watchlist("default")
    ]
```

**Portfolio valuation and LLM context** read `cache.get_price(t)` for each position. If the price is missing (Massive hasn't polled yet), fall back to `avg_cost` and flag `"price_stale": true` instead of failing the request.

---

## 13. Frontend Contract

```ts
// frontend/src/lib/types.ts
export interface PriceUpdate {
  ticker: string;
  price: number;
  previous_price: number;
  timestamp: number;          // Unix seconds
  change: number;             // vs previous tick → flash color
  change_percent: number;
  direction: "up" | "down" | "flat";
  session_open: number | null;
  day_change: number;         // vs session baseline → "Chg %" column
  day_change_percent: number;
}
export type PriceSnapshot = Record<string, PriceUpdate>;
```

```ts
// Minimal consumer
const es = new EventSource("/api/stream/prices");
es.onopen = () => setStatus("connected");
es.onerror = () => setStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
es.onmessage = (e) => {
  const snap: PriceSnapshot = JSON.parse(e.data);
  for (const u of Object.values(snap)) {
    if (u.direction !== "flat") flash(u.ticker, u.direction);  // ~500 ms CSS fade
    appendSparklinePoint(u.ticker, u.timestamp, u.price);
  }
  setPrices(snap);   // tickers absent from snap were removed
};
```

---

## 14. Testing Plan

All tests live in `backend/tests/market/`. The existing 73 tests stay. Some need small updates (§17). These are the new tests.

### 14.1 Massive parsing against the **real** SDK models (regression for bug #1)

```python
# tests/market/test_massive_parse.py
from massive.rest.models import TickerSnapshot

from app.market.massive_client import parse_snapshot, to_unix_seconds


def snap(**d):
    return TickerSnapshot.from_dict({"ticker": "AAPL", **d})


def test_last_trade_price_and_ns_timestamp():
    q = parse_snapshot(snap(lastTrade={"p": 190.5, "t": 1_707_580_800_123_456_789},
                            prevDay={"c": 188.0}))
    assert q.price == 190.5
    assert abs(q.timestamp - 1_707_580_800.123) < 1e-3
    assert q.session_open == 188.0


def test_fallback_when_no_last_trade():
    q = parse_snapshot(snap(day={"c": 189.9, "o": 189.0}))
    assert q.price == 189.9 and q.session_open == 189.0


def test_nothing_usable_returns_none():
    assert parse_snapshot(snap()) is None
    assert parse_snapshot(TickerSnapshot.from_dict({})) is None


def test_timestamp_units():
    assert to_unix_seconds(1_700_000_000) == 1_700_000_000
    assert to_unix_seconds(1_700_000_000_000) == 1_700_000_000
    assert to_unix_seconds(1_700_000_000_000_000_000) == 1_700_000_000
    assert to_unix_seconds(None) is None
```

### 14.2 Massive scheduling & races

```python
# tests/market/test_massive.py (additions)
from unittest.mock import MagicMock
from massive.exceptions import BadResponse
from massive.rest.models import TickerSnapshot


async def test_removed_ticker_not_rewritten_by_inflight_poll():
    cache = PriceCache()
    src = MassiveDataSource("k", cache, client=MagicMock())
    src._tickers = ["AAPL", "GOOGL"]

    def fetch(_tickers):
        src._tickers.remove("GOOGL")            # simulate remove during fetch
        return [TickerSnapshot.from_dict({"ticker": t, "lastTrade": {"p": 100}})
                for t in ("AAPL", "GOOGL")]

    src._fetch_snapshots = fetch
    assert await src._poll_once()
    assert "AAPL" in cache and "GOOGL" not in cache


async def test_bad_response_returns_false_and_keeps_cache():
    cache = PriceCache()
    client = MagicMock()
    client.get_snapshot_all.side_effect = BadResponse('{"status":"ERROR","error":"429"}')
    src = MassiveDataSource("k", cache, client=client)
    src._tickers = ["AAPL"]
    assert await src._poll_once() is False
    assert len(cache) == 0


async def test_add_ticker_wakes_loop():
    src = MassiveDataSource("k", PriceCache(), client=MagicMock())
    await src.add_ticker("pypl")
    assert src.get_tickers() == ["PYPL"] and src._wake.is_set()
```

### 14.3 Simulator determinism & matrix validity

```python
import numpy as np
from app.market.simulator import GBMSimulator
from app.market.seed_prices import SEED_PRICES


def test_seeded_runs_are_reproducible():
    a = GBMSimulator(["AAPL", "TSLA"], rng=np.random.default_rng(7))
    b = GBMSimulator(["AAPL", "TSLA"], rng=np.random.default_rng(7))
    assert [a.step() for _ in range(50)] == [b.step() for _ in range(50)]


def test_cholesky_ok_for_defaults_plus_many_unknowns():
    sim = GBMSimulator(list(SEED_PRICES) + [f"X{i}" for i in range(40)])
    assert sim._cholesky.shape == (50, 50)


def test_tickers_are_normalized():
    sim = GBMSimulator(["aapl"])
    sim.add_ticker(" AAPL ")
    assert sim.get_tickers() == ["AAPL"]


def test_log_return_volatility_matches_sigma():
    """Empirical per-tick σ of log-returns ≈ σ·√dt (no events)."""
    sim = GBMSimulator(["AAPL"], event_probability=0.0, rng=np.random.default_rng(1))
    p = [sim._prices["AAPL"]]
    for _ in range(20_000):
        sim.step(); p.append(sim._prices["AAPL"])
    r = np.diff(np.log(p))
    expected = 0.22 * np.sqrt(GBMSimulator.DEFAULT_DT)
    assert abs(r.std() / expected - 1) < 0.05
```

### 14.4 Cache & model additions

```python
def test_session_open_is_sticky():
    c = PriceCache()
    c.update("AAPL", 100.0, session_open=100.0)
    u = c.update("AAPL", 101.0)
    assert u.session_open == 100.0 and u.day_change_percent == 1.0


def test_remove_bumps_version():
    c = PriceCache(); c.update("AAPL", 1.0); v = c.version
    c.remove("AAPL"); assert c.version == v + 1
    c.remove("AAPL"); assert c.version == v + 1   # no-op doesn't bump
```

### 14.5 SSE generator (no server needed)

```python
import json
from app.market.stream import generate_price_events


class FakeRequest:
    client = None

    def __init__(self, polls: int):
        self._left = polls

    async def is_disconnected(self) -> bool:
        self._left -= 1
        return self._left < 0


async def test_sse_emits_retry_then_snapshot_then_stops():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    events = [e async for e in generate_price_events(cache, FakeRequest(3), interval=0.001)]
    assert events[0] == "retry: 1000\n\n"
    data = json.loads(events[1].removeprefix("data: "))
    assert data["AAPL"]["price"] == 190.0
    assert len(events) == 2          # unchanged version → no duplicate pushes


async def test_sse_heartbeat_when_idle():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    events = [e async for e in generate_price_events(
        cache, FakeRequest(5), interval=0.001, heartbeat=0.0)]
    assert ": ping\n\n" in events


def test_router_factory_is_reentrant():
    from app.market import create_stream_router
    r1, r2 = create_stream_router(PriceCache()), create_stream_router(PriceCache())
    assert len(r1.routes) == len(r2.routes) == 1
```

### 14.6 Tracking sync

```python
async def test_sync_tracked_tickers():
    cache = PriceCache()
    src = SimulatorDataSource(cache, seed=1)
    await src.start(["AAPL", "GOOGL"])
    added, removed = await sync_tracked_tickers(src, ["aapl", "NVDA"])
    assert added == ["NVDA"] and removed == ["GOOGL"]
    assert "NVDA" in cache and "GOOGL" not in cache
    await src.stop()
```

### 14.7 Optional live smoke test (skipped without a key)

```python
@pytest.mark.skipif(not os.environ.get("MASSIVE_API_KEY"), reason="needs real key")
async def test_massive_live_smoke():
    cache = PriceCache()
    src = MassiveDataSource(os.environ["MASSIVE_API_KEY"], cache)
    await src.start(["AAPL", "MSFT"])
    await src.stop()
    assert cache.get_price("AAPL") is not None
```

---

## 15. Edge Cases & Failure Modes

| Situation | Behaviour |
|---|---|
| Empty tracked set at startup | Simulator `step()` returns `{}`. Massive skips the API call. SSE sends nothing until a ticker is added (heartbeats keep the connection alive). |
| Invalid ticker string (`"$$$"`, `""`) | `normalize_ticker` raises `ValueError`, which the route maps to 422. |
| Well-formed but nonexistent symbol (`"ZZZZ"`) | **Simulator:** gets a random price in $50–$300 (by design: any symbol is tradable in the sim). **Massive:** no snapshot row, so no price. The watchlist shows "—" and trades return 409. Debug log lists the missing symbols. |
| Invalid / expired Massive key | First poll gets 401 → one ERROR log → backoff to 120 s. The app keeps serving with no prices. Fix the key and restart. |
| Rate limited (429) | urllib3 retries once, then `BadResponse`, then exponential backoff. |
| Network down during startup | `start()`'s first poll fails within ~10 s (5 s timeouts, 1 retry). Startup continues and the loop keeps retrying. |
| Trade on an untracked ticker | Route calls `add_ticker`. The simulator fills immediately. Massive returns 409 until the woken poll lands (≤ 12 s). |
| Ticker removed while a position is held | Still tracked via `sync_tracked_tickers` (watchlist ∪ positions). |
| Simulator step raises (shouldn't) | Logged with traceback. Loop continues on the next tick. |
| Client disconnects | `is_disconnected()` ends the generator within 500 ms, and the task is cleaned up. |
| Many SSE clients | Each client is one coroutine plus one `json.dumps` per version change. Fine for dozens. Cache the serialized string per version if it ever matters. |

---

## 16. Configuration Reference

| Env var | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | *(empty)* | Non-empty → Massive. Empty → simulator. |
| `MASSIVE_POLL_INTERVAL` | `15` | Seconds between polls (min 2). Use 2–5 on paid tiers. |
| `MARKET_SIM_INTERVAL` | `0.5` | Simulator tick period (seconds). |
| `MARKET_SIM_SEED` | *(unset)* | Integer → deterministic simulator (E2E tests). |

| Constant | Location | Value |
|---|---|---|
| `event_probability` | `SimulatorDataSource` | 0.001 per ticker per tick |
| `dt` | `GBMSimulator.DEFAULT_DT` | 0.5 / 5,896,800 ≈ 8.48e-8 |
| `min_poll_gap` | `MassiveDataSource` | 12 s (free-tier 5 req/min) |
| `max_backoff` | `MassiveDataSource` | 120 s |
| `PUSH_INTERVAL` | `stream.py` | 0.5 s |
| `HEARTBEAT_INTERVAL` | `stream.py` | 15 s |
| `RETRY_MS` | `stream.py` | 1000 ms |

Add the new optional vars to `.env.example`, commented out.

---

## 17. Implementation Checklist

Each step should leave `uv run --extra dev pytest` green.

1. **`models.py`**: add `normalize_ticker`, `session_open`, `day_change` and `day_change_percent`, and extend `to_dict()`. Update `test_models.py` for the new dict keys.
2. **`cache.py`**: add the `session_open` param, `snapshot()`, version reads under the lock, `remove()` bumping the version, and `timestamp is None`.
3. **`interface.py`**: add the abstract `name` property and the contract docstring.
4. **`simulator.py`**: inject the `rng`, add `add_tickers()` batching, normalization, eager simulator construction, a fixed-rate loop, and `session_open` seeding. Existing tests that read `sim._tickers` still work.
5. **`massive_client.py`** (**fixes the production bug**): add `parse_snapshot`, `to_unix_seconds`, backoff/wake scheduling, the remove-race guard, an injectable `client`, and short client timeouts. Update `_make_snapshot` in `test_massive.py` to use `TickerSnapshot.from_dict` (MagicMock hid the bug). Change `_fetch_snapshots` to take a `tickers` argument and update the mocks that patch it.
6. **`factory.py`**: add `MarketConfig.from_env()` with the new env vars. Keep the no-arg call working.
7. **`tracking.py`**: new `sync_tracked_tickers`.
8. **`stream.py`**: per-call router, `snapshot()`, heartbeat, and the public `generate_price_events` / `format_prices_event`.
9. **`__init__.py`**: export the new names.
10. **Tests**: add everything in §14. Target ≥ 90 % coverage on `app/market` (stream.py rises from 31 %).
11. **Docs**: update `backend/CLAUDE.md` (API list) and `MARKET_DATA_SUMMARY.md` once the work merges. Run `uv run market_data_demo.py` as a manual smoke test.
