# Market Data Backend — Summary

**Status:** Complete. v2 (per `planning/MARKET_DATA_DESIGN.md`) implemented: Massive timestamp bug fixed, daily change %, Massive backoff/early-wake, ticker normalization, watchlist ∪ positions tracking, SSE heartbeat. 191 tests, 99% coverage.

## What Was Built

A complete market data subsystem in `backend/app/market/` (9 modules, ~550 statements) providing live price simulation and real market data via a unified interface.

### Architecture

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Polygon.io REST poller (when MASSIVE_API_KEY set)
        │
        ▼
   PriceCache (thread-safe, in-memory)
        │
        ├──→ SSE stream endpoint (/api/stream/prices)
        ├──→ Portfolio valuation
        └──→ Trade execution
```

### Modules

| File | Purpose |
|------|---------|
| `models.py` | `PriceUpdate` — immutable frozen dataclass (ticker, price, previous_price, timestamp, change, direction) |
| `interface.py` | `MarketDataSource` — abstract base class defining `start/stop/add_ticker/remove_ticker/get_tickers` |
| `cache.py` | `PriceCache` — thread-safe price store with version counter for SSE change detection |
| `seed_prices.py` | Realistic seed prices, per-ticker GBM params (drift/volatility), correlation groups |
| `simulator.py` | `GBMSimulator` (Geometric Brownian Motion with Cholesky-correlated moves) + `SimulatorDataSource` |
| `massive_client.py` | `MassiveDataSource` — REST polling client for Polygon.io via the `massive` package |
| `factory.py` | `create_market_data_source()` — selects simulator or Massive based on `MASSIVE_API_KEY` env var |
| `stream.py` | `create_stream_router()` — FastAPI SSE endpoint factory using version-based change detection + idle heartbeat |
| `tracking.py` | `sync_tracked_tickers()` — reconciles the tracked set to watchlist ∪ open positions |

### Key Design Decisions

- **Strategy pattern** — both data sources implement the same ABC; downstream code is source-agnostic
- **PriceCache as single point of truth** — producers write, consumers read; no direct coupling
- **GBM with correlated moves** — Cholesky decomposition of sector-based correlation matrix; tech stocks correlate at 0.6, finance at 0.5, cross-sector at 0.3
- **Random shock events** — ~0.1% chance per tick per ticker of a 2-5% move for visual drama
- **SSE over WebSockets** — simpler, one-way push, universal browser support

## Test Suite

**191 tests, all passing; 99% line coverage** across 9 test modules in `backend/tests/market/`
(models, cache, simulator, simulator source, Massive source, Massive parsing, factory/config,
SSE stream, tracking). Massive tests build snapshots with the SDK's real `TickerSnapshot.from_dict`
instead of `MagicMock`, which had hidden the v1 timestamp bug.

## v2 Changes (from `MARKET_DATA_DESIGN.md`)

1. **Massive timestamp bug fixed** — v1 read `last_trade.timestamp`, which the SDK doesn't have; every real snapshot was skipped. Now `sip_timestamp` (ns) with fallback to `updated`, unit auto-detected.
2. **Massive price fallback** — `last_trade.price → min.close → day.close → prev_day.close`.
3. **Daily change** — `PriceUpdate.session_open`, `day_change`, `day_change_percent` (simulator: seed price; Massive: previous close).
4. **Massive scheduling** — exponential backoff on errors (cap 120s), early poll on `add_ticker` respecting a 12s free-tier gap, auth errors logged once, short client timeouts.
5. **Remove race fixed** — in-flight polls cannot re-add a removed ticker.
6. **`normalize_ticker()`** applied at every entry point; invalid symbols raise `ValueError`.
7. **SSE** — fresh router per call, `: ping` heartbeat every 15s idle, removal pushes an update.
8. **Config** — `MarketConfig.from_env()`: `MASSIVE_POLL_INTERVAL`, `MARKET_SIM_INTERVAL`, `MARKET_SIM_SEED`.
9. **Simulator** — injectable RNG (deterministic tests), `add_ticker` before `start` works, fixed-rate loop.

## Code Review & Fixes Applied

A comprehensive code review identified 7 issues. All were resolved:

1. **pyproject.toml build config** — added `[tool.hatch.build.targets.wheel] packages = ["app"]`
2. **Lazy imports removed** — `massive` is a core dependency; imports moved to top level
3. **SSE return type fixed** — `_generate_events` annotated as `AsyncGenerator[str, None]`
4. **Public `get_tickers()`** — added to `GBMSimulator` to avoid private attribute access
5. **Correlation constants cleaned up** — removed unused `DEFAULT_CORR`, consolidated into `CROSS_GROUP_CORR`
6. **Unused test imports removed** — `pytest`, `math`, `asyncio` cleaned from 4 test files
7. **Massive test mocks fixed** — `source._client` set in tests, patches target correct names

## Demo

A Rich terminal demo is available at `backend/market_data_demo.py`:

```bash
cd backend
uv run market_data_demo.py
```

Displays a live-updating dashboard with all 10 tickers, sparklines, color-coded direction arrows, and an event log for notable price moves. Runs 60 seconds or until Ctrl+C.

## Usage for Downstream Code

```python
from app.market import PriceCache, create_market_data_source, sync_tracked_tickers

# Startup
cache = PriceCache()
source = create_market_data_source(cache)  # Reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT", ...])

# Read prices
update = cache.get("AAPL")          # PriceUpdate or None
price = cache.get_price("AAPL")     # float or None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

# Dynamic watchlist
await sync_tracked_tickers(source, watchlist | position_tickers)
# or directly: await source.add_ticker("TSLA"); await source.remove_ticker("GOOGL")

# Shutdown
await source.stop()
```
