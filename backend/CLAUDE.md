# Backend — Developer Guide

## Project Setup

```bash
cd backend
uv sync --extra dev   # Install all dependencies including test/lint tools
```

## Market Data API

The market data subsystem lives in `app/market/`. Design: `planning/MARKET_DATA_DESIGN.md`. Import only from the package:

```python
from app.market import (
    PriceCache, PriceUpdate, MarketDataSource, MarketConfig,
    create_market_data_source, create_stream_router,
    normalize_ticker, sync_tracked_tickers,
)
```

### Core Types

- **`PriceUpdate`** — Immutable dataclass: `ticker`, `price`, `previous_price`, `timestamp`, `session_open`, plus properties `change`, `change_percent`, `direction` ("up"/"down"/"flat") for the last tick, `day_change`, `day_change_percent` vs. `session_open`, and `to_dict()` for JSON.

- **`PriceCache`** — Thread-safe in-memory store. Key methods:
  - `update(ticker, price, timestamp=None, session_open=None) -> PriceUpdate`
  - `get(ticker) -> PriceUpdate | None`, `get_price(ticker) -> float | None`
  - `get_all() -> dict[str, PriceUpdate]`, `snapshot() -> (version, prices)`
  - `remove(ticker)` (bumps version)
  - `version` property — monotonic counter for SSE change detection

- **`MarketDataSource`** — Abstract interface implemented by `SimulatorDataSource` and `MassiveDataSource`. Lifecycle: `start(tickers)` -> `add_ticker()` / `remove_ticker()` -> `stop()`. `name` is `"simulator"` or `"massive"`. Tickers are normalized; invalid ones raise `ValueError`.

- **`create_market_data_source(cache, config=None)`** — Factory. Returns `MassiveDataSource` if `MASSIVE_API_KEY` is set, otherwise `SimulatorDataSource`. Config comes from `MarketConfig.from_env()` unless passed.

- **`sync_tracked_tickers(source, desired)`** — Call after watchlist changes and trades with `watchlist ∪ open positions`, so held tickers always have prices.

- **`normalize_ticker(raw)`** — Strip + upper-case + validate. Use at API boundaries.

### Environment Variables

| Var | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | empty | Non-empty → real data via Massive; empty → simulator |
| `MASSIVE_POLL_INTERVAL` | `15` | Seconds between Massive polls (min 2) |
| `MARKET_SIM_INTERVAL` | `0.5` | Simulator tick period |
| `MARKET_SIM_SEED` | unset | Integer → deterministic simulator (tests) |

### SSE Streaming

```python
router = create_stream_router(price_cache)  # fresh FastAPI APIRouter per call
# Endpoint: GET /api/stream/prices (text/event-stream)
# Event: data: {"AAPL": {...PriceUpdate.to_dict()...}, ...}  on every change
# Idle keep-alive: ": ping" comment every 15s
```

### Seed Data

Default tickers: AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX. Seed prices and per-ticker volatility/drift params are in `app/market/seed_prices.py`.

## Running Tests

```bash
uv run --extra dev pytest -v              # All tests
uv run --extra dev pytest --cov=app       # With coverage
uv run --extra dev ruff check app/ tests/ # Lint
```

## Demo

```bash
uv run market_data_demo.py   # Live terminal dashboard with simulated prices
```
