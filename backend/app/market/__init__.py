"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate               - Immutable price snapshot dataclass
    PriceCache                - Thread-safe in-memory price store
    MarketDataSource          - Abstract interface for data providers
    MarketConfig              - Market data configuration (from env)
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router      - FastAPI router factory for the SSE endpoint
    normalize_ticker          - Canonical ticker form / validation
    sync_tracked_tickers      - Reconcile the source's tracked set
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
