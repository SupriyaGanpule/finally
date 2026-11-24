"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push PriceUpdates into a shared PriceCache on their own
    schedule; they never return prices to callers.

    Contract (both implementations satisfy it):
      * Tickers are normalized with normalize_ticker() at every entry point.
      * start() is called once; a second call raises RuntimeError.
      * add_ticker()/remove_ticker() are idempotent and may be called before
        start() (they edit the tracked set).
      * remove_ticker() also evicts the ticker from the PriceCache, and no
        later write for that ticker reaches the cache.
      * stop() is idempotent; after it returns the source never writes again.
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
