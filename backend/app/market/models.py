"""Data models for market data."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field

# 1-10 chars, starts with a letter; allows class shares like BRK.B / BF-B
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def normalize_ticker(raw: str) -> str:
    """Return the canonical ticker form used everywhere in the backend.

    Strips whitespace and upper-cases. Raises ValueError for malformed symbols.

    >>> normalize_ticker("  aapl ")
    'AAPL'
    >>> normalize_ticker("brk.b")
    'BRK.B'
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
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float  # price at the previous update
    timestamp: float = field(default_factory=time.time)  # Unix seconds
    session_open: float | None = None  # baseline for "day change"

    # --- tick-to-tick (drives the flash animation) ---

    @property
    def change(self) -> float:
        """Absolute price change from the previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from the previous update."""
        return _pct(self.price - self.previous_price, self.previous_price)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    # --- vs. session baseline (drives the "daily change %" column) ---

    @property
    def day_change(self) -> float:
        """Absolute change since the session open (0.0 if no baseline)."""
        if self.session_open is None:
            return 0.0
        return round(self.price - self.session_open, 4)

    @property
    def day_change_percent(self) -> float:
        """Percentage change since the session open (0.0 if no baseline)."""
        if self.session_open is None:
            return 0.0
        return _pct(self.price - self.session_open, self.session_open)

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
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
