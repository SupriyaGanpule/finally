"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)

MIN_MASSIVE_POLL_INTERVAL = 2.0


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning("Ignoring invalid %s=%r; using %s", name, raw, default)
        return default
    if value <= 0:
        logger.warning("Ignoring non-positive %s=%r; using %s", name, raw, default)
        return default
    return value


def _env_int(name: str) -> int | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning("Ignoring invalid %s=%r", name, raw)
        return None


@dataclass(frozen=True)
class MarketConfig:
    """Market data configuration, normally read from the environment."""

    massive_api_key: str = ""
    massive_poll_interval: float = 15.0
    sim_update_interval: float = 0.5
    sim_seed: int | None = None

    @classmethod
    def from_env(cls) -> MarketConfig:
        return cls(
            massive_api_key=os.environ.get("MASSIVE_API_KEY", "").strip(),
            massive_poll_interval=max(
                MIN_MASSIVE_POLL_INTERVAL, _env_float("MASSIVE_POLL_INTERVAL", 15.0)
            ),
            sim_update_interval=_env_float("MARKET_SIM_INTERVAL", 0.5),
            sim_seed=_env_int("MARKET_SIM_SEED"),
        )


def create_market_data_source(
    price_cache: PriceCache, config: MarketConfig | None = None
) -> MarketDataSource:
    """Create the market data source selected by configuration.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    cfg = config if config is not None else MarketConfig.from_env()

    if cfg.massive_api_key:
        logger.info("Market data source: Massive API (poll every %.1fs)", cfg.massive_poll_interval)
        return MassiveDataSource(
            api_key=cfg.massive_api_key,
            price_cache=price_cache,
            poll_interval=cfg.massive_poll_interval,
        )

    logger.info("Market data source: GBM Simulator")
    return SimulatorDataSource(
        price_cache=price_cache,
        update_interval=cfg.sim_update_interval,
        seed=cfg.sim_seed,
    )
