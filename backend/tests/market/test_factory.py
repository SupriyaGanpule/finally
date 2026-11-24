"""Tests for market data source factory."""

import os
from unittest.mock import patch

from app.market.cache import PriceCache
from app.market.factory import MarketConfig, create_market_data_source
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


class TestFactory:
    """Tests for create_market_data_source factory."""

    def test_creates_simulator_when_no_api_key(self):
        """Test that simulator is created when MASSIVE_API_KEY is not set."""
        cache = PriceCache()

        with patch.dict(os.environ, {}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)

    def test_creates_simulator_when_api_key_empty(self):
        """Test that simulator is created when MASSIVE_API_KEY is empty."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": ""}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)

    def test_creates_simulator_when_api_key_whitespace(self):
        """Test that simulator is created when MASSIVE_API_KEY is whitespace."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "   "}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)

    def test_creates_massive_when_api_key_set(self):
        """Test that Massive client is created when MASSIVE_API_KEY is set."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, MassiveDataSource)

    def test_massive_receives_api_key(self):
        """Test that Massive client receives the API key."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key-123"}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, MassiveDataSource)
        assert source._api_key == "test-key-123"

    def test_simulator_receives_cache(self):
        """Test that simulator receives the cache reference."""
        cache = PriceCache()

        with patch.dict(os.environ, {}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)
        assert source._cache is cache

    def test_massive_receives_cache(self):
        """Test that Massive client receives the cache reference."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, MassiveDataSource)
        assert source._cache is cache


class TestMarketConfig:
    """Tests for environment-driven configuration."""

    def test_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            cfg = MarketConfig.from_env()
        assert cfg == MarketConfig()
        assert cfg.massive_poll_interval == 15.0
        assert cfg.sim_update_interval == 0.5
        assert cfg.sim_seed is None

    def test_reads_env(self):
        env = {
            "MASSIVE_API_KEY": " key ",
            "MASSIVE_POLL_INTERVAL": "5",
            "MARKET_SIM_INTERVAL": "0.25",
            "MARKET_SIM_SEED": "42",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = MarketConfig.from_env()
        assert cfg.massive_api_key == "key"
        assert cfg.massive_poll_interval == 5.0
        assert cfg.sim_update_interval == 0.25
        assert cfg.sim_seed == 42

    def test_poll_interval_floor(self):
        with patch.dict(os.environ, {"MASSIVE_POLL_INTERVAL": "0.5"}, clear=True):
            assert MarketConfig.from_env().massive_poll_interval == 2.0

    def test_invalid_values_fall_back(self):
        env = {
            "MASSIVE_POLL_INTERVAL": "fast",
            "MARKET_SIM_INTERVAL": "-1",
            "MARKET_SIM_SEED": "abc",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = MarketConfig.from_env()
        assert cfg.massive_poll_interval == 15.0
        assert cfg.sim_update_interval == 0.5
        assert cfg.sim_seed is None

    def test_explicit_config_overrides_env(self):
        cache = PriceCache()
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "env-key"}, clear=True):
            source = create_market_data_source(cache, MarketConfig())
        assert isinstance(source, SimulatorDataSource)

    def test_poll_interval_passed_to_massive(self):
        cfg = MarketConfig(massive_api_key="k", massive_poll_interval=3.0)
        source = create_market_data_source(PriceCache(), cfg)
        assert isinstance(source, MassiveDataSource)
        assert source._interval == 3.0

    def test_sim_settings_passed_to_simulator(self):
        cfg = MarketConfig(sim_update_interval=0.1, sim_seed=7)
        source = create_market_data_source(PriceCache(), cfg)
        assert isinstance(source, SimulatorDataSource)
        assert source._interval == 0.1
