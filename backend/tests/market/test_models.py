"""Tests for PriceUpdate dataclass."""

import pytest

from app.market.models import PriceUpdate, normalize_ticker


class TestPriceUpdate:
    """Unit tests for the PriceUpdate model."""

    def test_price_update_creation(self):
        """Test basic PriceUpdate creation."""
        update = PriceUpdate(
            ticker="AAPL", price=190.50, previous_price=190.00, timestamp=1234567890.0
        )
        assert update.ticker == "AAPL"
        assert update.price == 190.50
        assert update.previous_price == 190.00
        assert update.timestamp == 1234567890.0

    def test_change_calculation(self):
        """Test price change calculation."""
        update = PriceUpdate(
            ticker="AAPL", price=190.50, previous_price=190.00, timestamp=1234567890.0
        )
        assert update.change == 0.50

    def test_change_negative(self):
        """Test negative price change."""
        update = PriceUpdate(
            ticker="AAPL", price=189.50, previous_price=190.00, timestamp=1234567890.0
        )
        assert update.change == -0.50

    def test_change_percent_up(self):
        """Test percentage change calculation (up)."""
        update = PriceUpdate(
            ticker="AAPL", price=190.00, previous_price=100.00, timestamp=1234567890.0
        )
        assert update.change_percent == 90.0

    def test_change_percent_down(self):
        """Test percentage change calculation (down)."""
        update = PriceUpdate(
            ticker="AAPL", price=100.00, previous_price=200.00, timestamp=1234567890.0
        )
        assert update.change_percent == -50.0

    def test_change_percent_zero_previous(self):
        """Test percentage change with zero previous price."""
        update = PriceUpdate(
            ticker="AAPL", price=100.00, previous_price=0.00, timestamp=1234567890.0
        )
        assert update.change_percent == 0.0

    def test_direction_up(self):
        """Test direction calculation (up)."""
        update = PriceUpdate(
            ticker="AAPL", price=191.00, previous_price=190.00, timestamp=1234567890.0
        )
        assert update.direction == "up"

    def test_direction_down(self):
        """Test direction calculation (down)."""
        update = PriceUpdate(
            ticker="AAPL", price=189.00, previous_price=190.00, timestamp=1234567890.0
        )
        assert update.direction == "down"

    def test_direction_flat(self):
        """Test direction calculation (flat)."""
        update = PriceUpdate(
            ticker="AAPL", price=190.00, previous_price=190.00, timestamp=1234567890.0
        )
        assert update.direction == "flat"

    def test_to_dict(self):
        """Test serialization to dictionary."""
        update = PriceUpdate(
            ticker="AAPL", price=190.50, previous_price=190.00, timestamp=1234567890.0
        )
        result = update.to_dict()

        assert result["ticker"] == "AAPL"
        assert result["price"] == 190.50
        assert result["previous_price"] == 190.00
        assert result["timestamp"] == 1234567890.0
        assert result["change"] == 0.50
        assert result["change_percent"] == 0.2632  # (0.50 / 190.00) * 100
        assert result["direction"] == "up"

    def test_immutability(self):
        """Test that PriceUpdate is immutable."""
        update = PriceUpdate(
            ticker="AAPL", price=190.50, previous_price=190.00, timestamp=1234567890.0
        )

        with pytest.raises(AttributeError):
            update.price = 200.00  # Should raise error


class TestDayChange:
    """Tests for session_open-based daily change."""

    def test_day_change_with_session_open(self):
        update = PriceUpdate("AAPL", price=191.0, previous_price=190.9, session_open=190.0)
        assert update.day_change == 1.0
        assert update.day_change_percent == 0.5263

    def test_day_change_negative(self):
        update = PriceUpdate("AAPL", price=95.0, previous_price=96.0, session_open=100.0)
        assert update.day_change == -5.0
        assert update.day_change_percent == -5.0

    def test_day_change_without_session_open(self):
        update = PriceUpdate("AAPL", price=191.0, previous_price=190.0)
        assert update.session_open is None
        assert update.day_change == 0.0
        assert update.day_change_percent == 0.0

    def test_day_change_zero_session_open(self):
        update = PriceUpdate("AAPL", price=1.0, previous_price=1.0, session_open=0.0)
        assert update.day_change_percent == 0.0

    def test_to_dict_includes_day_fields(self):
        update = PriceUpdate("AAPL", 191.0, 190.0, timestamp=1.0, session_open=190.0)
        result = update.to_dict()
        assert result["session_open"] == 190.0
        assert result["day_change"] == 1.0
        assert result["day_change_percent"] == 0.5263

    def test_to_dict_is_json_serializable(self):
        import json

        update = PriceUpdate("AAPL", 191.0, 190.0, timestamp=1.0)
        assert json.loads(json.dumps(update.to_dict()))["session_open"] is None


class TestNormalizeTicker:
    """Tests for normalize_ticker."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("AAPL", "AAPL"),
            ("aapl", "AAPL"),
            ("  msft\t", "MSFT"),
            ("brk.b", "BRK.B"),
            ("BF-B", "BF-B"),
            ("V", "V"),
            ("ABCDEFGHIJ", "ABCDEFGHIJ"),
        ],
    )
    def test_valid(self, raw, expected):
        assert normalize_ticker(raw) == expected

    @pytest.mark.parametrize(
        "raw", ["", "   ", "$$$", "1ABC", "AA PL", "ABCDEFGHIJK", ".A", "AAPL;DROP", None]
    )
    def test_invalid(self, raw):
        with pytest.raises(ValueError):
            normalize_ticker(raw)
