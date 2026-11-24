"""Tests for Massive snapshot parsing against the real SDK models."""

import time

import pytest
from massive.rest.models import TickerSnapshot

from app.market.massive_client import parse_snapshot, to_unix_seconds


def snap(**data) -> TickerSnapshot:
    return TickerSnapshot.from_dict({"ticker": "AAPL", **data})


class TestToUnixSeconds:
    @pytest.mark.parametrize(
        "raw",
        [1_700_000_000, 1_700_000_000_000, 1_700_000_000_000_000, 1_700_000_000_000_000_000],
        ids=["seconds", "millis", "micros", "nanos"],
    )
    def test_units_detected(self, raw):
        assert to_unix_seconds(raw) == pytest.approx(1_700_000_000)

    @pytest.mark.parametrize("raw", [None, 0, -5, "123", True, object()])
    def test_invalid_returns_none(self, raw):
        assert to_unix_seconds(raw) is None


class TestParseSnapshot:
    def test_full_snapshot(self):
        quote = parse_snapshot(
            snap(
                lastTrade={"p": 190.52, "s": 100, "t": 1_707_580_800_123_456_789},
                day={"o": 189.10, "c": 190.40},
                prevDay={"c": 188.00},
                updated=1_707_580_800_200_000_000,
            )
        )
        assert quote.ticker == "AAPL"
        assert quote.price == 190.52
        assert quote.timestamp == pytest.approx(1_707_580_800.1234567)
        assert quote.session_open == 188.0

    def test_last_trade_has_no_timestamp_attribute(self):
        """Documents the SDK shape that broke v1: use sip_timestamp, not timestamp."""
        s = snap(lastTrade={"p": 1.0, "t": 1})
        assert not hasattr(s.last_trade, "timestamp")
        assert hasattr(s.last_trade, "sip_timestamp")

    def test_price_falls_back_to_minute_bar(self):
        quote = parse_snapshot(snap(min={"c": 190.1}, day={"c": 189.0}))
        assert quote.price == 190.1

    def test_price_falls_back_to_day_close(self):
        quote = parse_snapshot(snap(day={"o": 189.0, "c": 189.9}))
        assert quote.price == 189.9
        assert quote.session_open == 189.0  # no prev_day → day open

    def test_price_falls_back_to_prev_close(self):
        quote = parse_snapshot(snap(prevDay={"c": 187.5}))
        assert quote.price == 187.5

    def test_zero_trade_price_ignored(self):
        quote = parse_snapshot(snap(lastTrade={"p": 0}, day={"c": 189.9}))
        assert quote.price == 189.9

    def test_timestamp_falls_back_to_updated(self):
        quote = parse_snapshot(snap(day={"c": 1.0}, updated=1_707_580_800_000_000_000))
        assert quote.timestamp == pytest.approx(1_707_580_800.0)

    def test_timestamp_falls_back_to_now(self):
        before = time.time()
        quote = parse_snapshot(snap(day={"c": 1.0}))
        assert before <= quote.timestamp <= time.time()

    def test_ticker_upper_cased(self):
        quote = parse_snapshot(TickerSnapshot.from_dict({"ticker": "aapl", "day": {"c": 1.0}}))
        assert quote.ticker == "AAPL"

    def test_no_price_returns_none(self):
        assert parse_snapshot(snap()) is None

    def test_no_ticker_returns_none(self):
        assert parse_snapshot(TickerSnapshot.from_dict({"day": {"c": 1.0}})) is None

    def test_arbitrary_object_returns_none(self):
        assert parse_snapshot(object()) is None
