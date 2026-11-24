"""Tests for the SSE streaming endpoint."""

import asyncio
import json

import pytest
from fastapi import FastAPI

from app.market.cache import PriceCache
from app.market.stream import (
    create_stream_router,
    format_prices_event,
    generate_price_events,
)


class FakeRequest:
    """Stands in for starlette's Request: disconnects after N checks."""

    client = None

    def __init__(self, polls: int):
        self._left = polls

    async def is_disconnected(self) -> bool:
        self._left -= 1
        return self._left < 0


def parse_data(event: str) -> dict:
    assert event.startswith("data: ") and event.endswith("\n\n")
    return json.loads(event.removeprefix("data: "))


async def collect(cache, polls, **kwargs) -> list[str]:
    kwargs.setdefault("interval", 0.001)
    return [e async for e in generate_price_events(cache, FakeRequest(polls), **kwargs)]


@pytest.mark.asyncio
class TestGeneratePriceEvents:
    async def test_retry_then_snapshot(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)

        events = await collect(cache, polls=3)

        assert events[0] == "retry: 1000\n\n"
        data = parse_data(events[1])
        assert data["AAPL"]["price"] == 190.0
        assert data["AAPL"]["direction"] == "flat"

    async def test_no_duplicate_push_without_changes(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)

        events = await collect(cache, polls=5)

        assert len(events) == 2  # retry + one snapshot

    async def test_pushes_again_after_change(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        request = FakeRequest(polls=50)
        gen = generate_price_events(cache, request, interval=0.001)

        assert (await anext(gen)).startswith("retry")
        first = parse_data(await anext(gen))
        cache.update("AAPL", 191.0)
        second = parse_data(await anext(gen))
        await gen.aclose()

        assert first["AAPL"]["price"] == 190.0
        assert second["AAPL"]["price"] == 191.0
        assert second["AAPL"]["direction"] == "up"

    async def test_removal_pushes_snapshot_without_ticker(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        cache.update("GOOGL", 175.0)
        gen = generate_price_events(cache, FakeRequest(polls=50), interval=0.001)

        await anext(gen)  # retry
        await anext(gen)  # initial snapshot
        cache.remove("GOOGL")
        data = parse_data(await anext(gen))
        await gen.aclose()

        assert set(data) == {"AAPL"}

    async def test_heartbeat_when_idle(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)

        events = await collect(cache, polls=5, heartbeat=0.0)

        assert ": ping\n\n" in events

    async def test_empty_cache_sends_empty_snapshot(self):
        events = await collect(PriceCache(), polls=2)
        assert parse_data(events[1]) == {}

    async def test_stops_on_disconnect(self):
        events = await collect(PriceCache(), polls=0)
        assert events == ["retry: 1000\n\n"]

    async def test_cancellation_is_clean(self):
        cache = PriceCache()
        gen = generate_price_events(cache, FakeRequest(polls=10_000), interval=0.01)

        async def consume():
            async for _ in gen:
                pass

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.03)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert task.done()


class TestFormatAndRouter:
    def test_format_prices_event(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        event = format_prices_event(cache.get_all())
        assert parse_data(event)["AAPL"]["ticker"] == "AAPL"
        assert " " not in event.removeprefix("data: ")  # compact JSON

    def test_router_has_prices_route(self):
        router = create_stream_router(PriceCache())
        assert [r.path for r in router.routes] == ["/api/stream/prices"]

    def test_router_factory_is_reentrant(self):
        r1 = create_stream_router(PriceCache())
        r2 = create_stream_router(PriceCache())
        assert len(r1.routes) == len(r2.routes) == 1

    def test_mounts_on_app(self):
        app = FastAPI()
        app.include_router(create_stream_router(PriceCache()))
        assert "/api/stream/prices" in {r.path for r in app.routes}


@pytest.mark.asyncio
async def test_endpoint_over_asgi():
    """Drive the real endpoint through the ASGI interface until the first snapshot."""
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    app = FastAPI()
    app.include_router(create_stream_router(cache))

    sent: list[dict] = []
    first_body = asyncio.Event()
    disconnect = asyncio.Event()

    async def receive():
        await disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)
        if message["type"] == "http.response.body" and b"data:" in message.get("body", b""):
            first_body.set()

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/api/stream/prices",
        "raw_path": b"/api/stream/prices",
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80),
    }
    task = asyncio.create_task(app(scope, receive, send))
    await asyncio.wait_for(first_body.wait(), timeout=5)
    disconnect.set()
    await asyncio.wait_for(task, timeout=5)

    start = sent[0]
    assert start["status"] == 200
    headers = dict(start["headers"])
    assert headers[b"content-type"].startswith(b"text/event-stream")
    assert headers[b"cache-control"] == b"no-cache"
    body = b"".join(m.get("body", b"") for m in sent[1:]).decode()
    assert "retry: 1000" in body
    assert '"AAPL"' in body
