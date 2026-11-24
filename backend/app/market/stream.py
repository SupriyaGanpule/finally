"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache
from .models import PriceUpdate

logger = logging.getLogger(__name__)

PUSH_INTERVAL = 0.5  # seconds between cache checks
HEARTBEAT_INTERVAL = 15.0  # idle keep-alive comment period
RETRY_MS = 1000  # EventSource reconnect delay


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE router bound to `price_cache`.

    A fresh APIRouter per call, so calling this more than once (e.g. in
    tests) never registers duplicate routes.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint: a full price snapshot whenever prices change.

        data: {"AAPL": {"ticker": "AAPL", "price": 190.5, ...}, ...}
        """
        return StreamingResponse(
            generate_price_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # disable nginx buffering if proxied
            },
        )

    return router


def format_prices_event(prices: dict[str, PriceUpdate]) -> str:
    """Serialize a price snapshot as one SSE `data:` event."""
    payload = {ticker: update.to_dict() for ticker, update in prices.items()}
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"


async def generate_price_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = PUSH_INTERVAL,
    heartbeat: float = HEARTBEAT_INTERVAL,
) -> AsyncGenerator[str, None]:
    """Yield a full snapshot whenever the cache version changes.

    Sends a `: ping` comment when idle for `heartbeat` seconds so proxies
    keep the connection open. Stops when the client disconnects.
    """
    yield f"retry: {RETRY_MS}\n\n"

    loop = asyncio.get_running_loop()
    last_version = -1
    last_sent = loop.time()
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)

    try:
        while not await request.is_disconnected():
            version, prices = price_cache.snapshot()
            if version != last_version:
                last_version = version
                yield format_prices_event(prices)  # may be {} after the last removal
                last_sent = loop.time()
            elif loop.time() - last_sent >= heartbeat:
                yield ": ping\n\n"  # SSE comment; ignored by EventSource
                last_sent = loop.time()
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("SSE client disconnected: %s", client)
