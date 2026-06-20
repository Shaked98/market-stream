"""Binance combined-stream WebSocket client.

Yields raw Binance `@trade` event dicts. Resilient by design: a dropped connection is
reconnected with exponential backoff + jitter (capped). Trade streams are stateless, so
a gap is acceptable — it is logged, not back-filled. The `websockets` library answers
the exchange's ping frames automatically, satisfying Binance's keepalive requirement.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import AsyncIterator

import websockets

from .config import Settings

log = logging.getLogger("market_producer.ws")


def build_stream_url(base: str, symbols: list[str]) -> str:
    """Compose a Binance combined-stream URL, e.g.
    wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
    """
    streams = "/".join(f"{s.lower()}@trade" for s in symbols)
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}streams={streams}"


def compute_backoff(attempt: int, base: float = 1.0, cap: float = 30.0) -> float:
    """Exponential backoff, capped. Pure (jitter is added by the caller) so it is
    deterministic under test."""
    return min(cap, base * (2 ** max(0, attempt)))


async def stream_trades(
    settings: Settings, stop: asyncio.Event | None = None
) -> AsyncIterator[dict]:
    """Async generator of raw Binance trade events, reconnecting forever until `stop`."""
    url = build_stream_url(settings.ws_url, settings.symbol_list)
    attempt = 0
    while stop is None or not stop.is_set():
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=60, max_queue=1024
            ) as ws:
                log.info("connected: %s", url)
                attempt = 0  # reset backoff on a healthy connection
                async for raw in ws:
                    if stop is not None and stop.is_set():
                        break
                    msg = json.loads(raw)
                    # Combined streams wrap the payload as {"stream": ..., "data": {...}}.
                    data = msg.get("data", msg)
                    if data.get("e") == "trade":
                        yield data
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — any WS/parse error → reconnect
            delay = compute_backoff(attempt, cap=settings.reconnect_max_backoff_seconds)
            delay += random.uniform(0, min(1.0, delay))  # jitter
            log.warning("ws disconnected (%s); reconnecting in %.1fs", exc, delay)
            await asyncio.sleep(delay)
            attempt += 1
