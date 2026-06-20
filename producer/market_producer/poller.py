"""Yahoo Finance quote poller (free, no API key).

Stock data is polled, not streamed: every `poll_interval_seconds` we fetch each symbol's
latest price + cumulative day volume from Yahoo's public chart endpoint and yield one raw
quote per symbol. `volume` is the day-volume *increment* since the previous poll (so VWAP
is weighted by actually-traded volume regardless of poll rate). Resilient by design: a
failed fetch backs off (exponential + cap) and is skipped, never fatal.

Markets close nights/weekends; off-hours the price is static and the increment is 0 — the
web demo's sample-feed fallback keeps the page alive in that case.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from collections.abc import Iterator

from .config import Settings

log = logging.getLogger("market_producer.poller")
_HEADERS = {"User-Agent": "Mozilla/5.0 (market-stream demo; +https://github.com/Shaked98)"}


def quote_url(template: str, symbol: str) -> str:
    return template.format(symbol=symbol)


def compute_backoff(attempt: int, base: float = 1.0, cap: float = 30.0) -> float:
    """Exponential backoff, capped. Pure (no jitter) so it's deterministic under test."""
    return min(cap, base * (2 ** max(0, attempt)))


def fetch_quote(template: str, symbol: str, timeout: float = 10.0) -> dict:
    """Fetch one symbol; return {symbol, price, day_volume}. Raises on any failure."""
    req = urllib.request.Request(quote_url(template, symbol), headers=_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 — fixed https host
        payload = json.loads(resp.read())
    result = payload["chart"]["result"][0]
    meta = result.get("meta", {})
    quote = (result.get("indicators", {}).get("quote") or [{}])[0]

    price = meta.get("regularMarketPrice")
    if price is None:  # fall back to the last non-null minute close
        closes = [c for c in (quote.get("close") or []) if c is not None]
        price = closes[-1] if closes else None
    if price is None:
        raise ValueError(f"no price in Yahoo response for {symbol}")

    day_volume = int(sum(v for v in (quote.get("volume") or []) if v))
    return {"symbol": symbol, "price": price, "day_volume": day_volume}


def _interruptible_sleep(seconds: float, stop) -> None:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if stop is not None and stop.is_set():
            return
        time.sleep(min(0.5, max(0.0, end - time.monotonic())))


def iter_quotes(settings: Settings, stop=None) -> Iterator[dict]:
    """Yield raw quotes forever (until `stop`), polling each symbol every interval."""
    prev_day_volume: dict[str, int] = {}
    attempt = 0
    while stop is None or not stop.is_set():
        for symbol in settings.symbol_list:
            if stop is not None and stop.is_set():
                break
            try:
                q = fetch_quote(settings.quote_url, symbol, settings.http_timeout_seconds)
                attempt = 0
            except Exception as exc:  # noqa: BLE001 — any fetch error → back off, skip
                delay = compute_backoff(attempt, cap=settings.reconnect_max_backoff_seconds)
                log.warning("quote fetch failed for %s (%s); backing off %.1fs", symbol, exc, delay)
                _interruptible_sleep(delay, stop)
                attempt += 1
                continue

            cum = q["day_volume"]
            prev = prev_day_volume.get(symbol)
            increment = max(0, cum - prev) if prev is not None else 0
            prev_day_volume[symbol] = cum
            yield {
                "symbol": symbol,
                "price": q["price"],
                "volume": increment,
                "day_volume": cum,
                "quote_time_ms": int(time.time() * 1000),
            }
        _interruptible_sleep(settings.poll_interval_seconds, stop)
