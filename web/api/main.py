"""market-stream demo API — read-only, rate-limited, CORS-locked.

Serves the recruiter demo from the Iceberg tables via Trino. There are NO write routes.
Endpoints:
  GET /healthz             — 200 only when Trino is reachable (the frontend's live probe)
  GET /api/symbols         — the allowed symbols
  GET /api/ohlcv?symbol&limit — 1-minute OHLCV/VWAP rows (chronological)
  GET /api/ticks?symbol&limit — recent raw ticks (newest first)
"""

from __future__ import annotations

import os
import time

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

import trino_client

SYMBOLS = [
    s.strip().upper()
    for s in os.environ.get("SYMBOLS", "AAPL,GOOG,MSFT").split(",")
    if s.strip()
]
CORS_ALLOW_ORIGIN = os.environ.get("CORS_ALLOW_ORIGIN", "http://localhost:8001")
RATE_LIMIT = os.environ.get("API_RATE_LIMIT", "30/minute")
MAX_LIMIT = 500
HEALTH_TTL_S = 5.0

limiter = Limiter(key_func=get_remote_address, default_limits=[RATE_LIMIT])
app = FastAPI(title="market-stream demo API", version="0.1.0")
app.state.limiter = limiter
app.add_exception_handler(
    RateLimitExceeded,
    lambda request, exc: JSONResponse(status_code=429, content={"detail": "rate limit exceeded"}),
)
# CORS locked to the single static-site origin; only GET is allowed.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in CORS_ALLOW_ORIGIN.split(",") if o.strip()],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── tiny TTL caches so a refresh-storm can't hammer Trino ───────────────────────
_cache: dict = {}
_health = {"t": 0.0, "ok": False}


def _cached(key, ttl, fn):
    now = time.monotonic()
    hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    val = fn()
    _cache[key] = (now, val)
    return val


def _is_healthy() -> bool:
    now = time.monotonic()
    if now - _health["t"] > HEALTH_TTL_S:
        _health["ok"] = trino_client.healthy()
        _health["t"] = now
    return _health["ok"]


def _validate_symbol(symbol: str) -> str:
    s = symbol.upper()
    if s not in SYMBOLS:
        raise HTTPException(status_code=400, detail=f"unknown symbol; allowed: {SYMBOLS}")
    return s


@app.get("/healthz")
def healthz():
    if _is_healthy():
        return {"status": "ok", "live": True, "symbols": SYMBOLS}
    raise HTTPException(status_code=503, detail="data backend not ready")


@app.get("/api/symbols")
@limiter.limit(RATE_LIMIT)
def symbols(request: Request):
    return {"symbols": SYMBOLS}


@app.get("/api/ohlcv")
@limiter.limit(RATE_LIMIT)
def ohlcv(
    request: Request,
    symbol: str = Query("AAPL"),
    limit: int = Query(120, ge=1, le=MAX_LIMIT),
):
    sym = _validate_symbol(symbol)

    def run():
        rows = trino_client.query(
            f"""
            SELECT symbol,
                   to_unixtime(window_start) * 1000 AS window_start_ms,
                   CAST(open AS double) AS open, CAST(high AS double) AS high,
                   CAST(low AS double) AS low, CAST(close AS double) AS close,
                   CAST(volume AS double) AS volume, CAST(vwap AS double) AS vwap,
                   trade_count
            FROM market.ohlcv_1m
            WHERE symbol = ?
            ORDER BY window_start DESC
            LIMIT {int(limit)}
            """,
            [sym],
        )
        rows.reverse()  # chronological for the chart
        return rows

    try:
        return {"symbol": sym, "rows": _cached(("ohlcv", sym, limit), 3.0, run)}
    except HTTPException:
        raise
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=503, detail="query failed")


@app.get("/api/ticks")
@limiter.limit(RATE_LIMIT)
def ticks(
    request: Request,
    symbol: str = Query("AAPL"),
    limit: int = Query(40, ge=1, le=MAX_LIMIT),
):
    sym = _validate_symbol(symbol)

    def run():
        return trino_client.query(
            f"""
            SELECT symbol,
                   CAST(price AS double) AS price,
                   CAST(volume AS double) AS quantity,
                   to_unixtime(quote_time) * 1000 AS trade_time_ms
            FROM market.quotes_raw
            WHERE symbol = ?
            ORDER BY quote_time DESC
            LIMIT {int(limit)}
            """,
            [sym],
        )

    try:
        return {"symbol": sym, "rows": _cached(("ticks", sym, limit), 2.0, run)}
    except HTTPException:
        raise
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=503, detail="query failed")
