"""Thin, read-only Trino client over the Iceberg tables. No mutations, ever — the API
only ever issues SELECTs, and the Trino user is expected to be read-only on the cluster."""

from __future__ import annotations

import os

from trino.dbapi import connect

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "market-stream-web")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "market")
REQUEST_TIMEOUT = float(os.environ.get("TRINO_TIMEOUT", "5.0"))


def _connect():
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        request_timeout=REQUEST_TIMEOUT,
    )


def query(sql: str, params: list | None = None) -> list[dict]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def healthy() -> bool:
    """Cheap reachability probe — backs the /healthz endpoint that the frontend uses to
    decide live-vs-sample."""
    try:
        query("SELECT 1")
        return True
    except Exception:  # noqa: BLE001 — any failure means "not live", fall back to sample
        return False
