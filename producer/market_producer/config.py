"""Typed, 12-factor configuration. Values come from the environment (or a local .env);
nothing is hard-coded so the same image runs locally and on the cluster."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── Source feed ──────────────────────────────────────────────────────────
    ws_url: str = "wss://stream.binance.com:9443/stream"
    symbols: str = "BTCUSDT,ETHUSDT,SOLUSDT"
    source: str = "binance"

    # ── Kafka / Redpanda ─────────────────────────────────────────────────────
    kafka_bootstrap: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    topic_trades: str = "market.trades"
    schema_path: str = "schemas/trade.avsc"

    # ── Producer tuning (durable + idempotent by default) ─────────────────────
    producer_acks: str = "all"
    producer_linger_ms: int = 50
    producer_compression: str = "zstd"

    # ── Resilience ───────────────────────────────────────────────────────────
    reconnect_max_backoff_seconds: float = 30.0

    @property
    def symbol_list(self) -> list[str]:
        """Normalised, de-duplicated upper-case symbols."""
        seen: dict[str, None] = {}
        for s in self.symbols.split(","):
            s = s.strip().upper()
            if s:
                seen.setdefault(s, None)
        return list(seen)
