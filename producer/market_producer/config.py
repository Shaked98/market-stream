"""Typed, 12-factor configuration. Values come from the environment (or a local .env);
nothing is hard-coded so the same image runs locally and on the cluster."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── Source feed (Yahoo Finance, polled, keyless) ─────────────────────────
    quote_url: str = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1m&range=1d"
    symbols: str = "AAPL,GOOG,MSFT"
    source: str = "yahoo"
    poll_interval_seconds: float = 3.0
    http_timeout_seconds: float = 10.0

    # ── Kafka / Redpanda ─────────────────────────────────────────────────────
    kafka_bootstrap: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    topic_quotes: str = "market.quotes"
    schema_path: str = "schemas/quote.avsc"

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
