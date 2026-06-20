"""market_producer — Binance WebSocket → Redpanda (Avro) market-data producer.

Resilient (reconnect with backoff, backpressure-aware), schema-validated, idempotent.
"""

__version__ = "0.1.0"
