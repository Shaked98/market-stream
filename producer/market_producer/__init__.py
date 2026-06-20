"""market_producer — Yahoo Finance quotes → Redpanda (Avro) market-data producer.

Polls a free, keyless stock feed; resilient (backoff on fetch failure, backpressure-aware),
schema-validated, idempotent.
"""

__version__ = "0.1.0"
