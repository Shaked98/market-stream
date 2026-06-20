"""Kafka/Redpanda publisher for serialized trade records.

Durable + idempotent (`acks=all`, `enable.idempotence=true`). Keyed by symbol so a
symbol's ticks keep their order on one partition (the streaming windowing relies on
this). Backpressure-aware: when librdkafka's local queue is full, we drain and retry
rather than dropping. Schema-invalid records are dropped (counted), never fatal.
"""

from __future__ import annotations

import logging

from confluent_kafka import Producer

from .config import Settings
from .serializer import TradeSerializer

log = logging.getLogger("market_producer.publisher")


class TradePublisher:
    def __init__(
        self, settings: Settings, serializer: TradeSerializer, producer: Producer | None = None
    ) -> None:
        self.serializer = serializer
        self.topic = settings.topic_trades
        self.sent = 0
        self.failed = 0
        self.dropped = 0
        self._producer = producer or Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "acks": settings.producer_acks,
                "enable.idempotence": True,
                "linger.ms": settings.producer_linger_ms,
                "compression.type": settings.producer_compression,
                "client.id": "market-producer",
            }
        )

    def _on_delivery(self, err, msg) -> None:  # noqa: ANN001 — confluent callback signature
        if err is not None:
            self.failed += 1
            log.warning("delivery failed (partition=%s): %s", getattr(msg, "partition", lambda: "?")(), err)
        else:
            self.sent += 1

    def publish(self, record: dict) -> None:
        try:
            key, value = self.serializer.serialize(record)
        except Exception as exc:  # noqa: BLE001 — schema violation → drop, keep streaming
            self.dropped += 1
            log.warning("dropping schema-invalid message: %s", exc)
            return

        while True:
            try:
                self._producer.produce(
                    self.topic, key=key, value=value, on_delivery=self._on_delivery
                )
                break
            except BufferError:
                # Local queue full → backpressure: serve callbacks to drain, then retry.
                self._producer.poll(0.5)
        self._producer.poll(0)  # serve any pending delivery callbacks

    def flush(self, timeout: float = 10.0) -> int:
        remaining = self._producer.flush(timeout)
        if remaining:
            log.warning("%d message(s) still queued at flush timeout", remaining)
        return remaining
