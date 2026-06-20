"""Map raw Binance trade events to the `Trade` Avro record and serialize them via the
Confluent-compatible Schema Registry.

`to_trade_record` is a pure function (no I/O) so it is unit-testable without a broker or
registry. `TradeSerializer` wraps the registry-backed Avro encoder + the key encoder.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)


def _to_dt(ms: int | str) -> datetime:
    """Epoch milliseconds → timezone-aware UTC datetime (fastavro encodes timestamp-millis
    from datetime)."""
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)


def to_trade_record(raw: dict, source: str = "binance") -> dict:
    """Normalise a raw Binance `@trade` event into a `Trade` record dict.

    Binance field map: s=symbol, t=trade id, p=price, q=qty, m=buyer-is-maker,
    T=trade time, E=event time.
    """
    return {
        "symbol": raw["s"],
        "trade_id": int(raw["t"]),
        "price": str(raw["p"]),       # keep exact exchange precision; Spark casts to decimal
        "quantity": str(raw["q"]),
        "is_buyer_maker": bool(raw["m"]),
        "trade_time": _to_dt(raw["T"]),
        "event_time": _to_dt(raw["E"]),
        "ingest_time": datetime.now(tz=timezone.utc),
        "source": source,
    }


class TradeSerializer:
    """Avro value serializer (Confluent wire format) + string key serializer."""

    def __init__(self, schema_registry_url: str, schema_path: str, topic: str) -> None:
        self.topic = topic
        self._sr = SchemaRegistryClient({"url": schema_registry_url})
        schema_str = Path(schema_path).read_text(encoding="utf-8")
        self._value = AvroSerializer(self._sr, schema_str)
        self._key = StringSerializer("utf_8")

    def serialize(self, record: dict) -> tuple[bytes, bytes]:
        """Return (key_bytes, value_bytes). Raises if the record violates the schema —
        the caller treats that as a drop, never a crash."""
        value = self._value(record, SerializationContext(self.topic, MessageField.VALUE))
        key = self._key(record["symbol"], SerializationContext(self.topic, MessageField.KEY))
        return key, value
