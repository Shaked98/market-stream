"""Produce N synthetic, schema-valid quote messages for the smoke test.

Uses a distinct symbol (default TEST) so the assertions don't race the live Yahoo feed.
Reuses the real producer's serializer/publisher, so this also exercises the Avro + Schema
Registry path end-to-end.
"""

from __future__ import annotations

import os
import random
import sys
import time

sys.path.insert(0, "producer")

from market_producer.config import Settings  # noqa: E402
from market_producer.publisher import QuotePublisher  # noqa: E402
from market_producer.serializer import QuoteSerializer, to_quote_record  # noqa: E402

N = int(os.environ.get("N", "50"))
SYMBOL = os.environ.get("SMOKE_SYMBOL", "TEST")

settings = Settings(
    kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"),
    schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081"),
    topic_quotes=os.environ.get("TOPIC_QUOTES", "market.quotes"),
    schema_path=os.environ.get("SCHEMA_PATH", "schemas/quote.avsc"),
)

serializer = QuoteSerializer(settings.schema_registry_url, settings.schema_path, settings.topic_quotes)
publisher = QuotePublisher(settings, serializer)

now_ms = int(time.time() * 1000)
price = 100.0
day_volume = 0
for i in range(N):
    price *= 1 + random.uniform(-0.002, 0.002)
    increment = random.randint(1, 100)
    day_volume += increment
    raw = {
        "symbol": SYMBOL,
        "price": round(price, 2),
        "volume": increment,
        "day_volume": day_volume,
        "quote_time_ms": now_ms + i,
    }
    publisher.publish(to_quote_record(raw, source="synthetic"))

publisher.flush()
print(f"produced {publisher.sent} synthetic quotes for {SYMBOL} "
      f"(dropped={publisher.dropped}, failed={publisher.failed})")
