"""Produce N synthetic, schema-valid trade messages for the smoke test.

Uses a distinct symbol (default TESTUSDT) so the assertions don't race the live Binance
feed. Reuses the real producer's serializer/publisher, so this also exercises the Avro +
Schema Registry path end-to-end.
"""

from __future__ import annotations

import os
import random
import sys
import time

sys.path.insert(0, "producer")

from market_producer.config import Settings  # noqa: E402
from market_producer.publisher import TradePublisher  # noqa: E402
from market_producer.serializer import TradeSerializer, to_trade_record  # noqa: E402

N = int(os.environ.get("N", "50"))
SYMBOL = os.environ.get("SMOKE_SYMBOL", "TESTUSDT")

settings = Settings(
    kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"),
    schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081"),
    topic_trades=os.environ.get("TOPIC_TRADES", "market.trades"),
    schema_path=os.environ.get("SCHEMA_PATH", "schemas/trade.avsc"),
)

serializer = TradeSerializer(settings.schema_registry_url, settings.schema_path, settings.topic_trades)
publisher = TradePublisher(settings, serializer)

now_ms = int(time.time() * 1000)
price = 100.0
for i in range(N):
    price *= 1 + random.uniform(-0.002, 0.002)
    raw = {
        "s": SYMBOL,
        "t": now_ms + i,
        "p": f"{price:.2f}",
        "q": "0.5",
        "m": bool(i % 2),
        "T": now_ms + i * 10,
        "E": now_ms + i * 10,
    }
    publisher.publish(to_trade_record(raw, source="synthetic"))

publisher.flush()
print(f"produced {publisher.sent} synthetic ticks for {SYMBOL} "
      f"(dropped={publisher.dropped}, failed={publisher.failed})")
