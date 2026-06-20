"""Entrypoint: stream Binance trades → Redpanda. `python -m market_producer`.

Graceful SIGTERM/SIGINT: stop the WS loop and flush buffered messages so a Kubernetes
rolling restart doesn't lose in-flight ticks.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from .config import Settings
from .publisher import TradePublisher
from .serializer import TradeSerializer, to_trade_record
from .ws_client import stream_trades

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("market_producer")


async def run() -> None:
    settings = Settings()
    serializer = TradeSerializer(
        settings.schema_registry_url, settings.schema_path, settings.topic_trades
    )
    publisher = TradePublisher(settings, serializer)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # Windows event loop — Ctrl+C still raises KeyboardInterrupt

    log.info(
        "producing %s ticks for %s → topic %s",
        settings.source, settings.symbol_list, settings.topic_trades,
    )
    try:
        async for raw in stream_trades(settings, stop):
            publisher.publish(to_trade_record(raw, settings.source))
    finally:
        log.info(
            "flushing (sent=%d failed=%d dropped=%d)",
            publisher.sent, publisher.failed, publisher.dropped,
        )
        publisher.flush()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
