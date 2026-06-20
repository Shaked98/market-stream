"""Entrypoint: poll Yahoo Finance quotes → Redpanda. `python -m market_producer`.

Graceful SIGTERM/SIGINT: stop the poll loop and flush buffered messages so a Kubernetes
rolling restart doesn't lose in-flight quotes.
"""

from __future__ import annotations

import logging
import signal
import threading

from .config import Settings
from .poller import iter_quotes
from .publisher import QuotePublisher
from .serializer import QuoteSerializer, to_quote_record

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("market_producer")


def main() -> None:
    settings = Settings()
    serializer = QuoteSerializer(
        settings.schema_registry_url, settings.schema_path, settings.topic_quotes
    )
    publisher = QuotePublisher(settings, serializer)

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, lambda *_: stop.set())
        except (ValueError, OSError):
            pass  # not on the main thread / unsupported platform

    log.info(
        "polling %s quotes for %s every %ss → topic %s",
        settings.source, settings.symbol_list, settings.poll_interval_seconds, settings.topic_quotes,
    )
    try:
        for raw in iter_quotes(settings, stop):
            publisher.publish(to_quote_record(raw, settings.source))
    except KeyboardInterrupt:
        pass
    finally:
        log.info(
            "flushing (sent=%d failed=%d dropped=%d)",
            publisher.sent, publisher.failed, publisher.dropped,
        )
        publisher.flush()


if __name__ == "__main__":
    main()
