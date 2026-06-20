"""Unit tests for the publisher: drop-on-invalid, key-by-symbol, backpressure retry.
A fake librdkafka Producer + a fake serializer keep these pure (no broker)."""

from market_producer.config import Settings
from market_producer.publisher import TradePublisher


class FakeProducer:
    def __init__(self, fail_buffer_times: int = 0):
        self.produced: list = []
        self.poll_calls = 0
        self._fail = fail_buffer_times

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self._fail > 0:
            self._fail -= 1
            raise BufferError("local queue full")
        self.produced.append((topic, key, value))
        if on_delivery:
            on_delivery(None, None)

    def poll(self, _timeout):
        self.poll_calls += 1

    def flush(self, _timeout):
        return 0


class FakeSerializer:
    def __init__(self, raise_on: str | None = None):
        self.raise_on = raise_on

    def serialize(self, record):
        if self.raise_on and record.get("symbol") == self.raise_on:
            raise ValueError("schema violation")
        return record["symbol"].encode(), b"value"


def _settings():
    return Settings(kafka_bootstrap="x", schema_registry_url="x", topic_trades="market.trades")


def test_publish_drops_schema_invalid_record():
    pub = TradePublisher(_settings(), FakeSerializer(raise_on="BAD"), producer=FakeProducer())
    pub.publish({"symbol": "BAD"})
    assert pub.dropped == 1
    assert pub.sent == 0


def test_publish_keys_by_symbol():
    fake = FakeProducer()
    pub = TradePublisher(_settings(), FakeSerializer(), producer=fake)
    pub.publish({"symbol": "BTCUSDT"})
    assert fake.produced[0][1] == b"BTCUSDT"     # key == symbol
    assert pub.sent == 1


def test_publish_retries_on_backpressure():
    fake = FakeProducer(fail_buffer_times=2)
    pub = TradePublisher(_settings(), FakeSerializer(), producer=fake)
    pub.publish({"symbol": "ETHUSDT"})
    assert len(fake.produced) == 1               # eventually delivered, never dropped
    assert fake.poll_calls >= 2                   # drained between retries
