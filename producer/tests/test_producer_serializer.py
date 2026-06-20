"""Unit tests for the raw-event → Trade record mapping and Avro encodability.
No broker or schema registry required."""

import io
from datetime import datetime, timezone

import fastavro
import pytest

from market_producer.serializer import to_trade_record

RAW = {
    "e": "trade",
    "E": 1700000000123,
    "s": "BTCUSDT",
    "t": 42,
    "p": "42000.50",
    "q": "0.123",
    "T": 1700000000000,
    "m": True,
}


def test_to_trade_record_maps_binance_fields():
    rec = to_trade_record(RAW, source="binance")
    assert rec["symbol"] == "BTCUSDT"
    assert rec["trade_id"] == 42
    assert rec["price"] == "42000.50"          # exact precision preserved as string
    assert rec["quantity"] == "0.123"
    assert rec["is_buyer_maker"] is True
    assert rec["source"] == "binance"
    assert rec["trade_time"] == datetime.fromtimestamp(1_700_000_000.0, tz=timezone.utc)
    assert rec["event_time"].tzinfo is timezone.utc


def test_valid_record_encodes_against_schema():
    schema = fastavro.schema.load_schema("schemas/trade.avsc")
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, to_trade_record(RAW))
    assert buf.getbuffer().nbytes > 0          # logical timestamp-millis encode works


def test_malformed_record_fails_to_encode():
    schema = fastavro.schema.load_schema("schemas/trade.avsc")
    with pytest.raises(Exception):
        fastavro.schemaless_writer(io.BytesIO(), schema, {"symbol": "BTCUSDT"})
