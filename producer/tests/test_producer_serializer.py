"""Unit tests for the raw-quote → Quote record mapping and Avro encodability.
No broker or schema registry required."""

import io
from datetime import datetime, timezone

import fastavro
import pytest

from market_producer.serializer import to_quote_record

RAW = {
    "symbol": "AAPL",
    "price": 190.42,
    "volume": 1250,
    "day_volume": 5_000_000,
    "quote_time_ms": 1_700_000_000_000,
}


def test_to_quote_record_maps_fields():
    rec = to_quote_record(RAW, source="yahoo")
    assert rec["symbol"] == "AAPL"
    assert rec["price"] == "190.42"          # exact precision preserved as string
    assert rec["volume"] == "1250"
    assert rec["day_volume"] == 5_000_000
    assert rec["source"] == "yahoo"
    assert rec["quote_time"] == datetime.fromtimestamp(1_700_000_000.0, tz=timezone.utc)
    assert rec["ingest_time"].tzinfo is timezone.utc


def test_valid_record_encodes_against_schema():
    schema = fastavro.schema.load_schema("schemas/quote.avsc")
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, to_quote_record(RAW))
    assert buf.getbuffer().nbytes > 0          # logical timestamp-millis encode works


def test_malformed_record_fails_to_encode():
    schema = fastavro.schema.load_schema("schemas/quote.avsc")
    with pytest.raises(Exception):
        fastavro.schemaless_writer(io.BytesIO(), schema, {"symbol": "AAPL"})
