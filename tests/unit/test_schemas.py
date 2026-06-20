"""Repo-level sanity test: the Avro contracts are valid and parseable. Needs no broker,
Spark, or JVM — runs anywhere fastavro is installed."""

import fastavro


def test_avro_schemas_parse():
    for path in ("schemas/quote.avsc", "schemas/ohlcv.avsc"):
        assert fastavro.schema.load_schema(path), f"{path} failed to parse"
