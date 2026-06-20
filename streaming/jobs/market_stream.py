"""Spark Structured Streaming: Redpanda (Avro trades) → Apache Iceberg.

Two streaming queries share the same Kafka topic:

  1. trades_raw — a stateless append of every decoded tick (exactly-once: Kafka offsets
     in the checkpoint + Iceberg's atomic commits).
  2. ohlcv_1m  — a stateful, watermarked 1-minute OHLCV/VWAP aggregation written via
     foreachBatch + Iceberg MERGE (idempotent upsert on (symbol, window_start)).

A stateful windowed aggregation can't live inside the raw append's foreachBatch (it would
lose cross-batch window state), so these are deliberately two queries. Each Kafka read is
independent; that double-read is the accepted cost of the clean separation.

Catalog / S3 config is supplied by the runtime (the SparkApplication `sparkConf` on the
cluster, or `--conf` flags in the local docker-compose). The only thing assembled here is
the OAuth2 client credential, which is secret and must come from the environment.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from lib import iceberg_io
from lib.transforms import aggregate_ohlcv, cast_trade_columns

CATALOG = os.environ.get("ICEBERG_CATALOG", "lake")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
TRADES_TOPIC = os.environ.get("TOPIC_TRADES", "market.trades")
TRADES_TABLE = os.environ.get("TRADES_TABLE", f"{CATALOG}.market.trades_raw")
OHLCV_TABLE = os.environ.get("OHLCV_TABLE", f"{CATALOG}.market.ohlcv_1m")
WINDOW_DURATION = os.environ.get("WINDOW_DURATION", "1 minute")
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "2 minutes")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "latest")
CHECKPOINT = os.environ.get("CHECKPOINT_LOCATION", "s3a://lakehouse/checkpoints/market-stream")
SCHEMA_PATH = os.environ.get("SCHEMA_PATH", "/opt/spark/jobs/schemas/trade.avsc")


def build_spark() -> SparkSession:
    builder = SparkSession.builder.appName("market-stream")
    # The catalog OAuth2 credential is secret → assembled from env, never baked into the
    # (plaintext) sparkConf. Mirrors spark-k8s/docker/spark-iceberg/iceberg_demo.py.
    if os.environ.get("OAUTH_ENABLED", "false").lower() == "true":
        client_id = os.environ.get("SPARK_OAUTH_CLIENT_ID")
        client_secret = os.environ.get("SPARK_OAUTH_CLIENT_SECRET")
        if client_id and client_secret:
            builder = builder.config(
                f"spark.sql.catalog.{CATALOG}.credential", f"{client_id}:{client_secret}"
            )
    return builder.getOrCreate()


def read_trades(spark: SparkSession, schema_json: str):
    """Read the Kafka topic and Avro-decode it. confluent-kafka prepends a 5-byte wire
    header (magic byte + 4-byte schema id) that OSS `from_avro` doesn't understand, so we
    strip it before decoding."""
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TRADES_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
    )
    decoded = (
        kafka.select(F.expr("substring(value, 6, length(value) - 5)").alias("avro"))
        .select(from_avro(F.col("avro"), schema_json).alias("t"))
        .select("t.*")
    )
    return cast_trade_columns(decoded)


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    schema_json = Path(SCHEMA_PATH).read_text(encoding="utf-8")

    iceberg_io.ensure_namespace(spark, CATALOG, "market")
    iceberg_io.ensure_tables(spark, TRADES_TABLE, OHLCV_TABLE)

    trades = read_trades(spark, schema_json)

    # Query 1: raw append.
    raw_query = (
        trades.writeStream.queryName("trades_raw")
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT}/trades_raw")
        .toTable(TRADES_TABLE)
    )

    # Query 2: watermarked 1-minute OHLCV/VWAP → MERGE upsert.
    ohlcv = aggregate_ohlcv(trades.withWatermark("trade_time", WATERMARK_DELAY), WINDOW_DURATION)

    def write_ohlcv(batch_df, _batch_id):
        iceberg_io.upsert_ohlcv(batch_df, OHLCV_TABLE)

    ohlcv_query = (
        ohlcv.writeStream.queryName("ohlcv_1m")
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT}/ohlcv_1m")
        .foreachBatch(write_ohlcv)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
