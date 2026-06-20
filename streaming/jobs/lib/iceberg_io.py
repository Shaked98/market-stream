"""Iceberg table DDL + write helpers (append for raw ticks, MERGE upsert for rollups).

The MERGE on (symbol, window_start) makes a re-processed window an idempotent overwrite,
which — together with Spark checkpointing and Iceberg's atomic commits — gives the
aggregate "exactly-once-ish" behaviour even if a micro-batch is retried.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def ensure_namespace(spark: SparkSession, catalog: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def ensure_tables(spark: SparkSession, quotes_table: str, ohlcv_table: str) -> None:
    """Create the append + aggregate tables if absent. Partitioned by symbol + event date."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {quotes_table} (
            symbol      STRING,
            price       DECIMAL(38,8),
            volume      DECIMAL(38,8),
            day_volume  BIGINT,
            quote_time  TIMESTAMP,
            ingest_time TIMESTAMP,
            source      STRING
        ) USING iceberg
        PARTITIONED BY (symbol, days(quote_time))
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ohlcv_table} (
            symbol       STRING,
            window_start TIMESTAMP,
            window_end   TIMESTAMP,
            open         DECIMAL(38,8),
            high         DECIMAL(38,8),
            low          DECIMAL(38,8),
            close        DECIMAL(38,8),
            volume       DECIMAL(38,8),
            quote_volume DECIMAL(38,8),
            vwap         DECIMAL(38,8),
            trade_count  BIGINT,
            updated_at   TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (symbol, days(window_start))
        """
    )


def append_quotes(batch_df: DataFrame, quotes_table: str) -> None:
    batch_df.writeTo(quotes_table).append()


def upsert_ohlcv(batch_df: DataFrame, ohlcv_table: str) -> None:
    """Idempotent upsert of a micro-batch of rollups via Iceberg MERGE.

    Inside foreachBatch the batch DataFrame belongs to its own SparkSession, so the temp
    view and the MERGE must use THAT session (not the driver's outer session) — otherwise
    the view isn't visible to the query.
    """
    session = batch_df.sparkSession
    batch_df.createOrReplaceTempView("_ohlcv_updates")
    session.sql(
        f"""
        MERGE INTO {ohlcv_table} t
        USING _ohlcv_updates s
          ON t.symbol = s.symbol AND t.window_start = s.window_start
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
