"""Pure DataFrame transforms for the streaming job.

Kept free of any streaming/IO concerns so they can be unit-tested on a plain (batch)
SparkSession: feed a fixed set of quotes, assert the exact OHLCV/VWAP output. The job
applies `.withWatermark(...)` before calling `aggregate_ohlcv` in streaming mode; on a
batch DataFrame the same function produces the same windows, which is what the tests use.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DECIMAL = "decimal(38,8)"


def cast_quote_columns(df: DataFrame) -> DataFrame:
    """Cast the on-wire string price/volume to fixed-precision decimals (no float drift)."""
    return df.withColumn("price", F.col("price").cast(DECIMAL)).withColumn(
        "volume", F.col("volume").cast(DECIMAL)
    )


def aggregate_ohlcv(quotes: DataFrame, window_duration: str = "1 minute") -> DataFrame:
    """Roll quotes up into tumbling-window OHLCV + VWAP, keyed by (symbol, window_start).

    open/close use min_by/max_by on quote time so they're deterministic regardless of row
    order. VWAP = Σ(price·volume) / Σ(volume); when no volume traded in the window (markets
    closed), VWAP falls back to the close so the row is never null.
    """
    win = F.window(F.col("quote_time"), window_duration)
    agg = quotes.groupBy(win, F.col("symbol")).agg(
        F.min_by("price", "quote_time").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", "quote_time").alias("close"),
        F.sum("volume").alias("volume"),
        F.sum(F.col("price") * F.col("volume")).alias("quote_volume"),
        F.count(F.lit(1)).alias("trade_count"),
    )
    return agg.select(
        F.col("symbol"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("open").cast(DECIMAL).alias("open"),
        F.col("high").cast(DECIMAL).alias("high"),
        F.col("low").cast(DECIMAL).alias("low"),
        F.col("close").cast(DECIMAL).alias("close"),
        F.col("volume").cast(DECIMAL).alias("volume"),
        F.col("quote_volume").cast(DECIMAL).alias("quote_volume"),
        F.when(F.col("volume") > 0, F.col("quote_volume") / F.col("volume"))
        .otherwise(F.col("close"))
        .cast(DECIMAL)
        .alias("vwap"),
        F.col("trade_count").cast("bigint").alias("trade_count"),
        F.current_timestamp().alias("updated_at"),
    )
