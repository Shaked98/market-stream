"""Pure DataFrame transforms for the streaming job.

Kept free of any streaming/IO concerns so they can be unit-tested on a plain (batch)
SparkSession: feed a fixed set of trades, assert the exact OHLCV/VWAP output. The job
applies `.withWatermark(...)` before calling `aggregate_ohlcv` in streaming mode; on a
batch DataFrame the same function produces the same windows, which is what the tests use.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DECIMAL = "decimal(38,8)"


def cast_trade_columns(df: DataFrame) -> DataFrame:
    """Cast the on-wire string price/quantity to fixed-precision decimals (no float drift)."""
    return df.withColumn("price", F.col("price").cast(DECIMAL)).withColumn(
        "quantity", F.col("quantity").cast(DECIMAL)
    )


def aggregate_ohlcv(trades: DataFrame, window_duration: str = "1 minute") -> DataFrame:
    """Roll trades up into tumbling-window OHLCV + VWAP, keyed by (symbol, window_start).

    open/close use min_by/max_by on event time so they are deterministic regardless of
    row order (unlike first/last). VWAP = Σ(price·qty) / Σ(qty).
    """
    win = F.window(F.col("trade_time"), window_duration)
    agg = trades.groupBy(win, F.col("symbol")).agg(
        F.min_by("price", "trade_time").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", "trade_time").alias("close"),
        F.sum("quantity").alias("volume"),
        F.sum(F.col("price") * F.col("quantity")).alias("quote_volume"),
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
        (F.col("quote_volume") / F.col("volume")).cast(DECIMAL).alias("vwap"),
        F.col("trade_count").cast("bigint").alias("trade_count"),
        F.current_timestamp().alias("updated_at"),
    )
