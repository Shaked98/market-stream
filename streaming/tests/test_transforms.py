"""Unit tests for the OHLCV/VWAP transform — the project's one real transform, verified
with exact expected values on a fixed set of trades."""

import datetime as dt
from decimal import Decimal

from lib.transforms import aggregate_ohlcv, cast_trade_columns

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)


def test_aggregate_ohlcv_single_window(spark):
    rows = [
        ("BTCUSDT", "100.0", "1.0", T0 + dt.timedelta(seconds=1)),
        ("BTCUSDT", "110.0", "2.0", T0 + dt.timedelta(seconds=2)),
        ("BTCUSDT", "90.0", "1.0", T0 + dt.timedelta(seconds=3)),
        ("BTCUSDT", "105.0", "1.0", T0 + dt.timedelta(seconds=4)),
    ]
    df = spark.createDataFrame(rows, ["symbol", "price", "quantity", "trade_time"])
    out = aggregate_ohlcv(cast_trade_columns(df)).collect()

    assert len(out) == 1
    r = out[0]
    assert r["symbol"] == "BTCUSDT"
    assert r["open"] == Decimal("100.00000000")   # earliest by trade_time
    assert r["high"] == Decimal("110.00000000")
    assert r["low"] == Decimal("90.00000000")
    assert r["close"] == Decimal("105.00000000")  # latest by trade_time
    assert r["volume"] == Decimal("5.00000000")
    # quote_volume = 100*1 + 110*2 + 90*1 + 105*1 = 515
    assert r["quote_volume"] == Decimal("515.00000000")
    assert r["vwap"] == Decimal("103.00000000")   # 515 / 5
    assert r["trade_count"] == 4


def test_aggregate_ohlcv_splits_windows_and_symbols(spark):
    rows = [
        ("BTCUSDT", "100.0", "1.0", T0 + dt.timedelta(seconds=10)),
        ("BTCUSDT", "200.0", "1.0", T0 + dt.timedelta(seconds=70)),   # next 1-min window
        ("ETHUSDT", "10.0", "3.0", T0 + dt.timedelta(seconds=15)),
    ]
    df = spark.createDataFrame(rows, ["symbol", "price", "quantity", "trade_time"])
    out = {(r["symbol"], r["window_start"]): r for r in aggregate_ohlcv(cast_trade_columns(df)).collect()}

    assert len(out) == 3  # 2 BTC windows + 1 ETH window
    eth = next(r for (s, _), r in out.items() if s == "ETHUSDT")
    assert eth["vwap"] == Decimal("10.00000000")
    assert eth["trade_count"] == 1
