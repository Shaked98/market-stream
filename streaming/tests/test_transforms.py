"""Unit tests for the OHLCV/VWAP transform — the project's one real transform, verified
with exact expected values on a fixed set of quotes."""

import datetime as dt
from decimal import Decimal

from lib.transforms import aggregate_ohlcv, cast_quote_columns

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)


def test_aggregate_ohlcv_single_window(spark):
    # columns: symbol, price, volume (increment), quote_time
    rows = [
        ("AAPL", "100.0", "1.0", T0 + dt.timedelta(seconds=1)),
        ("AAPL", "110.0", "2.0", T0 + dt.timedelta(seconds=2)),
        ("AAPL", "90.0", "1.0", T0 + dt.timedelta(seconds=3)),
        ("AAPL", "105.0", "1.0", T0 + dt.timedelta(seconds=4)),
    ]
    df = spark.createDataFrame(rows, ["symbol", "price", "volume", "quote_time"])
    out = aggregate_ohlcv(cast_quote_columns(df)).collect()

    assert len(out) == 1
    r = out[0]
    assert r["symbol"] == "AAPL"
    assert r["open"] == Decimal("100.00000000")   # earliest by quote_time
    assert r["high"] == Decimal("110.00000000")
    assert r["low"] == Decimal("90.00000000")
    assert r["close"] == Decimal("105.00000000")  # latest by quote_time
    assert r["volume"] == Decimal("5.00000000")
    # quote_volume = 100*1 + 110*2 + 90*1 + 105*1 = 515
    assert r["quote_volume"] == Decimal("515.00000000")
    assert r["vwap"] == Decimal("103.00000000")   # 515 / 5
    assert r["trade_count"] == 4


def test_vwap_falls_back_to_close_when_no_volume(spark):
    # Market-closed case: zero increments → VWAP must equal close, never null.
    rows = [
        ("MSFT", "430.0", "0.0", T0 + dt.timedelta(seconds=5)),
        ("MSFT", "431.0", "0.0", T0 + dt.timedelta(seconds=9)),
    ]
    df = spark.createDataFrame(rows, ["symbol", "price", "volume", "quote_time"])
    r = aggregate_ohlcv(cast_quote_columns(df)).collect()[0]
    assert r["volume"] == Decimal("0.00000000")
    assert r["vwap"] == Decimal("431.00000000")   # == close
    assert r["trade_count"] == 2


def test_aggregate_ohlcv_splits_windows_and_symbols(spark):
    rows = [
        ("AAPL", "100.0", "1.0", T0 + dt.timedelta(seconds=10)),
        ("AAPL", "200.0", "1.0", T0 + dt.timedelta(seconds=70)),   # next 1-min window
        ("GOOG", "140.0", "3.0", T0 + dt.timedelta(seconds=15)),
    ]
    df = spark.createDataFrame(rows, ["symbol", "price", "volume", "quote_time"])
    out = {(r["symbol"], r["window_start"]): r for r in aggregate_ohlcv(cast_quote_columns(df)).collect()}

    assert len(out) == 3  # 2 AAPL windows + 1 GOOG window
    goog = next(r for (s, _), r in out.items() if s == "GOOG")
    assert goog["vwap"] == Decimal("140.00000000")
    assert goog["trade_count"] == 1
