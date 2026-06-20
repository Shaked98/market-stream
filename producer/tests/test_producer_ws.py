"""Unit tests for the WebSocket URL builder and backoff schedule (both pure)."""

from market_producer.ws_client import build_stream_url, compute_backoff


def test_build_stream_url_combined():
    url = build_stream_url("wss://stream.binance.com:9443/stream", ["BTCUSDT", "ETHUSDT"])
    assert url == "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"


def test_build_stream_url_appends_to_existing_query():
    url = build_stream_url("wss://host/stream?foo=1", ["BTCUSDT"])
    assert url == "wss://host/stream?foo=1&streams=btcusdt@trade"


def test_compute_backoff_is_exponential_and_capped():
    assert compute_backoff(0) == 1.0
    assert compute_backoff(1) == 2.0
    assert compute_backoff(2) == 4.0
    assert compute_backoff(100, cap=30.0) == 30.0
