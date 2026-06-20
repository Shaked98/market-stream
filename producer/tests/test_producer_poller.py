"""Unit tests for the Yahoo poller's pure helpers: URL building, backoff, and the
day-volume → increment logic (parsing a recorded Yahoo response, no network)."""

from market_producer import poller


def test_quote_url_formats_symbol():
    url = poller.quote_url(
        "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1m&range=1d", "AAPL"
    )
    assert url == "https://query1.finance.yahoo.com/v8/finance/chart/AAPL?interval=1m&range=1d"


def test_compute_backoff_is_exponential_and_capped():
    assert poller.compute_backoff(0) == 1.0
    assert poller.compute_backoff(1) == 2.0
    assert poller.compute_backoff(2) == 4.0
    assert poller.compute_backoff(100, cap=30.0) == 30.0


def test_fetch_quote_parses_price_and_day_volume(monkeypatch):
    # A trimmed Yahoo v8 chart payload.
    payload = {
        "chart": {
            "result": [
                {
                    "meta": {"regularMarketPrice": 190.42},
                    "indicators": {"quote": [{"close": [189.9, 190.42], "volume": [1000, 250]}]},
                }
            ]
        }
    }

    class _Resp:
        def read(self):
            import json
            return json.dumps(payload).encode()
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False

    monkeypatch.setattr(poller.urllib.request, "urlopen", lambda *a, **k: _Resp())
    q = poller.fetch_quote("http://x/{symbol}", "AAPL")
    assert q == {"symbol": "AAPL", "price": 190.42, "day_volume": 1250}  # 1000 + 250
