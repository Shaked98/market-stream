# Source choice — why Yahoo Finance (polled)

The pipeline needs a **free, no-paid-key** market feed. For stocks that rules out most
real-time providers (their real-time tiers gate data behind paid keys). The original brief
explicitly allows "a polled stock API", which is the cleanest keyless option.

## Decision: Yahoo Finance chart endpoint, polled

- **Endpoint:** `https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1m&range=1d`
- **No API key, no signup.** The producer polls each symbol every few seconds
  (`producer/poller.py`), reads `regularMarketPrice` + the day's cumulative volume, and emits
  one `Quote` per poll. `volume` is the day-volume **increment** since the last poll, so VWAP
  is weighted by actually-traded volume regardless of how fast we poll.
- **Configurable symbols.** Defaults are `AAPL,GOOG,MSFT`; set `SYMBOLS` to any tickers.
- **Quote time = poll time.** Using wall-clock poll time as the event/watermark column keeps
  the 1-minute windows advancing even when the (delayed) price is momentarily static — the
  demo keeps ticking over.

## Trade-offs (and how they're handled)

- **Delayed (~15 min) and polled, not true real-time.** Fine for a *pipeline* demo — the point
  is the architecture (feed → Kafka → Spark → Iceberg), not low-latency trading.
- **Markets close nights/weekends.** Off-hours the price is static and volume increments are 0
  (VWAP falls back to the close). The web demo's **sample-feed fallback** keeps the page alive
  and moving whenever live data is thin — so a recruiter never sees a frozen page.

## Alternatives considered

| Source | Why not (here) |
|--------|----------------|
| **Alpaca IEX** WebSocket | Genuinely real-time on a free IEX feed, but needs a free Alpaca account (API key + secret) and SOPS secret handling. A great upgrade if you want true real-time; swap `producer/poller.py` for a WS client and add the key to `secrets.sops.yaml`. |
| **Finnhub** real-time WS | Free token, but free real-time US-equity coverage has become restricted/unreliable. |
| **Paid real-time** (Polygon, Twelve Data, …) | Violates the "no paid key" constraint. |
| **Crypto** (Binance/Coinbase) | The previous version of this project — 24/7 and keyless, but the brief here is stocks. |

## Resilience notes

A failed fetch backs off (exponential + cap, `producer/poller.py`) and is skipped for that
cycle — never fatal. Polling 3 symbols every ~3s is gentle on Yahoo. A browser-like
`User-Agent` is sent to avoid the occasional anonymous-request 403.
