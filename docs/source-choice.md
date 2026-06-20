# Source choice — why Binance

The pipeline needs a **free, keyless, high-volume, real-time** feed so the demo is visibly
alive and the streaming/windowing logic has enough events to be interesting. Crypto exchanges
fit best: public market data needs no account, and trades arrive continuously (unlike stock
APIs, which gate real-time data behind paid keys and close overnight/weekends).

## Decision: Binance public combined-stream WebSocket

- **Endpoint:** `wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/...`
- **No API key** for public market data; generous connection limits.
- **`@trade` stream** gives exactly what we model: per-trade `price`, `quantity`, `trade_time`,
  and the `is_buyer_maker` aggressor flag — a clean fit for the `Trade` Avro schema and the
  OHLCV/VWAP rollup.
- Combined streams let one connection carry many symbols; the producer keys by `symbol` so
  per-symbol order is preserved into Kafka.

## Alternatives considered

| Source | Why not (here) |
|--------|----------------|
| **Coinbase** WS (Advanced Trade / Exchange `matches`) | Equally free/keyless and US-friendly; kept as the documented fallback. Slightly lower volume and a different message shape. **Switch to it if you're in a region where `stream.binance.com` is geo-restricted** — only `producer/ws_client.py`'s parsing + the symbol format (`BTC-USD` vs `BTCUSDT`) change. |
| **Stock APIs** (Alpaca, Finnhub, Polygon) | Real-time tiers need a paid key; markets close, so the demo would sit idle nights/weekends. |
| **Synthetic generator** | Used for the smoke test (`tests/produce_synthetic.py`), but a portfolio demo should show a *real* feed. |

## Resilience notes

Trade streams are stateless, so a dropped connection is simply reconnected (exponential
backoff + jitter, `producer/ws_client.py`); a momentary gap is acceptable and logged, not
back-filled. Binance's keepalive ping frames are answered automatically by the `websockets`
client.
