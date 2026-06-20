// Deploy-time configuration for the static demo.
//
// API_BASE_URL is the ONLY thing you change per environment:
//   - local dev:        "http://localhost:8000"
//   - Cloudflare Pages: "https://api.market-stream.<your-domain>"
//   - sample-only mode: ""   ← the page never calls a backend; it just loops the recorded
//                              feed. Use this if the cluster/API isn't running, so the demo
//                              is still fully alive with zero backend.
window.MARKET_STREAM_CONFIG = {
  API_BASE_URL: "http://localhost:8000",
  SYMBOLS: ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
  HEALTH_TIMEOUT_MS: 1500, // how long to wait for the live probe before staying on sample
  OHLCV_POLL_MS: 5000,
  TICK_POLL_MS: 2000,
};
