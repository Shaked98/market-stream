"""Generate the bundled sample feed (web/frontend/sample/replay.json).

This is the recorded slice the demo falls back to when the live backend is offline (or the
market is closed), so the page is always populated and moving. It's a deterministic
random-walk (seeded) standing in for a real capture; replace replay.json with a genuine
recording any time — the frontend doesn't care how it was produced. Regenerate with:
  python web/frontend/sample/_generate.py
"""

import datetime as dt
import json
import random
from pathlib import Path

random.seed(42)

SEEDS = {"AAPL": 190.0, "GOOG": 140.0, "MSFT": 430.0}
WINDOWS = 12          # 1-minute windows of history
TICKS_PER_WINDOW = 12
NOW = dt.datetime(2026, 6, 20, 16, 0, 0, tzinfo=dt.timezone.utc)

out = {"generated_at": NOW.isoformat(), "symbols": list(SEEDS), "ticks": {}, "ohlcv": {}}

for sym, seed_price in SEEDS.items():
    price = seed_price
    ticks: list[dict] = []
    ohlcv: list[dict] = []
    for w in range(WINDOWS):
        win_start = NOW - dt.timedelta(minutes=WINDOWS - w)
        open_p = price
        hi = lo = price
        vol = qv = 0.0
        for i in range(TICKS_PER_WINDOW):
            price = max(0.01, price * (1 + random.uniform(-0.0008, 0.0008)))  # stocks move less
            shares = random.randint(50, 1500)
            tt = win_start + dt.timedelta(seconds=i * 5)
            ticks.append(
                {"t": int(tt.timestamp() * 1000), "price": round(price, 2), "quantity": shares}
            )
            hi, lo = max(hi, price), min(lo, price)
            vol += shares
            qv += price * shares
        ohlcv.append(
            {
                "window_start_ms": int(win_start.timestamp() * 1000),
                "open": round(open_p, 2),
                "high": round(hi, 2),
                "low": round(lo, 2),
                "close": round(price, 2),
                "volume": round(vol, 0),
                "quote_volume": round(qv, 2),
                "vwap": round(qv / vol, 2) if vol else round(price, 2),
                "trade_count": TICKS_PER_WINDOW,
            }
        )
    out["ticks"][sym] = ticks
    out["ohlcv"][sym] = ohlcv

dest = Path(__file__).with_name("replay.json")
dest.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")
print(f"wrote {dest} ({dest.stat().st_size} bytes)")
