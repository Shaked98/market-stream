#!/usr/bin/env bash
# Live end-to-end smoke test: produce N synthetic ticks → wait for the streaming micro-batch
# → assert rows landed in Iceberg (queried via Trino). Codifies the manual verification.
#
# Against the LOCAL stack (after `make local-up`):
#   tests/smoke-test.sh
# Against the CLUSTER (port-forward Trino + Redpanda first, or set the hosts):
#   TRINO_HOST=... TRINO_PORT=... KAFKA_BOOTSTRAP=... SCHEMA_REGISTRY_URL=... tests/smoke-test.sh
#
# Env: N (default 50), SMOKE_SYMBOL (default TEST), PYTHON (override interpreter).
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PY="${PYTHON:-}"
if [ -z "$PY" ]; then
  for p in .venv/Scripts/python.exe .venv/bin/python python3 python; do
    if command -v "$p" >/dev/null 2>&1; then PY="$p"; break; fi
  done
fi

N="${N:-50}"
SMOKE_SYMBOL="${SMOKE_SYMBOL:-TEST}"
export TRINO_HOST="${TRINO_HOST:-localhost}" TRINO_PORT="${TRINO_PORT:-8080}"
export TRINO_CATALOG="${TRINO_CATALOG:-iceberg}" TRINO_SCHEMA="market"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
export SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:18081}"

pass=0; fail=0
ok() { printf '  [ OK ] %s\n' "$1"; pass=$((pass + 1)); }
no() { printf '  [FAIL] %s\n' "$1"; fail=$((fail + 1)); }
section() { printf '\n== %s ==\n' "$1"; }

section "Produce $N synthetic ticks ($SMOKE_SYMBOL → $KAFKA_BOOTSTRAP)"
if N="$N" SMOKE_SYMBOL="$SMOKE_SYMBOL" "$PY" tests/produce_synthetic.py; then
  ok "produced synthetic ticks"
else
  no "synthetic producer failed"; echo; echo "RESULT: cannot continue"; exit 1
fi

section "Assert rows land in Iceberg (via Trino @ $TRINO_HOST:$TRINO_PORT)"
trades=0; ohlcv=0
printf '  waiting for the streaming micro-batch'
for _ in $(seq 1 30); do
  trades=$("$PY" tests/smoke_query.py \
    "SELECT count(*) FROM market.quotes_raw WHERE symbol='$SMOKE_SYMBOL'" 2>/dev/null || echo 0)
  ohlcv=$("$PY" tests/smoke_query.py \
    "SELECT count(*) FROM market.ohlcv_1m WHERE symbol='$SMOKE_SYMBOL'" 2>/dev/null || echo 0)
  if [ "${trades:-0}" -ge "$N" ] 2>/dev/null && [ "${ohlcv:-0}" -ge 1 ] 2>/dev/null; then break; fi
  printf '.'; sleep 5
done
echo

if [ "${trades:-0}" -ge "$N" ] 2>/dev/null; then
  ok "quotes_raw has >= $N rows ($trades)"
else
  no "quotes_raw rows ($trades, want >= $N)"
fi
if [ "${ohlcv:-0}" -ge 1 ] 2>/dev/null; then
  ok "ohlcv_1m has >= 1 window ($ohlcv)"
else
  no "ohlcv_1m windows ($ohlcv, want >= 1)"
fi

printf '\n== RESULT: %d passed, %d failed ==\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
