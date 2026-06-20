# tests/

Two layers, mirroring the sibling `spark-k8s` repo: fast offline checks + Python unit tests,
and a live end-to-end smoke test.

## Unit tests (pytest)

```bash
pip install -r requirements-dev.txt
pytest -v
```

- `producer/tests/` — the raw→Avro mapping, schema-invalid drop, key-by-symbol, backpressure
  retry, and the WS URL/backoff helpers. No broker required.
- `streaming/tests/` — the OHLCV/VWAP transform on a local SparkSession, asserting exact
  values. Skips cleanly if PySpark/Java isn't available.
- `tests/unit/` — the Avro schemas parse.

## `validate.sh` (offline)

```bash
tests/validate.sh
```
Parses all YAML/manifests + the Avro schemas, checks `secrets.sops.yaml` is encrypted and
`age.key` isn't tracked, and runs the unit tests when their deps are installed. Anything whose
tool is missing is SKIPPED, not failed — so it runs in Git Bash or WSL.

## `smoke-test.sh` (live end-to-end)

The headline test: **produce N synthetic ticks → assert rows land in Iceberg** (via Trino).

```bash
# Against the local stack (after `make local-up`):
tests/smoke-test.sh

# Against the cluster (point at port-forwarded / exposed endpoints):
TRINO_HOST=... TRINO_PORT=... KAFKA_BOOTSTRAP=... SCHEMA_REGISTRY_URL=... tests/smoke-test.sh
```
It publishes to a distinct symbol (`TEST`) so the assertions don't race the live Yahoo
feed, then polls Trino until `quotes_raw` has ≥ N rows and `ohlcv_1m` has ≥ 1 window. Requires
the producer deps + the `trino` client (both in `requirements-dev.txt`).
