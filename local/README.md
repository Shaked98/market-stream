# local/ — the laptop pipeline

The **primary verification path**. One `docker compose` stack runs the whole pipeline with
zero cloud cost: Redpanda + Schema Registry, MinIO, Lakekeeper, Trino, the producer (pulling
**real** live Binance ticks), the Spark streaming job, and the read-only web API.

## Run it

```bash
cp .env.example .env          # from the repo root; dev defaults, nothing secret
make local-up                 # docker compose up -d --build  (first build pulls Spark — a few min)
make local-logs               # watch the producer + spark-job
```

| Service | URL | Notes |
|---------|-----|-------|
| Redpanda Console | http://localhost:8085 | watch `market.trades` fill with live ticks |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin`; data under `lakehouse/` |
| Lakekeeper UI | http://localhost:8181/ui | the Iceberg REST catalog |
| Trino | http://localhost:8080 | catalog `iceberg` |
| Web API | http://localhost:8000/healthz | the demo's read API |

Confirm data is landing in Iceberg:

```bash
docker exec -it market-stream-trino-1 trino --catalog iceberg --execute \
  "SELECT count(*) FROM market.trades_raw"
docker exec -it market-stream-trino-1 trino --catalog iceberg --execute \
  "SELECT symbol, window_start, open, high, low, close, vwap, trade_count \
   FROM market.ohlcv_1m ORDER BY window_start DESC LIMIT 10"
```

Open the demo against the local API:

```bash
python -m http.server -d ../web/frontend 8001   # → http://localhost:8001
```

(The web API's CORS is set to `http://localhost:8001` to match; serve the frontend there.)

```bash
make local-down               # stop;  add ARGS=-v to wipe the volumes for a clean slate
```

## How this differs from the cluster (one intentional divergence)

The local Lakekeeper runs **without OAuth2** (same as `LakeHouse/local`), so the Spark job is
launched with `OAUTH_ENABLED=false` and skips the Keycloak client-credential. On the cluster,
`OAUTH_ENABLED=true` and the credential is injected from the `oidc-spark` Secret. Everything
else — the catalog name `lake`, warehouse `lakehouse`, table layout, S3FileIO data path, S3A
checkpoint — is identical, so what you verify locally is what runs in production.

## First-run timing

The `spark-job` image build downloads the Iceberg/Kafka/Avro/S3A jars; the first `make local-up`
takes a few minutes. Subsequent runs are cached. The producer streams continuously; the Spark
micro-batch trigger means OHLCV rows appear within ~1–2 minutes of the first ticks.
