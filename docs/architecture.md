# Architecture

market-stream is the **streaming application layer** on top of the platform the
[`spark-k8s`](../../spark-k8s) repo provisions. It ingests a live market feed, processes it
as a stream, and lands it in the shared Apache Iceberg lakehouse.

## Runtime topology

```mermaid
flowchart TB
  subgraph ext[External]
    BIN["Yahoo Finance<br/>chart endpoint (polled)"]
    CF["Cloudflare Pages<br/>(static frontend)"]
  end

  subgraph ms[namespace: market-stream]
    PROD["producer<br/>(Deployment)"]
    RP["Redpanda<br/>Kafka API :9093<br/>Schema Registry :8081"]
    TR["Trino<br/>(iceberg catalog)"]
    API["web API<br/>(FastAPI, read-only)"]
    LB(["Hetzner LoadBalancer"])
  end

  subgraph sj[namespace: spark-jobs]
    SPARK["SparkApplication market-stream<br/>(Structured Streaming, restart=Always)"]
  end

  subgraph lh[namespace: lakehouse / identity — from spark-k8s]
    LK[("Lakekeeper REST catalog")]
    KC["Keycloak (OAuth2)"]
  end

  S3[("Hetzner Object Storage<br/>spark-k8s-lakehouse")]

  BIN -->|polled quotes| PROD -->|Avro| RP
  RP -->|subscribe| SPARK
  SPARK -->|append quotes_raw| LK
  SPARK -->|MERGE ohlcv_1m| LK
  SPARK -->|OAuth2| KC
  SPARK -->|Parquet + checkpoint| S3
  LK --> S3
  TR -->|REST + OAuth2| LK
  TR --> S3
  API --> TR
  CF -->|HTTPS, CORS| LB --> API
```

Two streaming queries share the Kafka topic: a stateless append (`quotes_raw`, exactly-once
via Kafka offsets in the checkpoint + Iceberg's atomic commits) and a stateful, watermarked
1-minute aggregation (`ohlcv_1m`, idempotent upsert via Iceberg `MERGE` on
`(symbol, window_start)`).

## Build-time pipeline

```mermaid
flowchart LR
  subgraph src[This repo]
    P["producer/<br/>(Python)"]
    SJ["streaming/jobs/<br/>+ schemas/"]
    W["web/api/<br/>(FastAPI)"]
  end
  P -->|"docker build -f producer/Dockerfile ."| PI["market-producer:latest"]
  SJ -->|"docker build -f streaming/Dockerfile ."| SI["spark-market-stream:3.5.3-iceberg1.7.1<br/>(apache/spark + Iceberg + Kafka + Avro + S3A jars)"]
  W -->|"docker build web/api"| WI["market-stream-api:latest"]
  PI & SI & WI -->|make push| GHCR["ghcr.io/shaked98/*"]
  GHCR -->|ansible-playbook site.yml| K8S["spark-k8s cluster"]
```

## The cloud seam

Only `infra/<cloud>/` is cloud-aware (and in reuse mode it's just a README pointing at the
spark-k8s cluster). `ansible/`, `producer/`, `streaming/` and `web/` operate on whatever
cluster + Lakekeeper endpoint they're handed — exactly the spark-k8s design. See
[infra/hetzner/README.md](../infra/hetzner/README.md).

## Local vs cluster — the one divergence

| | Local (`local/docker-compose.yml`) | Cluster (`ansible/`) |
|---|---|---|
| Object store | MinIO (`lakehouse` bucket) | Hetzner Object Storage (`spark-k8s-lakehouse`) |
| Catalog auth | none | Keycloak OAuth2 (`OAUTH_ENABLED=true`) |
| Spark | `spark-submit local[2]` | `SparkApplication` (Spark Operator) |
| Checkpoint | `s3a://lakehouse/...` (MinIO) | `s3a://spark-k8s-lakehouse/...` |
| Trino | container | Helm release |

Everything else — catalog name `lake`, warehouse `lakehouse`, table layout, S3FileIO data
path, the transform — is identical, so the local stack is a faithful rehearsal of production.
