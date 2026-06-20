# Stream engine — why Spark Structured Streaming (not Flink)

Both Apache Spark Structured Streaming and Apache Flink can do this job. For *this* project,
Spark wins decisively — mostly for reasons of platform reuse, not raw capability.

## Why Spark here

1. **The platform already runs Spark.** The sibling [`spark-k8s`](../../spark-k8s) repo
   provides a Kubeflow Spark Operator, a custom Spark+Iceberg image, the `spark-jobs`
   namespace + RBAC, and the OAuth2/S3 secret wiring. market-stream's streaming job is a
   `SparkApplication` that drops straight onto that — near-zero new infrastructure. Adding
   Flink would mean a second runtime (Flink Operator, image, state backend) for no benefit.
2. **First-class Iceberg sink.** Iceberg's Spark runtime gives `MERGE INTO` for the idempotent
   OHLCV upsert and an atomic streaming append for raw ticks, all through the same Lakekeeper
   REST catalog the batch jobs use. One catalog, one set of tables, two engines.
3. **Latency requirements are modest.** This is a 1-minute OHLCV rollup for a recruiter-facing
   dashboard. Spark's micro-batch latency (seconds) is far inside budget; Flink's
   true-per-event, sub-second latency would be capability we'd never use.
4. **One language, one mental model.** The transforms are plain PySpark DataFrame code,
   unit-tested on a local SparkSession (`streaming/tests/`) and reusable in batch backfills.

## What Flink would buy (and why we don't need it)

- **Sub-second, true streaming latency** and fine-grained event-time control — overkill for
  minute-bars.
- **More expressive state / CEP** (pattern detection across events) — not needed for OHLCV.
- **Lower per-event overhead at very high throughput** — our volume (a few symbols' trades)
  is comfortably within Spark micro-batches.

## Honest trade-offs of the Spark choice

- Micro-batch means OHLCV windows update on the trigger interval, not instantly. Fine for a
  1-minute rollup.
- Two streaming queries each read the Kafka topic (stateless append vs. stateful aggregation
  can't share one query), so Kafka is read twice. Accepted for the clean separation; at this
  volume it's negligible.

**Conclusion:** reuse beats novelty. Spark Structured Streaming gives a production-grade
pipeline with idempotent Iceberg writes and checkpointed fault tolerance, on infrastructure
that already exists.
