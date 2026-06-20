# schemas/ — message contracts

The pipeline is **contract-first**: every message on Kafka/Redpanda carries an Avro payload
that conforms to a schema registered in Redpanda's Confluent-compatible **Schema Registry**.
The producer serializes against the registered schema; the Spark job decodes with `from_avro`.
A malformed or schema-violating message is logged and dropped — never silently mis-parsed.

## Why Avro (not JSON Schema)

- **Compact binary on the wire** and a Confluent-compatible registry both the Python producer
  (`confluent-kafka[avro]`) and Spark (`from_avro`) speak natively (magic byte + schema id +
  payload).
- **First-class evolution** — add nullable fields with defaults and old/new consumers keep
  working. Subjects are registered `BACKWARD`-compatible (the default).

## Schemas

| File | Record | Topic | Subject (`TopicNameStrategy`) |
|------|--------|-------|-------------------------------|
| `quote.avsc` | `Quote` | `market.quotes` | `market.quotes-value` |
| `ohlcv.avsc` | `Ohlcv` | `market.ohlcv` *(optional)* | `market.ohlcv-value` |

`quote.avsc` is the live contract the producer publishes — a **polled stock quote** (free,
keyless Yahoo Finance; see [docs/source-choice.md](../docs/source-choice.md)). `ohlcv.avsc`
documents the aggregated Iceberg table `lake.market.ohlcv_1m`.

## Conventions

- **Subject naming:** `TopicNameStrategy` → `<topic>-value`.
- **Keys:** messages are keyed by `symbol` (plain string) so all quotes for a symbol land on
  one partition, preserving per-symbol order for the windowed aggregation downstream.
- **Precision:** `price`/`volume` travel as **strings** (exact, no float drift); Spark casts
  them to `decimal(38,8)`. Monetary OHLCV fields are Avro `decimal(38,8)`.
- **Volume:** `volume` is the day-volume *increment* between polls (≥ 0), used to weight VWAP;
  `day_volume` is the running cumulative for the session.
- **Time:** timestamps are `long` `timestamp-millis` (epoch millis UTC). `quote_time` (poll
  time) is the event-time / watermark column.
- **Evolution policy:** only backward-compatible changes (add optional fields with defaults).
  Breaking changes get a new topic + subject, never an in-place mutation.

## Validate locally

```bash
python -c "import fastavro; fastavro.schema.load_schema('schemas/quote.avsc'); \
           fastavro.schema.load_schema('schemas/ohlcv.avsc'); print('schemas OK')"
```
