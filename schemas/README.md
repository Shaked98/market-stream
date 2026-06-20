# schemas/ — message contracts

The pipeline is **contract-first**: every message on Kafka/Redpanda carries an Avro payload that
conforms to a schema registered in Redpanda's Confluent-compatible **Schema Registry**. The
producer serializes against the registered schema; the Spark job decodes with `from_avro`. A
malformed or schema-violating message is logged and dropped — never silently mis-parsed.

## Why Avro (not JSON Schema)

- **Compact binary on the wire** — matters at trade-tick volume.
- **Native registry support** — `confluent-kafka[avro]` (producer) and Spark's `from_avro`
  (consumer) both speak the Confluent wire format (magic byte + 4-byte schema id + payload).
- **First-class evolution** — add nullable fields with defaults and old/new consumers keep
  working. We register subjects with `BACKWARD` compatibility (the default).

## Schemas

| File | Record | Topic | Subject (`TopicNameStrategy`) |
|------|--------|-------|-------------------------------|
| `trade.avsc` | `Trade` | `market.trades` | `market.trades-value` |
| `ohlcv.avsc` | `Ohlcv` | `market.ohlcv` *(optional)* | `market.ohlcv-value` |

`trade.avsc` is the live contract the producer publishes. `ohlcv.avsc` documents the schema of
the aggregated Iceberg table `lake.market.ohlcv_1m`; it doubles as the registry subject if the
rollups are ever published to Kafka to feed the live web tape directly.

## Conventions

- **Subject naming:** `TopicNameStrategy` → `<topic>-value`.
- **Keys:** messages are keyed by `symbol` (a plain string, not Avro) so all ticks for a symbol
  land on one partition, preserving per-symbol order for the windowed aggregation downstream.
- **Precision:** `price`/`quantity` travel as **strings** (exact exchange precision, no float
  drift); Spark casts them to `decimal(38,8)`. Monetary OHLCV fields are Avro `decimal(38,8)`.
- **Time:** all timestamps are `long` with `logicalType: timestamp-millis` (epoch millis UTC).
  `trade_time` is the event-time / watermark column.
- **Evolution policy:** only backward-compatible changes (add optional fields with defaults).
  Breaking changes get a new topic + subject, never an in-place mutation.

## Validate locally

```bash
python -c "import fastavro; fastavro.schema.load_schema('schemas/trade.avsc'); \
           fastavro.schema.load_schema('schemas/ohlcv.avsc'); print('schemas OK')"
```
