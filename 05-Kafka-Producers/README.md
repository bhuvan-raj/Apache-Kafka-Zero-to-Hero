# 05 — Kafka Producers

## Topics

- Producer architecture
- Sending records
- Producer configuration
- `acks`
- Retries
- Batching
- Compression
- Message keys
- Partitioners
- Idempotent producer
- Delivery semantics

## Important Settings

```properties
acks=all
enable.idempotence=true
compression.type=snappy
```

## CLI Lab

```bash
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Send several messages and inspect where they are stored.
