# 08 — Storage and Retention

## Topics

- Append-only logs
- Log segments
- Offsets
- Retention by time
- Retention by size
- `retention.ms`
- `retention.bytes`
- Cleanup policies
- Log compaction
- Tombstone records

## Lab

Create a test topic with a short retention period, publish records, and observe how retention affects stored data.

Example:

```bash
bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name test-retention \
  --alter \
  --add-config retention.ms=60000
```
