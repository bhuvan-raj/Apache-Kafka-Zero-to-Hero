# 04 — Topics and Partitions

## Topics

- Creating topics
- Describing topics
- Deleting topics
- Partitions
- Partition ordering
- Message keys
- Key-based partitioning
- Offsets
- Partition scaling
- Partition planning

## Commands

```bash
bin/kafka-topics.sh --create --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

bin/kafka-topics.sh --describe \
  --topic orders \
  --bootstrap-server localhost:9092
```

## Lab

Create a topic with three partitions, publish keyed messages, and inspect partition assignment.
