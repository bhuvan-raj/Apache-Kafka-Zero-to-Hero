# 06 — Kafka Consumers

## Topics

- Consumer architecture
- Consumer groups
- Partition assignment
- Offset commits
- Automatic commits
- Manual commits
- Polling
- Rebalancing
- Consumer lag
- Scaling consumers
- Failure handling

## CLI Lab

```bash
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group orders-group
```

Check the group:

```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group orders-group
```

## Exercise

Run three consumers in one group against a topic with three partitions. Stop one consumer and observe the rebalance.
