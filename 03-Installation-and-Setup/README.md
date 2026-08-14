# 03 — Installation and Setup

## Topics

- Kafka with KRaft
- Kafka installation
- Kafka directory structure
- Configuration
- Starting and stopping Kafka
- Single-broker setup
- Multi-broker setup
- Kafka CLI
- Kafka logs

## Basic Workflow

```bash
# Start Kafka using the distribution's configured KRaft setup
bin/kafka-server-start.sh config/server.properties

# Create a topic
bin/kafka-topics.sh --create \
  --topic demo \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# List topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

> Commands can differ slightly between Kafka releases. Always check the version-specific documentation shipped with the distribution.
