# 10 — Kafka Connect

## Topics

- What Kafka Connect solves
- Source connectors
- Sink connectors
- Connector
- Task
- Worker
- Standalone mode
- Distributed mode
- Connector configuration
- Source → Kafka
- Kafka → destination

## Architecture

```text
Source System → Kafka Connect → Kafka → Kafka Connect → Destination
```

## Lab Ideas

- Database → Kafka
- Kafka → Elasticsearch
- Kafka → object storage

Use connector-specific documentation for the exact configuration required by the chosen connector.
