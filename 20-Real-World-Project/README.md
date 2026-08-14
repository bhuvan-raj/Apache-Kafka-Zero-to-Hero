# 20 — Real-World Project: Event-Driven E-Commerce

## Objective

Build an event-driven e-commerce platform using Kafka.

## Architecture

```text
                    +----------------+
                    | Order Service  |
                    +-------+--------+
                            |
                            v
                      +-----------+
                      |   Kafka   |
                      +-----------+
                       /    |    \
                      /     |     \
                     v      v      v
                Payment  Inventory  Notification
                Service   Service      Service
                   |         |           |
                   v         v           v
                Database  Database    Email/SMS
```

## Requirements

- Kafka cluster
- Multiple brokers
- Topics
- Partitions
- Replication
- Producer application
- Multiple consumer groups
- Consumer scaling
- Retry mechanism
- Dead Letter Topic
- Schema Registry
- Kafka Connect
- Security
- Prometheus
- Grafana
- Docker
- Kubernetes
- Failure testing
- Troubleshooting

## Suggested Topics

```text
orders
payments
inventory
notifications
orders-retry
orders-dlt
```

## Project Milestones

### Phase 1
Deploy Kafka and create topics.

### Phase 2
Build producer and consumer services.

### Phase 3
Introduce multiple consumer groups.

### Phase 4
Add replication and failure testing.

### Phase 5
Add retry and DLT handling.

### Phase 6
Add monitoring with Prometheus and Grafana.

### Phase 7
Containerize the application.

### Phase 8
Deploy Kafka and applications on Kubernetes.

### Phase 9
Secure Kafka.

### Phase 10
Perform production troubleshooting drills.
