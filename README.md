# Apache Kafka Zero to Hero

A practical, DevOps-focused Apache Kafka course covering Kafka fundamentals, architecture, producers, consumers, replication, administration, security, monitoring, Docker, Kubernetes, troubleshooting, and production practices.

## Course Modules

| # | Module | Topics |
|---|---|---|
| 01 | Introduction to Kafka | Kafka, use cases, messaging, event-driven architecture |
| 02 | Kafka Architecture | Brokers, topics, partitions, offsets, consumers, replication |
| 03 | Installation & Setup | KRaft, single broker, multi-broker cluster, CLI |
| 04 | Topics & Partitions | Topic management, partitions, keys, offsets |
| 05 | Producers | Producer configuration, acknowledgements, retries, idempotency |
| 06 | Consumers | Consumer groups, offsets, commits, rebalancing, lag |
| 07 | Replication & HA | Leaders, followers, ISR, leader election, durability |
| 08 | Storage & Retention | Logs, segments, retention, compaction, tombstones |
| 09 | Administration | CLI, configs, groups, reassignment, scaling |
| 10 | Kafka Connect | Source/sink connectors, workers, distributed mode |
| 11 | Schema & Serialization | JSON, Avro, Protobuf, Schema Registry |
| 12 | Security | TLS, SASL, ACLs, authentication, authorization |
| 13 | Monitoring | Metrics, JMX, Prometheus, Grafana, alerting |
| 14 | Docker | Kafka with Docker Compose |
| 15 | Kubernetes | Strimzi, Stateful workloads, persistence, networking |
| 16 | Error Handling | Retry topics, DLT, poison messages, idempotency |
| 17 | Advanced Kafka | Delivery semantics, transactions, Streams, CQRS |
| 18 | Troubleshooting | Production failures and diagnostic workflows |
| 19 | Production | Sizing, capacity, HA, DR, performance, best practices |
| 20 | Project | Event-driven e-commerce platform |

## Suggested Learning Path

1. Learn Kafka concepts and architecture.
2. Build a local KRaft cluster.
3. Work with topics, partitions, producers, and consumers.
4. Build a multi-broker cluster and test failures.
5. Learn administration and monitoring.
6. Secure Kafka.
7. Run Kafka with Docker and Kubernetes.
8. Practice production troubleshooting.
9. Complete the capstone project.

## Prerequisites

- Basic Linux
- Basic networking
- Git
- Docker fundamentals
- Kubernetes fundamentals
- Basic programming knowledge
- Basic Prometheus/Grafana knowledge

## Repository Structure

```text
Apache-Kafka-Zero-to-Hero/
├── README.md
├── LICENSE
├── docs/
├── 01-Introduction-to-Kafka/
├── 02-Kafka-Architecture/
├── 03-Installation-and-Setup/
├── 04-Topics-and-Partitions/
├── 05-Kafka-Producers/
├── 06-Kafka-Consumers/
├── 07-Replication-and-High-Availability/
├── 08-Storage-and-Retention/
├── 09-Kafka-Administration/
├── 10-Kafka-Connect/
├── 11-Schema-and-Serialization/
├── 12-Kafka-Security/
├── 13-Kafka-Monitoring/
├── 14-Kafka-with-Docker/
├── 15-Kafka-with-Kubernetes/
├── 16-Error-Handling-and-DLT/
├── 17-Advanced-Kafka/
├── 18-Kafka-Troubleshooting/
├── 19-Production-Kafka/
└── 20-Real-World-Project/
```

## Goal

By the end of this repository, students should be able to deploy, configure, use, monitor, secure, troubleshoot, and operate Apache Kafka in a production-oriented DevOps environment.
