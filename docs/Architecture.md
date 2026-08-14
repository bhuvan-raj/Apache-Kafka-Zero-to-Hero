# Kafka Architecture Reference

```text
                         +----------------+
                         |    Producer    |
                         +-------+--------+
                                 |
                                 v
                    +--------------------------+
                    |      Kafka Cluster       |
                    |                          |
                    | Broker 1  Broker 2  B3   |
                    |    |         |       |    |
                    |  Topic / Partitions       |
                    +------------+-------------+
                                 |
                                 v
                       +----------------+
                       | Consumer Group |
                       +----------------+
```

## Core Components

- **Broker** — Kafka server that stores and serves records.
- **Topic** — Logical stream of records.
- **Partition** — Ordered append-only log inside a topic.
- **Offset** — Position of a record within a partition.
- **Producer** — Publishes records.
- **Consumer** — Reads records.
- **Consumer Group** — Coordinates consumers so partitions can be processed in parallel.
- **Replication** — Copies partitions across brokers.
- **ISR** — Replicas currently considered in sync.

## Key Rule

Within a consumer group, a partition is assigned to only one consumer at a time. A topic with more partitions can therefore provide more consumer parallelism.
