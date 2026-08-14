# 13 — Kafka Monitoring

## Topics

- Kafka metrics
- Broker metrics
- Producer metrics
- Consumer metrics
- Consumer lag
- Throughput
- Request latency
- Under-replicated partitions
- Offline partitions
- ISR
- Disk usage
- CPU and memory
- Network utilization
- Alerting

## Architecture

```text
Kafka
  ↓
JMX / Metrics Exporter
  ↓
Prometheus
  ↓
Grafana
```

## Suggested Dashboards

- Broker health
- Consumer lag
- Topic throughput
- Partition health
- Under-replicated partitions
- Disk utilization

## Lab

Deploy Kafka, expose metrics, collect them with Prometheus, build Grafana panels, and create alerts for consumer lag and unavailable partitions.
