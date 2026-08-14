# 15 — Kafka with Kubernetes

## Topics

- Kafka on Kubernetes
- Stateful workloads
- StatefulSets
- Persistent Volumes
- Services
- Storage
- Networking
- Advertised listeners
- Operators
- Strimzi
- Kafka CRDs
- Kafka users
- Kafka topics

## Recommended Approach

For teaching, use the **Strimzi Kafka Operator** so students learn the Kubernetes-native operational model.

## Lab

1. Install Strimzi.
2. Deploy a Kafka cluster.
3. Create a Kafka topic.
4. Deploy a producer.
5. Deploy a consumer.
6. Verify persistence.
7. Scale the cluster.
8. Test broker failure.

> Use the current Strimzi documentation for version-specific CRDs and manifests.
