# 14 — Kafka with Docker

## Topics

- Kafka containers
- Docker networking
- Environment configuration
- Advertised listeners
- Persistent volumes
- Docker Compose
- Multi-broker Kafka

## Lab

Create a Docker Compose environment containing:

- Three Kafka brokers
- Persistent storage
- A test topic
- Producer and consumer clients

## Important

Kafka networking depends heavily on listener and advertised-listener configuration. Test connectivity from both inside and outside the Docker network.
