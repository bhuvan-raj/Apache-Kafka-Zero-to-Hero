# 07 — Replication and High Availability

## Topics

- Replication factor
- Leader replica
- Follower replica
- ISR
- Leader election
- Broker failure
- `acks=all`
- `min.insync.replicas`
- Unclean leader election
- Durability

## Lab

Build a three-broker cluster, create a topic with replication factor three, stop one broker, and verify that the cluster continues serving traffic.

## Important Concept

Replication improves availability and durability, but it does not replace backups or a disaster-recovery strategy.
