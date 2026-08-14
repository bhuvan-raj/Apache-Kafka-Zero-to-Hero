# 18 — Kafka Troubleshooting

## Scenarios

### Broker problems
- Broker unavailable
- Broker not joining cluster
- Controller/quorum problems

### Producer problems
- Cannot connect
- Timeout
- Not enough replicas
- Authorization failure

### Consumer problems
- No messages received
- Consumer lag increasing
- Rebalance loops
- Offset problems

### Cluster problems
- Under-replicated partitions
- Offline partitions
- ISR shrinking
- Disk full
- High CPU
- High memory
- High network utilization

## Troubleshooting Workflow

1. Identify the symptom.
2. Check broker health.
3. Check topic and partition state.
4. Check consumer group state.
5. Check logs.
6. Check metrics.
7. Check network connectivity.
8. Check storage.
9. Identify the root cause.
10. Apply and validate the fix.

## Lab

Simulate at least five failures and document:

- Symptom
- Commands used
- Root cause
- Fix
- Preventive monitoring/alert
