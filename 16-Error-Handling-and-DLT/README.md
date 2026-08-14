# 16 — Error Handling and Dead Letter Topics

## Topics

- Processing failures
- Retry mechanisms
- Retry topics
- Dead Letter Topic
- Poison messages
- Backoff
- Reprocessing
- Idempotent consumers

## Architecture

```text
orders
  ↓
consumer
  ↓
processing error
  ↓
orders-retry
  ↓
still failing
  ↓
orders-dlt
```

## Lab

Build a consumer that intentionally fails for invalid records and routes failed records through a retry topic to a dead-letter topic.
