# 05 — Kafka Producers

> 🎯 **Goal of this module:** Understand how producers actually work internally — batching, acknowledgments, retries, idempotency — and how to tune them for the durability/throughput trade-off your application needs.

---

## 📚 Topics

- [Producer architecture](#-producer-architecture)
- [Sending records](#-sending-records)
- [Producer configuration](#-producer-configuration)
- [`acks`](#-acks)
- [Retries](#-retries)
- [Batching](#-batching)
- [Compression](#-compression)
- [Message keys](#-message-keys)
- [Partitioners](#-partitioners)
- [Idempotent producer](#-idempotent-producer)
- [Delivery semantics](#-delivery-semantics)

---

## 🔹 Producer Architecture

A Kafka producer isn't a simple "send and forget" HTTP-style client — internally it's a small pipeline optimized for throughput:

```
 Application thread              Producer internals                Network
┌─────────────┐    .send()     ┌────────────────────┐   batches   ┌────────┐
│ your code    │ ─────────────► │ Serializer          │            │        │
│ producer.send│                │      ↓               │            │ Broker │
└─────────────┘                │ Partitioner          │            │        │
                                │      ↓               │            └────────┘
                                │ Record Accumulator   │  I/O thread
                                │  (per-partition       │ ─────────► sends
                                │   batching buffer)    │   batched requests
                                └────────────────────┘
```

**Key pieces:**
1. **Serializer** — converts your key/value objects (e.g. Java objects, JSON, Avro) into bytes.
2. **Partitioner** — decides which partition a record goes to (by key hash, or round-robin/sticky if no key).
3. **Record Accumulator** — buffers records per-partition into batches in memory, rather than sending one network request per record.
4. **I/O (sender) thread** — a separate background thread that pulls completed batches and sends them to the appropriate broker.

This design is what allows `producer.send()` to return almost immediately (asynchronously) while a background thread handles the actual network I/O and batching — critical for throughput.

---

## 🔹 Sending Records

Producers can send in three modes, trading off simplicity for throughput/reliability visibility:

| Mode | Behavior | When to use |
|------|----------|-------------|
| **Fire-and-forget** | `producer.send(record)` — don't wait for or check the result | Highest throughput, but risk silently losing messages on failure |
| **Synchronous** | `producer.send(record).get()` — block until the broker acknowledges | Simplicity and strong guarantees per-record, at the cost of throughput (waits per record) |
| **Asynchronous with callback** | `producer.send(record, callback)` — non-blocking, callback fires on ack/failure | Best of both — high throughput *and* you still find out about failures, typically the recommended default |

```java
producer.send(new ProducerRecord<>("orders", "customer-1", "OrderPlaced"),
    (metadata, exception) -> {
        if (exception != null) {
            // handle/log the failure — this is where you'd retry, alert, or dead-letter it
        } else {
            System.out.printf("Sent to partition %d at offset %d%n",
                metadata.partition(), metadata.offset());
        }
    });
```

---

## 🔹 Producer Configuration

A handful of configs govern almost everything about how a producer behaves. The most consequential ones (several detailed further below):

| Config | Purpose |
|--------|---------|
| `bootstrap.servers` | Initial broker(s) to connect to, to discover the rest of the cluster |
| `acks` | How many replicas must confirm a write before it's considered successful |
| `retries` | How many times to retry a failed send |
| `enable.idempotence` | Prevents duplicate messages caused by retries |
| `batch.size` | Max bytes to batch per partition before sending |
| `linger.ms` | Max time to wait, hoping to fill a batch, before sending anyway |
| `compression.type` | Compression algorithm applied to batches (`none`, `gzip`, `snappy`, `lz4`, `zstd`) |
| `max.in.flight.requests.per.connection` | How many unacknowledged requests can be outstanding at once (affects ordering guarantees under retries) |

---

## 🔹 `acks`

The `acks` setting controls **how many replicas must confirm receipt** before the producer considers a write successful — the single biggest lever for the durability-vs-latency trade-off.

| `acks` value | Behavior | Durability | Latency |
|---|---|---|---|
| `0` | Producer doesn't wait for any acknowledgment at all | ❌ Weakest — messages can be silently lost | ⚡ Fastest |
| `1` | Only the **partition leader** must acknowledge the write | ⚠️ Moderate — lost if leader fails before followers replicate it | 🏃 Fast |
| `all` (or `-1`) | **All in-sync replicas (ISR)** must acknowledge | ✅ Strongest — safe against single (or multiple, per replication factor) broker failure | 🐢 Slowest |

```
acks=0:    producer ──send──►                              (no wait, assumed success)

acks=1:    producer ──send──► [Leader] ──ack──► producer     (followers may not have it yet)

acks=all:  producer ──send──► [Leader] ──replicate──► [Follower ISR]
                                        ◄──all acked───
                               producer receives ack only after ISR confirms
```

**Rule of thumb:** use `acks=all` for anything where losing a message is unacceptable (orders, payments, audit events); `acks=1` or `0` only for high-volume, loss-tolerant data (e.g. metrics, logs where occasional gaps are fine).

---

## 🔹 Retries

Transient failures (leader election in progress, temporary network blip, broker restart) are common in a distributed system — `retries` tells the producer to automatically resend rather than fail immediately.

| Config | Purpose |
|---|---|
| `retries` | Number of retry attempts (modern Kafka defaults this very high — effectively "keep retrying") |
| `retry.backoff.ms` | Delay between retry attempts |
| `delivery.timeout.ms` | Overall time budget for a record to be acknowledged — bounds total retry time, after which `send()` finally fails |

**The catch:** naive retries can **reorder** messages. If message A fails and is being retried while message B (sent right after) succeeds on the first try, B may land before A. This is why retries are almost always discussed together with idempotency and `max.in.flight.requests.per.connection` (see [Idempotent producer](#-idempotent-producer) below).

---

## 🔹 Batching

Rather than sending one network request per message, the producer's Record Accumulator groups records destined for the same partition into a **batch**, sent as a single request.

Two settings control when a batch is sent:

| Config | Meaning |
|---|---|
| `batch.size` | Send the batch once it reaches this many bytes |
| `linger.ms` | ...or send it anyway after waiting this long, even if not full |

```
Without batching:  [msg]→network  [msg]→network  [msg]→network   (1 request per message)

With batching:     [msg][msg][msg] ──► one network request       (batch.size or linger.ms triggers send)
```

**Trade-off:** higher `linger.ms` increases per-message latency slightly (records wait longer before being sent) but dramatically improves throughput and compression efficiency (bigger batches compress better) — a classic latency-vs-throughput knob.

---

## 🔹 Compression

Kafka can compress entire batches before sending, configured via `compression.type`.

| Algorithm | Compression ratio | CPU cost | Notes |
|---|---|---|---|
| `none` | — | None | Default if unset |
| `gzip` | High | High | Best ratio, slowest — good for archival/low-throughput topics |
| `snappy` | Moderate | Low | Good general-purpose balance of speed and size |
| `lz4` | Moderate | Very low | Very fast, slightly larger output than snappy |
| `zstd` | High | Moderate | Best modern balance — often outperforms gzip on both speed and ratio |

Compression happens **per batch**, on the producer side, and stays compressed all the way to the consumer (brokers don't decompress/recompress — this is called "end-to-end compression"), saving network and disk I/O throughout the pipeline. Combined with [batching](#-batching), larger `linger.ms`/`batch.size` values give compression more data to work with, improving ratios further.

---

## 🔹 Message Keys

Already covered in depth in Module 04 — quick recap in the producer context: the key you set on a `ProducerRecord` is what the [partitioner](#-partitioners) uses to choose a partition, and therefore what determines per-key ordering.

---

## 🔹 Partitioners

The **partitioner** decides which partition a record lands in. Kafka's default partitioner:

- If a **key is present**: `partition = hash(key) % numPartitions` (technically using a "sticky" variant in modern versions, but conceptually this).
- If **no key**: uses the **sticky partitioner** — batches all keyless records to one randomly chosen partition for a while (to build efficient batches), then switches to another, rather than pure round-robin per-message.

You can also implement a **custom partitioner** (implementing the `Partitioner` interface) when you need routing logic beyond simple key hashing — e.g. routing high-priority messages to a dedicated partition, or geographic-based routing.

---

## 🔹 Idempotent Producer

**Idempotence** (`enable.idempotence=true`) solves the duplicate-message problem that plain retries can introduce.

**The problem without idempotence:**
```
1. Producer sends message X
2. Broker writes X and sends ack
3. Ack is lost in the network (broker never hears back that producer got it)
4. Producer times out, assumes failure, retries — sends X again
5. Broker now has X written TWICE
```

**How idempotence fixes it:** the producer is assigned a unique **Producer ID (PID)**, and tags every message with a **sequence number** per partition. The broker tracks the last sequence number it wrote per PID/partition, and **silently discards** any duplicate it recognizes — without erroring the producer.

```
enable.idempotence=true
```
enables this and, as a side effect, automatically sets safe defaults:
- `acks=all`
- `retries` = a high number (essentially unlimited within `delivery.timeout.ms`)
- `max.in.flight.requests.per.connection` ≤ 5 (preserves ordering even with retries, via broker-side sequence checking)

**Important scope:** idempotence guarantees *exactly-once delivery per partition, per producer session* — it does not by itself give you exactly-once across multiple partitions or across producer restarts. That broader guarantee requires **transactions** (covered in Module 17 — Advanced Kafka).

---

## 🔹 Delivery Semantics

Putting `acks`, retries, and idempotence together, Kafka producers can achieve one of three delivery guarantees:

| Semantic | Description | How to achieve it |
|---|---|---|
| **At-most-once** | Message might be lost, never duplicated | `acks=0` or `1`, no retries |
| **At-least-once** | Message is never lost, but might be duplicated | `acks=all` + retries enabled, idempotence **off** |
| **Exactly-once** | Message is neither lost nor duplicated (per partition, per producer session) | `acks=all` + `enable.idempotence=true` (extend to multi-partition/multi-topic exactly-once with transactions — Module 17) |

**Most production systems target at-least-once + idempotent producer** as the practical sweet spot — durable, no silent loss, and duplicates are eliminated at the broker level without extra application logic.

---

## ⚙️ Important Settings

```properties
acks=all
enable.idempotence=true
compression.type=snappy
```

This combination is a strong, common production default:
- `acks=all` — no data loss from a single broker failure.
- `enable.idempotence=true` — no duplicate messages from producer-side retries.
- `compression.type=snappy` — good throughput/CPU balance for most workloads (swap for `zstd` if you want a better ratio and can afford slightly more CPU).

---

## 🧪 CLI Lab

```bash
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Send several messages and inspect where they are stored.

### Step-by-step

```bash
# 1. Produce a few plain messages (type each line, Enter to send, Ctrl+C to exit)
bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```
At the prompt:
```
hello kafka
another message
final message
```

```bash
# 2. Inspect where they landed — describe the topic for leader/replica info
bin/kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092

# 3. Check the earliest/latest offsets per partition
bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic orders --time -1

# 4. Physically inspect the on-disk log segment (path depends on your log.dirs config)
ls /tmp/kraft-combined-logs/orders-0/
```

### Sample Result & Explanation

<details>
<summary>💡 Click to reveal expected output and analysis</summary>

Since no key was provided, the console producer's default partitioner (sticky partitioning) will send these keyless messages to **one partition at a time**, e.g. all three landing on Partition 1:

```
orders:1:3     ← GetOffsetShell output: topic:partition:latest-offset
orders:0:0
orders:2:0
```

Listing the log directory shows the actual segment files backing that partition:
```
$ ls /tmp/kraft-combined-logs/orders-1/
00000000000000000000.index
00000000000000000000.log
00000000000000000000.timeindex
leader-epoch-checkpoint
```

The `.log` file is where your three messages are physically stored as an append-only binary log — you can even inspect it with:
```bash
bin/kafka-dump-log.sh --files /tmp/kraft-combined-logs/orders-1/00000000000000000000.log --print-data-log
```
which will show each record's offset, timestamp, key (empty here), and value in human-readable form.

**Try extending this lab:**
- Re-run the producer with `--property "parse.key=true" --property "key.separator=:"` and send keyed messages — compare which partitions they land on versus the keyless run above.
- Set `--producer-property acks=0` on the console producer and pull the network cable (or just note conceptually) — compare to running with `acks=all` to reason about what could be lost in each case.
- Try `--producer-property compression.type=gzip` and compare the `.log` file's on-disk size for the same messages against `snappy`/no compression.

</details>

---

**◀ Previous:** [04 — Topics & Partitions](../04-Topics-and-Partitions/README.md) · **Next ▶** [06 — Kafka Consumers](../06-Kafka-Consumers/README.md)
