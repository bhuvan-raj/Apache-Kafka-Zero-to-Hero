# 06 — Kafka Consumers

> 🎯 **Goal of this module:** Understand how consumers coordinate through consumer groups, how offsets are committed, and what actually happens — mechanically — during a rebalance.

---

## 📚 Topics

- [Consumer architecture](#-consumer-architecture)
- [Consumer groups](#-consumer-groups)
- [Partition assignment](#-partition-assignment)
- [Offset commits](#-offset-commits)
- [Automatic commits](#-automatic-commits)
- [Manual commits](#-manual-commits)
- [Polling](#-polling)
- [Rebalancing](#-rebalancing)
- [Consumer lag](#-consumer-lag)
- [Scaling consumers](#-scaling-consumers)
- [Failure handling](#-failure-handling)

---

## 🔹 Consumer Architecture

A Kafka consumer is a **pull-based** client — unlike many messaging systems that push messages to subscribers, Kafka consumers actively poll brokers for new data.

```
┌──────────────┐   poll()    ┌──────────────────┐
│  Consumer     │ ──────────► │ Partition Leader   │
│  application  │ ◄────────── │ (broker)           │
└──────────────┘   records   └──────────────────┘
       │
       ▼
  process records
       │
       ▼
  commit offset (mark progress)
```

**Why pull instead of push?** The consumer controls its own pace — it can poll as fast or slow as it can process, rather than being overwhelmed by a broker pushing data faster than it can handle. This is fundamental to how Kafka avoids consumer overload without complex flow-control protocols.

Internally, a consumer maintains a **fetch buffer** per assigned partition, prefetching batches of records ahead of when the application actually calls `poll()`, to minimize latency between calls.

---

## 🔹 Consumer Groups

Recap + practical angle (conceptually introduced in Module 02): a **consumer group** is an identifier (`group.id`) shared by multiple consumer instances that split the work of reading a topic's partitions between them.

```
Topic "orders" — 3 partitions        Group: "orders-group"

Partition 0 ───────────────────────► Consumer 1
Partition 1 ───────────────────────► Consumer 2
Partition 2 ───────────────────────► Consumer 3
```

- **Within a group:** each partition goes to exactly one consumer — this is how Kafka achieves parallel processing while preserving per-partition order.
- **Across groups:** completely independent — two different `group.id`s each get their own full copy of the data stream and their own offset tracking.
- The Kafka broker responsible for coordinating a group's membership and offsets is called the **group coordinator**.

---

## 🔹 Partition Assignment

When consumers join a group, Kafka must decide **which consumer gets which partitions**. This is governed by `partition.assignment.strategy`, with a few built-in strategies:

| Strategy | Behavior |
|---|---|
| `RangeAssignor` (default, legacy) | Assigns contiguous partition ranges per topic to each consumer — can lead to uneven load if a consumer subscribes to many topics |
| `RoundRobinAssignor` | Spreads all partitions across all consumers round-robin, generally more even |
| `StickyAssignor` | Like round-robin, but minimizes partition movement across rebalances (keeps prior assignments where possible) |
| `CooperativeStickyAssignor` | Sticky assignment **plus** incremental rebalancing — consumers keep processing unaffected partitions during a rebalance instead of a full stop-the-world pause (see [Rebalancing](#-rebalancing)) |

**Practical guidance:** `CooperativeStickyAssignor` is generally the best default for modern Kafka clients — it minimizes both partition churn and rebalance-related downtime.

---

## 🔹 Offset Commits

Committing an offset tells Kafka "I've successfully processed up to this point" for a given partition — stored in the internal `__consumer_offsets` topic, keyed by `(group.id, topic, partition)`.

```
Partition 0:  [msg0][msg1][msg2][msg3][msg4]
                                  ▲
                        committed offset = 3
                        (meaning: next poll should start at offset 4)
```

If the consumer restarts, or a rebalance reassigns this partition to a different consumer in the group, the new owner resumes from the **last committed offset** — not from the beginning, and not from wherever the old consumer happened to be mid-processing.

---

## 🔹 Automatic Commits

With `enable.auto.commit=true` (the default), the consumer client automatically commits the latest offsets returned by `poll()`, in the background, every `auto.commit.interval.ms` (default 5000ms).

**The trade-off:**
```
poll() returns records [10, 11, 12]
   │
   ▼
application starts processing record 10...
   │
   ▼
   ⏱ auto-commit fires (every auto.commit.interval.ms) — commits offset 12 (all 3, even if not done!)
   │
   ▼
application crashes while still processing record 11
   │
   ▼
On restart: consumer resumes from committed offset 12 → records 11 and 12 are SKIPPED, never reprocessed
```

Auto-commit is simple but **can silently lose messages** if the app crashes between poll and finishing processing, because it commits based on what was *fetched*, not what was *actually finished*. It's fine for workloads where occasional message loss is tolerable (e.g. best-effort metrics); risky for anything requiring at-least-once processing guarantees.

---

## 🔹 Manual Commits

Setting `enable.auto.commit=false` puts the application in control of exactly when an offset is considered "done."

```java
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
    for (ConsumerRecord<String, String> record : records) {
        process(record);          // do the actual work first
    }
    consumer.commitSync();        // only commit AFTER processing succeeds
}
```

| Method | Behavior |
|---|---|
| `commitSync()` | Blocks until the broker acknowledges the commit; safest, but adds latency |
| `commitAsync()` | Non-blocking, faster; requires a callback to handle failures, and commit ordering isn't guaranteed under retries |

**Best practice:** commit only after successfully processing a batch (or, for stricter guarantees, after each record) — this is what enables genuine **at-least-once** processing, at the cost of writing a bit more application logic than relying on auto-commit.

---

## 🔹 Polling

`poll()` is the heartbeat of a Kafka consumer — it does more than just fetch records:

- Fetches new records for all partitions currently assigned to this consumer.
- Sends **heartbeats** to the group coordinator, signaling "I'm still alive" (in newer clients, heartbeating runs on a separate background thread, but the group membership is still tied to regular `poll()` calls in older client versions/configs).
- Triggers rebalance participation if group membership has changed.

**Critical setting:** `max.poll.interval.ms` — the maximum time allowed between two `poll()` calls before the coordinator assumes the consumer is dead/stuck and kicks it out of the group, triggering a rebalance. If your per-batch processing can be slow (e.g. calling a slow downstream API), either increase this value or reduce `max.poll.records` so each batch is processed quickly enough to keep polling regularly.

---

## 🔹 Rebalancing

A **rebalance** is the process of redistributing partition ownership among the consumers in a group — triggered by a consumer joining, leaving (including crashing or exceeding `max.poll.interval.ms`), or a topic's partition count changing.

**Eager rebalancing (older default):**
```
1. Rebalance triggered
2. ALL consumers in the group revoke ALL their partitions (stop-the-world)
3. Group coordinator computes new assignment
4. Partitions reassigned to consumers
5. Consumers resume processing

              ⚠️ Every partition is unavailable to any consumer during steps 2-4
```

**Cooperative (incremental) rebalancing** (`CooperativeStickyAssignor`):
```
1. Rebalance triggered
2. Only partitions that actually NEED to move are revoked
3. Unaffected consumers keep processing their existing partitions the whole time
4. Only the affected partitions are reassigned

              ✅ Minimal disruption — most of the group keeps working
```

**Practical impact:** with eager rebalancing, every group membership change (even a single consumer restarting for a deploy) pauses the *entire* group momentarily. Cooperative rebalancing is strongly recommended for any latency-sensitive consumer group.

---

## 🔹 Consumer Lag

**Lag** is the gap between the latest offset produced to a partition and the offset a consumer group has committed — i.e., how far behind the consumer is.

```
Partition 0:  latest offset = 1000
              committed offset = 940
                                       lag = 1000 - 940 = 60 messages behind
```

Lag is the single most important **health metric** for a consumer group:
- **Steady, near-zero lag** → consumer is keeping up with production.
- **Growing lag** → consumer can't keep pace (too slow, too few instances, or downstream bottleneck) — needs investigation or scaling.
- **Spiky-then-recovering lag** → normal, e.g., after a deploy-triggered rebalance or a traffic burst.

Checked via:
```bash
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group orders-group
```
which shows a `LAG` column per partition — also the primary metric fed into monitoring/alerting pipelines (Module 13).

---

## 🔹 Scaling Consumers

Recap with an operational lens: scaling a consumer group means adding/removing consumer instances, bounded by partition count.

| Scenario | Result |
|---|---|
| Consumers < Partitions | Some consumers handle multiple partitions each |
| Consumers = Partitions | Perfectly balanced, 1 partition per consumer |
| Consumers > Partitions | Extra consumers sit **idle** — Kafka cannot split a single partition across multiple consumers within the same group |

**Practical implication:** if you need more processing parallelism than your current partition count allows, you must increase partitions (Module 04) — simply adding more consumer instances beyond partition count buys you nothing (only redundancy for faster failover, not extra throughput).

---

## 🔹 Failure Handling

What happens when a consumer fails, and how to design around it:

| Failure mode | What happens | Mitigation |
|---|---|---|
| Consumer process crashes | Coordinator detects missed heartbeats, triggers rebalance, reassigns its partitions | Design processing to be idempotent, since the new owner resumes from the last **committed** offset, potentially reprocessing a few messages |
| Consumer hangs (slow processing) | Exceeds `max.poll.interval.ms`, treated as dead even if the process is technically alive | Tune `max.poll.records`/`max.poll.interval.ms`, or offload slow work asynchronously so `poll()` loop stays responsive |
| Poison message (repeatedly fails to process) | Without handling, can block the partition forever (retry loop, offset never advances) | Route to a **dead-letter topic** after N failed attempts, then commit past it (full pattern in Module 16) |
| Broker/leader failure | Partition leader fails over to an in-sync replica; consumer reconnects to the new leader transparently | No consumer-side action generally needed — handled by the client's internal metadata refresh |

---

## 🧪 CLI Lab

```bash
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group orders-group
```

Check the group:
```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group orders-group
```

---

## 📝 Exercise

**Task:** Run three consumers in one group against a topic with three partitions. Stop one consumer and observe the rebalance.

### Step-by-step

```bash
# Terminal 1
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders --group orders-group --property print.partition=true

# Terminal 2
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders --group orders-group --property print.partition=true

# Terminal 3
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders --group orders-group --property print.partition=true

# Terminal 4 — produce some messages while all 3 are running
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders

# Terminal 5 — watch the group's assignment live
watch -n 2 "bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-group"
```

Now, **stop Terminal 2** (Ctrl+C) and watch Terminal 5.

### Sample Result & Explanation

<details>
<summary>💡 Click to reveal expected output and analysis</summary>

**Before stopping (3 consumers, 3 partitions) — describe output:**
```
GROUP          TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID        HOST
orders-group   orders  0          5               5               0    consumer-1-abc...  /127.0.0.1
orders-group   orders  1          3               3               0    consumer-2-def...  /127.0.0.1
orders-group   orders  2          4               4               0    consumer-3-ghi...  /127.0.0.1
```
Each consumer owns exactly one partition — a 1:1 balanced assignment.

**After stopping Terminal 2 (consumer-2) — describe output within a few seconds:**
```
GROUP          TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID        HOST
orders-group   orders  0          5               5               0    consumer-1-abc...  /127.0.0.1
orders-group   orders  1          3               3               0    consumer-3-ghi...  /127.0.0.1   ← reassigned!
orders-group   orders  2          4               4               0    consumer-3-ghi...  /127.0.0.1
```

**What happened:**
1. Terminal 2's consumer stopped sending heartbeats.
2. After `session.timeout.ms` elapses (default in the 10–45s range depending on client version), the group coordinator detects it's gone.
3. A **rebalance** is triggered — Partition 1 (previously owned by consumer-2) is reassigned to one of the remaining consumers (consumer-3 here).
4. Terminal 3 will start printing new messages tagged `Partition:1` in addition to `Partition:2` — it's now doing double duty.
5. If you produce new messages during this window, you may see a brief pause on Partition 1's messages while the rebalance completes — this is the "stop-the-world" effect described in [Rebalancing](#-rebalancing) (unless your client is configured with `CooperativeStickyAssignor`, in which case Partition 2 traffic to consumer-3 wouldn't even pause).

**Try extending this exercise:**
- Restart the stopped consumer (Terminal 2) again and watch whether Kafka moves Partition 1 back to it, or leaves it with consumer-3 (depends on assignor — `StickyAssignor`/`CooperativeStickyAssignor` tend to minimize movement, so it may *not* move back immediately).
- Kill a consumer with `kill -9` instead of Ctrl+C to simulate a hard crash rather than a graceful shutdown, and compare how long the rebalance takes to trigger (graceful leave is detected immediately; a hard crash waits for the session timeout).

</details>

---

**◀ Previous:** [05 — Kafka Producers](../05-Kafka-Producers/README.md) · **Next ▶** [07 — Replication & High Availability](../07-Replication-and-High-Availability/README.md)
