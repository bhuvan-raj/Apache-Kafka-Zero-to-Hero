# 03 — Topics and Partitions

> 🎯 **Goal of this module:** Get hands-on with the topic/partition lifecycle — creating, inspecting, and deleting topics — and understand how message keys determine partition placement and ordering.

---

## 📚 Topics

- [Creating topics](#-creating-topics)
- [Describing topics](#-describing-topics)
- [Deleting topics](#-deleting-topics)
- [Partitions](#-partitions)
- [Partition ordering](#-partition-ordering)
- [Message keys](#-message-keys)
- [Key-based partitioning](#-key-based-partitioning)
- [Offsets](#-offsets)
- [Partition scaling](#-partition-scaling)
- [Partition planning](#-partition-planning)

---

## 🔹 Creating Topics

Topics are created with `kafka-topics.sh --create`, specifying a name, partition count, and replication factor.

```bash
bin/kafka-topics.sh --create --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

**Key flags:**

| Flag | Meaning |
|------|---------|
| `--topic` | Topic name (must be unique in the cluster) |
| `--partitions` | Number of partitions to split the topic into |
| `--replication-factor` | How many copies of each partition to keep, across brokers |
| `--config <key>=<value>` | Set topic-level overrides at creation time (e.g. `--config retention.ms=604800000`) |

**Note on auto-creation:** Kafka can auto-create a topic the first time a producer writes to a nonexistent topic (if `auto.create.topics.enable=true` on the broker), using cluster defaults for partitions/replication. This is convenient for quick testing but discouraged in production — it leads to inconsistent, undocumented topic configs. Always create topics explicitly in real environments.

---

## 🔹 Describing Topics

`--describe` shows a topic's current partition layout: leader, replicas, and ISR per partition.

```bash
bin/kafka-topics.sh --describe \
  --topic orders \
  --bootstrap-server localhost:9092
```

**Example output:**
```
Topic: orders   TopicId: abc123   PartitionCount: 3   ReplicationFactor: 1   Configs: ...
   Topic: orders   Partition: 0   Leader: 1   Replicas: 1   Isr: 1
   Topic: orders   Partition: 1   Leader: 1   Replicas: 1   Isr: 1
   Topic: orders   Partition: 2   Leader: 1   Replicas: 1   Isr: 1
```

| Column | Meaning |
|--------|---------|
| `Leader` | Broker ID currently serving reads/writes for this partition |
| `Replicas` | All broker IDs holding a copy of this partition (leader + followers) |
| `Isr` | In-sync replicas — the subset of `Replicas` fully caught up (see Module 02) |

This is the go-to command whenever you need to confirm how a topic is actually laid out across the cluster, or diagnose an under-replicated partition (`Isr` shorter than `Replicas`).

---

## 🔹 Deleting Topics

```bash
bin/kafka-topics.sh --delete --topic orders --bootstrap-server localhost:9092
```

**Things to know:**
- Deletion is **irreversible** — all messages in the topic are gone; there's no recycle bin.
- Requires `delete.topic.enable=true` on the broker (the default in modern Kafka versions — older versions defaulted this to `false` for safety).
- Deletion is asynchronous — the topic may briefly still appear in `--list` output while brokers clean up partition data in the background.
- Any consumer groups that were reading the topic will have stale committed offsets left behind for it — harmless, but worth knowing if you recreate a topic with the same name later.

---

## 🔹 Partitions

A partition is the unit of **parallelism and storage** within a topic — covered conceptually in Module 02. Here, the practical takeaway is: partition count is a decision you make *at* (or after) topic creation, and it directly controls:

- **Maximum consumer parallelism** — a consumer group can have at most as many *active* consumers as there are partitions; extras sit idle.
- **Throughput ceiling** — more partitions generally means more aggregate throughput, since writes/reads are spread across more brokers/disks.
- **Per-key ordering granularity** — ordering is only guaranteed within a partition, so partition count affects how finely ordering is scoped (see [Partition ordering](#-partition-ordering) below).

---

## 🔹 Partition Ordering

Kafka guarantees **strict ordering within a single partition** — messages are appended and read back in the exact order they were written.

Kafka makes **no ordering guarantee across partitions**. If `orders` has 3 partitions and messages for `order-1`, `order-2`, `order-3` land on different partitions, there's no relationship between the order those topics-level events are processed relative to each other.

```
Partition 0:  [A1]───[A2]───[A3]     ◄── strictly ordered within this partition
Partition 1:  [B1]───[B2]            ◄── strictly ordered within this partition
Partition 2:  [C1]                   ◄── strictly ordered within this partition

No guarantee about interleaving order between A, B, and C streams.
```

**Practical implication:** if you need all events for a given entity (e.g. a specific customer, order, or device) to be processed in order, they must all land in the **same partition** — which is exactly what message keys are for.

---

## 🔹 Message Keys

Every Kafka message is a `(key, value)` pair (plus optional headers). The **key** is optional, but when present it serves one critical purpose: **determining which partition the message is routed to.**

- `key = null` → the producer uses round-robin or sticky partitioning, spreading messages evenly with no ordering relationship between them.
- `key = "customer-123"` → Kafka's default partitioner hashes the key to consistently pick the **same partition** for every message with that key.

```
producer.send(topic="orders", key="customer-123", value="OrderPlaced")
producer.send(topic="orders", key="customer-123", value="OrderShipped")
                     │
                     ▼
        hash("customer-123") % numPartitions  →  always Partition 1

Result: both events land on Partition 1, in the order they were sent.
```

---

## 🔹 Key-Based Partitioning

Building on message keys — **key-based partitioning** is the standard technique for getting per-entity ordering guarantees without sacrificing overall topic parallelism.

**How it works:**
1. Choose a key that represents the entity whose events must stay ordered (e.g. `customerId`, `deviceId`, `accountId`).
2. Kafka's default partitioner computes `hash(key) % numPartitions` to consistently map that key to one partition.
3. All messages sharing that key always land in the same partition, and are therefore strictly ordered relative to each other.
4. Different keys are free to spread across all partitions, preserving overall throughput and parallelism.

**Trade-off to be aware of:** if your key distribution is skewed (e.g. one giant customer producing 90% of traffic), that partition becomes a hotspot — one broker does disproportionate work, and one consumer in the group becomes a bottleneck. Good key selection balances *even distribution* against *the ordering scope you actually need*.

---

## 🔹 Offsets

Recap from Module 02, with a hands-on angle: offsets are per-partition sequence numbers, and the CLI gives you tools to inspect them directly.

```bash
# Get the earliest and latest offsets for each partition of a topic
bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic orders --time -1   # latest
bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic orders --time -2   # earliest
```

Consumers commit offsets (usually to the internal `__consumer_offsets` topic) to track progress — this is explored fully in Module 06, but it's worth knowing now that offsets are what make replay possible: resetting a consumer group's committed offset to an earlier point causes it to reprocess messages from there.

---

## 🔹 Partition Scaling

You can **increase** a topic's partition count after creation:

```bash
bin/kafka-topics.sh --alter --topic orders \
  --bootstrap-server localhost:9092 --partitions 6
```

**Critical caveat:** Kafka does **not** support decreasing partition count, and increasing it **changes the key → partition mapping** for every existing key (since the hash is computed modulo the *current* partition count). This means:
- Messages already written keep their original partition — history isn't reshuffled.
- New messages with a previously-seen key may now be routed to a *different* partition than older messages with that same key — silently breaking your per-key ordering guarantee going forward.

**Rule of thumb:** if ordering-by-key matters to you, decide your partition count carefully upfront (see below) rather than relying on scaling it later.

---

## 🔹 Partition Planning

Practical guidance for choosing a partition count at topic creation:

| Factor | Guidance |
|--------|----------|
| **Target throughput** | Estimate MB/s needed; a single partition typically sustains tens of MB/s writes — divide target by per-partition capacity to get a rough floor |
| **Consumer parallelism** | Partition count sets the ceiling on how many consumers in a group can be simultaneously active |
| **Ordering requirements** | If ordering-by-key matters, avoid frequent partition count changes (see above) |
| **Cluster size** | More partitions = more replication traffic, more open file handles, longer leader elections during failover — don't over-provision "just in case" |
| **Typical starting point** | Many teams start with a modest number (e.g. 6–12) and scale based on measured throughput, rather than guessing very high upfront |

There's no universally "correct" number — it's a balance between parallelism headroom and operational overhead, revisited as real traffic patterns emerge (explored further in Module 19 — Production Kafka).

---

## 🧪 Lab

**Task:** Create a topic with three partitions, publish keyed messages, and inspect partition assignment.

### Step-by-step

```bash
# 1. Create the topic
bin/kafka-topics.sh --create --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# 2. Confirm the layout
bin/kafka-topics.sh --describe \
  --topic orders \
  --bootstrap-server localhost:9092

# 3. Produce keyed messages (key:value format, using a custom separator)
bin/kafka-console-producer.sh --topic orders \
  --bootstrap-server localhost:9092 \
  --property "parse.key=true" \
  --property "key.separator=:"
```
At the producer prompt, type:
```
customer-1:OrderPlaced
customer-2:OrderPlaced
customer-1:OrderShipped
customer-3:OrderPlaced
customer-2:OrderShipped
```

```bash
# 4. Consume with keys and partition numbers visible
bin/kafka-console-consumer.sh --topic orders \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true
```

### Sample Result & Explanation

<details>
<summary>💡 Click to reveal expected output and analysis</summary>

Example consumer output (exact partition numbers will vary by hash, but the *pattern* will match):

```
Partition:0	customer-1	OrderPlaced
Partition:0	customer-1	OrderShipped
Partition:2	customer-2	OrderPlaced
Partition:2	customer-2	OrderShipped
Partition:1	customer-3	OrderPlaced
```

**What this demonstrates:**
- Every message for `customer-1` lands on the **same partition** (Partition 0 here), and `OrderPlaced` appears before `OrderShipped` — ordering is preserved for that key.
- Likewise, `customer-2`'s two messages both land on Partition 2, in order.
- `customer-3` lands on a third partition (Partition 1) — different keys are free to spread across partitions.
- If you re-run the producer with the exact same keys later, they'll be routed to the **same partitions again** — `hash(key) % partitions` is deterministic as long as partition count doesn't change.

**Try extending this lab:**
- Run `--describe` again and check which broker is `Leader` for each partition (only meaningful with replication-factor > 1 / multiple brokers).
- Send a message with `key=null` (just type a value with no `key.separator` match) and observe it gets spread round-robin rather than hashed.
- Alter the topic to 6 partitions, then produce `customer-1:AnotherEvent` again — see if it still lands on the same partition as before (it may not, illustrating the [Partition Scaling](#-partition-scaling) caveat above).

</details>

---

**◀ Previous:** [03 — Installation & Setup](../03-Installation-and-Setup/README.md) · **Next ▶** [05 — Kafka Producers](../05-Kafka-Producers/README.md)
