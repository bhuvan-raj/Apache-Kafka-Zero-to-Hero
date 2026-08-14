# 08 — Storage and Retention

> 🎯 **Goal of this module:** Understand how Kafka physically stores data on disk, and the two very different cleanup strategies — deleting old data by age/size, versus compacting to keep only the latest value per key.

---

## 📚 Topics

- [Append-only logs](#-append-only-logs)
- [Log segments](#-log-segments)
- [Offsets](#-offsets)
- [Retention by time](#-retention-by-time)
- [Retention by size](#-retention-by-size)
- [`retention.ms`](#-retentionms)
- [`retention.bytes`](#-retentionbytes)
- [Cleanup policies](#-cleanup-policies)
- [Log compaction](#-log-compaction)
- [Tombstone records](#-tombstone-records)

---

## 🔹 Append-Only Logs

At its physical core, every Kafka partition is an **append-only log** — new records are always written to the end of the file; existing records are never modified in place.

```
Partition 0 log (conceptually):

[offset 0][offset 1][offset 2][offset 3][offset 4] ──► new writes append here
    ▲
  oldest                                          newest ▲
```

**Why append-only matters:**
- **Extremely fast writes** — sequential disk I/O (appending) is dramatically faster than random-access writes/updates, even on spinning disks, and Kafka leans heavily on OS page cache + sequential I/O for its performance.
- **Immutability** — once written, a record's position and content never change, which is what makes offsets a stable, reliable reference point.
- **Simplicity** — no in-place updates means no locking complexity for concurrent writers to the same partition (there's only ever one writer: the leader).

This is also why Kafka *deletes* data rather than *edits* it — cleanup (retention, compaction) works by removing whole segments or superseded records, never by rewriting existing ones in place.

---

## 🔹 Log Segments

A partition's log isn't one giant file — it's split into **segments**, rolled over based on size or time.

```
Partition 0 directory on disk:

00000000000000000000.log   ─┐
00000000000000000000.index  │  Segment 1 (offsets 0-999)
00000000000000000000.timeindex ─┘

00000000000001000000.log   ─┐
00000000000001000000.index  │  Segment 2 (offsets 1000-1999) ← currently active
00000000000001000000.timeindex ─┘
```

| File | Purpose |
|---|---|
| `.log` | The actual message data, appended sequentially |
| `.index` | Maps offsets → byte position in the `.log` file, for fast lookup (avoids scanning the whole file) |
| `.timeindex` | Maps timestamps → offsets, for time-based lookups (e.g. "give me messages after 3pm") |

**Segment rollover** is controlled by:
- `log.segment.bytes` (default 1GB) — roll to a new segment once the active one hits this size.
- `log.segment.ms` — roll to a new segment after this much time, regardless of size.

**Why segments matter for retention:** Kafka can only delete/expire data at the **segment** level, not the individual-record level. An entire segment must be fully "expired" (every record in it past the retention threshold) before Kafka deletes the file. This is why retention is often described as "at least X", not "exactly X" — a segment isn't removed until the newest record in it also qualifies for deletion.

---

## 🔹 Offsets

Recap from Module 02/04, with the storage angle: offsets are what the `.index` file maps to physical byte positions, letting Kafka jump directly to a requested offset within a segment (via binary search on the sparse index) rather than scanning from the start of the file every time a consumer requests a specific position.

---

## 🔹 Retention by Time

The default and most common cleanup strategy: delete segments once **every record in them** is older than a configured age.

```
retention.ms = 604800000  (7 days)

Segment created 10 days ago, all records older than 7 days → ELIGIBLE for deletion
Segment created 3 days ago, all records within 7 days       → KEPT
```

Kafka's log cleaner thread periodically checks each segment's *newest* record timestamp against `retention.ms` — only fully-expired segments are removed.

---

## 🔹 Retention by Size

An alternative (or additional) strategy: cap the **total size** of a partition's log, deleting the oldest segments once the cap is exceeded — regardless of age.

```
retention.bytes = 1073741824  (1 GB)

Partition log grows past 1GB
   → oldest segment(s) deleted until total size is back under the cap
```

**Time and size retention can be combined** — whichever limit is hit first triggers deletion. This is common when you want a safety net against runaway disk usage (size cap) alongside a normal business-driven retention window (time cap).

---

## 🔹 `retention.ms`

The topic (or broker-default) config controlling time-based retention, in milliseconds.

```properties
retention.ms=604800000    # 7 days (a common default)
retention.ms=-1           # retain forever (no time-based deletion)
```

Can be set broker-wide (`log.retention.ms`, `log.retention.hours`, `log.retention.minutes` — broker defaults) or overridden per-topic via `kafka-configs.sh --alter --add-config retention.ms=...`, which is the recommended approach when different topics have genuinely different retention needs (e.g. raw clickstream events vs. financial audit logs).

---

## 🔹 `retention.bytes`

The topic (or broker-default) config controlling size-based retention, **per partition** — not per topic.

```properties
retention.bytes=1073741824   # 1 GB per partition
retention.bytes=-1           # no size limit (the default)
```

⚠️ **Common gotcha:** `retention.bytes` applies **per partition**, so a topic with 10 partitions and `retention.bytes=1GB` can hold up to ~10GB total, not 1GB total. Always multiply by partition count when reasoning about total disk footprint.

---

## 🔹 Cleanup Policies

`cleanup.policy` determines which cleanup *strategy* applies to a topic — this is the setting that decides between the two retention approaches covered so far versus compaction:

| Policy | Behavior |
|---|---|
| `delete` (default) | Old segments are deleted based on `retention.ms`/`retention.bytes`, as covered above |
| `compact` | Old records are **not** deleted by age — instead, superseded records (same key, newer value exists) are removed, keeping only the latest value per key forever |
| `compact,delete` | Both apply — compaction keeps the latest value per key, while retention limits still expire very old data (used e.g. to eventually clean up tombstones — see below) |

```properties
cleanup.policy=delete              # e.g. clickstream/event topics
cleanup.policy=compact             # e.g. a topic acting as a "latest state" store
cleanup.policy=compact,delete      # e.g. state topics that also need a hard age cap
```

---

## 🔹 Log Compaction

**Compaction** is a fundamentally different cleanup model from deletion: instead of removing data by *age*, it removes data by *redundancy* — for each key, only the most recent value is retained; everything else is eventually discarded.

```
Before compaction (by offset):

offset:  0        1        2        3        4        5
key:     K1       K2       K1       K3       K2       K1
value:   "v1"     "v1"     "v2"     "v1"     "v2"     "v3"

After compaction — only the LATEST value per key survives:

offset:  3        4        5
key:     K3       K2       K1
value:   "v1"     "v2"     "v3"
```

**Why this is useful:** compaction turns a Kafka topic into something like a durable, replayable **key-value snapshot** — a new consumer reading from the beginning gets the *current state* of every key (not the full history of changes), in a fraction of the storage a full `delete`-policy retention would need.

**Classic use cases:**
- Kafka's own internal `__consumer_offsets` topic (latest committed offset per group/partition — old commits are irrelevant).
- Change Data Capture (CDC) topics mirroring a database table's current row state.
- Feeding compacted topics into a `KTable` in Kafka Streams, which is exactly this "latest value per key" model.

**Mechanics note:** compaction happens **within a segment boundary** on a periodic background cycle (the *cleaner* thread) — the *active* (currently-being-written) segment is never compacted, only prior, closed segments. So a key's old value can briefly coexist with its new value until the next compaction pass runs.

---

## 🔹 Tombstone Records

A **tombstone** is a record with a **null value** for a given key — it's the mechanism for *deleting* a key entirely from a compacted topic (not just superseding it with a new value).

```
key: "customer-42"   value: null      ← this is a tombstone

Compaction behavior:
1. All prior values for "customer-42" are eventually removed (superseded, as normal)
2. The tombstone itself is ALSO eventually removed — but only after
   `delete.retention.ms` has passed (default 24h), giving consumers
   time to see the deletion before it vanishes
```

**Why the delay matters:** if the tombstone were removed immediately, a consumer that was behind (hadn't yet read up to that offset) would never learn the key was deleted — it would just quietly stop seeing updates for that key, indistinguishable from "no new data yet." The `delete.retention.ms` window ensures every consumer has a reasonable chance to observe the explicit deletion before the tombstone itself disappears.

```properties
delete.retention.ms=86400000   # 24 hours — how long tombstones survive after compaction
```

---

## 🧪 Lab

**Task:** Create a test topic with a short retention period, publish records, and observe how retention affects stored data.

### Step-by-step

```bash
# 1. Create the topic
bin/kafka-topics.sh --create --topic test-retention \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# 2. Set a short retention window (60 seconds) AND force small segments
#    so a new segment rolls quickly and becomes eligible for deletion sooner
bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name test-retention \
  --alter \
  --add-config retention.ms=60000,segment.ms=30000

# 3. Confirm the config took effect
bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name test-retention --describe

# 4. Produce a handful of messages
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-retention
# type a few lines, then Ctrl+C

# 5. Immediately check the on-disk segment files
ls -la /tmp/kraft-combined-logs/test-retention-0/

# 6. Wait ~90 seconds (past both segment.ms rollover and retention.ms), then check again
sleep 90
ls -la /tmp/kraft-combined-logs/test-retention-0/

# 7. Try consuming from the beginning — see what's left
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic test-retention --from-beginning --timeout-ms 5000
```

### Sample Result & Explanation

<details>
<summary>💡 Click to reveal expected output and analysis</summary>

**Immediately after producing (step 5):**
```
-rw-r--r--  1 user  wheel   142 00000000000000000000.log
-rw-r--r--  1 user  wheel    10 00000000000000000000.index
-rw-r--r--  1 user  wheel    12 00000000000000000000.timeindex
```
All messages are in a single active segment — nothing has been deleted yet, because deletion only happens to *closed* (rolled-over) segments, and the retention clock only starts counting once a segment closes.

**After waiting ~90 seconds (step 6):**
```
-rw-r--r--  1 user  wheel     0 00000000000000000000.log
```
(or the file/segment may be gone entirely, depending on exact timing) — because:
1. `segment.ms=30000` forced the original segment to roll over into a new (now-active) segment after 30s, even though it never hit a size threshold.
2. Once closed, that old segment's newest record age was checked against `retention.ms=60000` — once it exceeded 60s, the **log cleaner thread** deleted the entire segment file.

**Consuming from the beginning (step 7):** if you waited long enough, the consumer will return **no messages** (or times out with nothing) — the data is genuinely gone, not just hidden. This is different from compaction, where old *values* disappear but the *key* itself would still show its latest value.

**Try extending this lab:**
- Repeat the same experiment but set `cleanup.policy=compact` instead, publish the **same key** multiple times with different values, and observe that after compaction runs, only the latest value per key remains — even without any `retention.ms` expiry at all.
- Produce a tombstone (`key:` with an empty/null value using the console producer's `parse.key=true` option and a blank value) into a compacted topic, and watch it eventually disappear after `delete.retention.ms`.
- Check `log.retention.check.interval.ms` (broker-level, default 5 minutes) — this is the interval at which Kafka's cleaner thread actually scans for expired segments; retention isn't instantaneous even after the threshold passes, it happens on this periodic sweep.

</details>

---

**◀ Previous:** [07 — Replication & High Availability](../07-Replication-and-High-Availability/README.md) · **Next ▶** [09 — Kafka Administration](../09-Kafka-Administration/README.md)
