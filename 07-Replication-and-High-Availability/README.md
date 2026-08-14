# 07 — Replication and High Availability

> 🎯 **Goal of this module:** Understand how Kafka survives broker failure without losing data or blocking traffic — and where the limits of that protection actually are.

---

## 📚 Topics

- [Replication factor](#-replication-factor)
- [Leader replica](#-leader-replica)
- [Follower replica](#-follower-replica)
- [ISR](#-isr)
- [Leader election](#-leader-election)
- [Broker failure](#-broker-failure)
- [`acks=all`](#-acksall)
- [`min.insync.replicas`](#-mininsyncreplicas)
- [Unclean leader election](#-unclean-leader-election)
- [Durability](#-durability)

---

## 🔹 Replication Factor

Recap from Module 02, now with an operational lens: **replication factor (RF)** is how many total copies of each partition Kafka maintains, spread across distinct brokers.

```
RF = 3  →  1 leader + 2 followers, on 3 different brokers
```

| RF | Tolerates | Notes |
|---|---|---|
| 1 | 0 broker failures | No fault tolerance — any broker loss = data loss for its partitions. Fine for local dev only. |
| 2 | 1 broker failure | Minimal safety net; leaves zero margin during maintenance/rolling restarts. |
| 3 | 2 simultaneous broker failures | Standard production default — balances durability against storage/network cost. |

**Rule of thumb:** RF=3 is the de facto production standard because it survives a broker failure *while also* tolerating a second broker being down for maintenance (e.g. a rolling upgrade) without losing availability.

---

## 🔹 Leader Replica

The **leader** is the single replica that handles all reads and writes for a given partition at any point in time — covered conceptually in Module 02. In the HA context, the leader is the thing that becomes unavailable when its broker crashes, which is precisely the failure this whole module is about surviving.

```
Partition 0:
  Broker 1 → [LEADER]     ◄── all producer/consumer traffic goes here
  Broker 2 → [Follower]
  Broker 3 → [Follower]
```

---

## 🔹 Follower Replica

**Followers** continuously fetch and replicate the leader's log, staying ready to be promoted. A follower is not passive filler — it's actively pulling data (similar to a consumer, internally) to stay caught up.

```
Follower fetch loop:
   Follower ──fetch(offset)──► Leader
   Follower ◄──new records──── Leader
   (repeats continuously)
```

A follower that keeps up closely enough is considered **in-sync** — see ISR below. A follower that falls behind is still a follower, but it's *not* eligible to become leader until it catches back up.

---

## 🔹 ISR

The **In-Sync Replica set (ISR)** — recap from Module 02, now framed around what it protects you from: the ISR is the set of replicas guaranteed to have all the data the leader has (as of the last check), and therefore the **only** replicas eligible for leader election.

```
ISR for Partition 0 = { Broker 1 (leader), Broker 2, Broker 3 }
```

A replica is dropped from the ISR if it falls behind by more than `replica.lag.time.max.ms`. This matters directly for HA: **if a broker fails while its replica is out of the ISR, promoting it would mean data loss** — which is why Kafka refuses to do so by default (see [Unclean leader election](#-unclean-leader-election)).

---

## 🔹 Leader Election

When a partition's leader broker fails, Kafka must promote a new leader — fast, and without losing data.

```
Before failure:
  Broker 1 → [LEADER]      Broker 2 → [Follower, in ISR]      Broker 3 → [Follower, in ISR]

Broker 1 crashes
         │
         ▼
Controller detects the failure (via KRaft metadata / broker heartbeats)
         │
         ▼
Controller picks a new leader from the ISR — e.g. Broker 2
         │
         ▼
After failover:
  Broker 1 → [DOWN]        Broker 2 → [LEADER]                Broker 3 → [Follower, in ISR]
```

This process is coordinated by the **controller** (Module 02/03) and is typically fast — sub-second to a few seconds in a healthy cluster — because the new leader is already caught up (it was in the ISR) and just needs to start accepting traffic.

---

## 🔹 Broker Failure

What actually happens across the cluster when a broker goes down, end to end:

1. **Detection** — the controller notices the broker has stopped sending heartbeats / is unreachable.
2. **Leader re-election** — every partition for which the dead broker was leader gets a new leader promoted from its ISR (per [Leader election](#-leader-election) above).
3. **Metadata propagation** — the controller pushes the updated partition leadership map to all brokers, so producers/consumers get redirected on their next metadata refresh.
4. **Client redirection** — producers/consumers transparently discover the new leader (via a `NOT_LEADER_OR_FOLLOWER` error triggering a metadata refresh) and resume sending/fetching, usually without any application code changes needed.
5. **Under-replication** — partitions that lost a replica now show `Isr` shorter than `Replicas` in `--describe` output, until the broker comes back (or a replacement is provisioned) and catches up.

**What the client experiences:** a brief blip (failed requests, retried automatically) during steps 1–3, then traffic resumes normally — as long as `acks`/`min.insync.replicas` were configured to tolerate this (see below).

---

## 🔹 `acks=all`

Recap from Module 05, now in the HA context: `acks=all` is what actually **connects replication to durability** from the producer's point of view.

```
acks=all:  producer send ──► Leader ──replicate──► ISR followers
                                      ◄──all ISR acked───
                             producer gets confirmation only once ISR has it
```

Without `acks=all`, replication is still happening in the background, but the producer has no *guarantee* its message reached more than the leader before returning success — meaning a leader crash immediately after acking could still lose that message. `acks=all` is the setting that makes replication factor actually pay off for durability.

---

## 🔹 `min.insync.replicas`

`min.insync.replicas` (set at the **broker or topic level**) works together with `acks=all` to define the minimum ISR size required to accept a write at all.

```
Topic config: replication.factor=3, min.insync.replicas=2

Scenario: ISR shrinks to just 1 replica (2 brokers down)
   → Producer using acks=all gets a NotEnoughReplicasException
   → Writes are REJECTED rather than silently accepted with weaker durability
```

| `min.insync.replicas` | Meaning |
|---|---|
| `1` (default) | `acks=all` only requires the leader — effectively same durability as `acks=1` once ISR shrinks to 1 |
| `2` (common with RF=3) | At least 2 replicas (leader + 1 follower) must ack — tolerates 1 broker failure while still guaranteeing the write survives it |

**Practical takeaway:** `acks=all` alone isn't enough for a real durability guarantee — you need `min.insync.replicas ≥ 2` (with RF=3) so that "all ISR members" can never shrink down to just the leader without the cluster explicitly refusing writes rather than silently weakening the guarantee.

---

## 🔹 Unclean Leader Election

An **unclean leader election** is when Kafka promotes a replica that is **not** in the ISR — i.e., one that is known to be missing recent data — because no in-sync replica is available.

```
Normal (clean) election:        Unclean election (data loss!):
ISR = {Broker2, Broker3}        ISR = {}  (all in-sync replicas are down)
Broker1 (leader) fails          Broker1 (leader) fails
   │                               │
   ▼                               ▼
Promote Broker2 (in ISR)        Only Broker3 available, but it's LAGGING (not in ISR)
   → no data loss                → promoting it means messages after its last
                                    known offset are PERMANENTLY LOST
```

Controlled by `unclean.leader.election.enable`:

| Setting | Behavior |
|---|---|
| `false` (recommended, and the modern default) | Partition stays **offline/unavailable** until an in-sync replica comes back — protects data, sacrifices availability |
| `true` | Kafka promotes a non-ISR replica to restore availability immediately — accepts data loss to avoid downtime |

**This is a real availability-vs-durability decision**, not a "just pick the safe option" default: some workloads (e.g. real-time metrics, non-critical logs) may prefer `true` to stay available at the cost of a small data gap; anything requiring correctness (financial transactions, orders) should stay with `false`.

---

## 🔹 Durability

Pulling the whole module together — Kafka's durability for a given topic is the product of several settings working together, not any single one:

| Setting | Role |
|---|---|
| `replication.factor` | How many copies exist to survive broker loss |
| `acks=all` | Ensures the producer only considers a write successful once it's actually replicated |
| `min.insync.replicas` | Ensures "replicated" can't silently degrade to "only the leader has it" |
| `unclean.leader.election.enable=false` | Ensures Kafka never promotes a replica that's missing data, even to stay available |

A production-grade durable setup typically looks like: **RF=3, acks=all, min.insync.replicas=2, unclean.leader.election.enable=false** — this combination survives up to 2 broker failures with zero data loss for acknowledged writes, at the cost of write availability if too many brokers go down simultaneously (which is the intentional, correct trade-off).

---

## ⚠️ Important Concept

> Replication improves **availability** and **durability**, but it does not replace **backups** or a **disaster-recovery strategy**.

Why this distinction matters:
- Replication protects against **broker/hardware failure** — it does *not* protect against **logical errors**: a bad deploy that produces corrupted data, an accidental topic deletion, or a bug that overwrites/compacts away data you needed. Those bad writes get replicated to every replica just as faithfully as good ones.
- Replication is **within one cluster**. It doesn't protect against a **whole-cluster or whole-datacenter outage** — that requires cross-cluster/cross-region replication (e.g. **MirrorMaker 2**, covered alongside DR strategy in Module 19 — Production Kafka).
- Retention policies mean old data eventually **expires** regardless of replication — replication isn't long-term storage; if you need to reprocess data from months ago, that's a data lake/archival concern, not something replication factor gives you.

**Takeaway:** replication answers "how do I survive a broker dying right now?" Backups and DR planning answer "how do I recover from a mistake, or losing an entire cluster?" — different problems, both necessary.

---

## 🧪 Lab

**Task:** Build a three-broker cluster, create a topic with replication factor three, stop one broker, and verify that the cluster continues serving traffic.

### Step-by-step

```bash
# 1. Format and start 3 brokers (assuming separate config files: server-1/2/3.properties
#    each with unique node.id and listener ports, sharing the same controller.quorum.voters —
#    see Module 03 for the multi-broker setup)
bin/kafka-server-start.sh -daemon config/server-1.properties
bin/kafka-server-start.sh -daemon config/server-2.properties
bin/kafka-server-start.sh -daemon config/server-3.properties

# 2. Create a topic with RF=3 and a safe min.insync.replicas
bin/kafka-topics.sh --create --topic payments \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2

# 3. Confirm the layout
bin/kafka-topics.sh --describe --topic payments --bootstrap-server localhost:9092

# 4. Start a durable producer in one terminal
bin/kafka-console-producer.sh --bootstrap-server localhost:9092,localhost:9093,localhost:9094 \
  --topic payments \
  --producer-property acks=all

# 5. Start a consumer in another terminal
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092,localhost:9093,localhost:9094 \
  --topic payments --from-beginning

# 6. Identify and kill the broker currently leading a partition, e.g. Partition 0's leader
bin/kafka-server-stop.sh    # or kill -9 <pid> for the specific broker to simulate a hard crash

# 7. Immediately re-check topic health
bin/kafka-topics.sh --describe --topic payments --bootstrap-server localhost:9092,localhost:9093,localhost:9094

# 8. Send another message from the still-running producer terminal and confirm the consumer receives it
```

### Sample Result & Explanation

<details>
<summary>💡 Click to reveal expected output and analysis</summary>

**Describe output before killing a broker:**
```
Topic: payments   PartitionCount: 3   ReplicationFactor: 3
   Partition: 0   Leader: 1   Replicas: 1,2,3   Isr: 1,2,3
   Partition: 1   Leader: 2   Replicas: 2,3,1   Isr: 2,3,1
   Partition: 2   Leader: 3   Replicas: 3,1,2   Isr: 3,1,2
```

**After stopping Broker 1 (was leader for Partition 0):**
```
Topic: payments   PartitionCount: 3   ReplicationFactor: 3
   Partition: 0   Leader: 2   Replicas: 1,2,3   Isr: 2,3        ← new leader, Broker 1 dropped from ISR
   Partition: 1   Leader: 2   Replicas: 2,3,1   Isr: 2,3        ← Broker 1 was a follower here too, also dropped
   Partition: 2   Leader: 3   Replicas: 3,1,2   Isr: 3,2        ← same
```

**What this demonstrates:**
- Partition 0's leadership **failed over automatically** to Broker 2 — no manual intervention needed.
- `Replicas` still lists Broker 1 (it's still configured as a replica) — but `Isr` no longer includes it, since it's offline and can't be confirmed in-sync. This is the "under-replicated partition" state.
- The producer (using `acks=all`) and consumer both kept working — you should see the message sent in step 8 show up on the consumer with no manual reconnection needed. Client libraries handle the leader-change transparently via metadata refresh.
- If you'd set `min.insync.replicas=3` instead of `2`, this same scenario would have caused writes to **fail** with `NotEnoughReplicasException` the moment the ISR dropped below 3 — illustrating exactly why `min.insync.replicas=2` (with RF=3) is the standard choice: it tolerates exactly this single-broker-failure scenario without sacrificing write availability.

**Try extending this lab:**
- Restart the stopped broker and watch `Isr` grow back to include it once it catches up on replication.
- Kill a *second* broker while the first is still down (simulating 2 simultaneous failures with RF=3) — for whichever partition now has an empty ISR, observe that the partition becomes unavailable for writes (with `unclean.leader.election.enable=false`), rather than silently electing an out-of-date leader.

</details>

---

**◀ Previous:** [06 — Kafka Consumers](../06-Kafka-Consumers/README.md) · **Next ▶** [08 — Storage & Retention](../08-Storage-and-Retention/README.md)
