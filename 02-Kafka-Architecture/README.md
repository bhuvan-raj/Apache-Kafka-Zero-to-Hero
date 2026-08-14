# 02 — Kafka Architecture

> 🎯 **Goal of this module:** Understand the core building blocks of a Kafka cluster and how they fit together — brokers, topics, partitions, offsets, replication, and the path a message takes from producer to consumer.

---

## 📚 Topics

- [Kafka cluster](#-kafka-cluster)
- [Broker](#-broker)
- [Topic](#-topic)
- [Partition](#-partition)
- [Offset](#-offset)
- [Producer](#-producer)
- [Consumer](#-consumer)
- [Consumer group](#-consumer-group)
- [Leader and follower](#-leader-and-follower)
- [Replication](#-replication)
- [ISR (In-Sync Replicas)](#-isr-in-sync-replicas)
- [Controller](#-controller)
- [Message flow](#-message-flow)

---

## 🔹 Kafka Cluster

A **Kafka cluster** is a group of one or more **brokers** working together, coordinated so that topics can be spread across machines for scalability and fault tolerance.

```
┌─────────────────────────── Kafka Cluster ───────────────────────────┐
│                                                                       │
│   ┌────────────┐        ┌────────────┐        ┌────────────┐       │
│   │  Broker 1  │        │  Broker 2  │        │  Broker 3  │       │
│   └────────────┘        └────────────┘        └────────────┘       │
│                                                                       │
└───────────────────────────────────────────────────────────────────┘
```

A cluster's job is to:
- Store topic data durably, spread across brokers.
- Serve reads/writes to producers and consumers.
- Tolerate broker failure without losing data (via replication).
- Elect a **controller** to manage cluster metadata (partition leadership, broker membership).

---

## 🔹 Broker

A **broker** is a single Kafka server. It:
- Stores a subset of the data for each topic (specific partitions).
- Handles read/write requests from producers and consumers for the partitions it hosts.
- Communicates with other brokers to replicate data and stay in sync.

Each broker is identified by a **broker ID**, and a cluster typically runs 3+ brokers in production for fault tolerance. A broker doesn't store *all* data — data is spread out via partitioning, described below.

---

## 🔹 Topic

A **topic** is a named stream of related messages — the logical channel producers write to and consumers read from (e.g. `orders`, `payments`, `user-clicks`).

Key properties:
- Topics are **append-only, ordered logs** (per partition — see below).
- A topic can have any number of producers and consumers.
- Topics are split into **partitions** to allow parallelism and scale beyond a single broker.

```
Topic: "orders"
┌───────────────────────────────────────────────────┐
│  Partition 0  │  Partition 1  │  Partition 2       │
└───────────────────────────────────────────────────┘
```

---

## 🔹 Partition

A **partition** is an ordered, immutable sequence of records — the actual unit of storage and parallelism in Kafka. A topic is divided into one or more partitions, and each partition:

- Lives on a broker (its **leader**, plus replicas on other brokers).
- Is written to sequentially (append-only).
- Guarantees **ordering only within itself**, not across the whole topic.
- Is the unit of parallel consumption — each partition can be read by only one consumer within a given consumer group at a time.

```
Partition 0:  [msg0][msg1][msg2][msg3][msg4] ──► new messages appended here
               offset: 0    1    2    3    4
```

**Why partitions matter:** more partitions = more parallelism (more consumers can read simultaneously), but also more overhead (more files, more replication traffic, longer leader elections).

---

## 🔹 Offset

An **offset** is a unique, sequential ID assigned to each message *within a partition*. It marks a message's position in the log.

- Offsets are **per-partition**, not global to the topic — `Partition 0` and `Partition 1` both have their own offset `0, 1, 2, ...`.
- Consumers track the offset up to which they've processed messages, so they know where to resume.
- Offsets never change once assigned — deleting/compacting doesn't reuse them.

```
Partition 0:  offset 0 → 1 → 2 → 3 → 4 → 5
                                        ▲
                              consumer is currently here
```

---

## 🔹 Producer

A **producer** is a client application that publishes (writes) messages to a Kafka topic.

- Chooses which **partition** a message goes to — either explicitly, via a **key** (same key → same partition, preserving order for that key), or via round-robin/sticky partitioning if no key is given.
- Can wait for different levels of write acknowledgment (`acks=0`, `1`, `all`) trading off speed vs durability (covered in depth in Module 05).

---

## 🔹 Consumer

A **consumer** is a client application that subscribes to and reads messages from one or more topics.

- Reads messages **in order within a partition**, tracking its position via offsets.
- Can read from the earliest offset (start of the log), the latest (only new messages), or a specific offset.
- Consumption doesn't delete messages — many consumers can independently read the same data.

---

## 🔹 Consumer Group

A **consumer group** is a set of consumers that cooperate to read a topic, with each partition assigned to exactly **one consumer within the group** at a time. This is how Kafka achieves parallel, load-balanced consumption while keeping per-partition ordering.

```
Topic "orders" (3 partitions)          Consumer Group "order-service"

Partition 0  ───────────────────────►  Consumer A
Partition 1  ───────────────────────►  Consumer B
Partition 2  ───────────────────────►  Consumer C
```

- If you have **more consumers than partitions**, the extra consumers sit idle.
- If you have **fewer consumers than partitions**, some consumers handle multiple partitions.
- Different consumer groups are fully independent — each group gets its own copy of the full data stream, tracked with its own offsets.
- When a consumer joins/leaves a group, Kafka triggers a **rebalance**, reassigning partitions among the remaining members (covered in depth in Module 06).

---

## 🔹 Leader and Follower

Each partition has multiple **replicas** spread across brokers for fault tolerance. Among these replicas:

- **Leader** — the replica that handles *all* reads and writes for that partition. There is exactly one leader per partition at any time.
- **Follower(s)** — replicas that passively copy data from the leader, ready to take over if the leader fails.

```
Partition 0 (replication factor 3):

  Broker 1: [Leader]    ◄── producers/consumers talk to this one
  Broker 2: [Follower]  ◄── replicates from leader
  Broker 3: [Follower]  ◄── replicates from leader
```

If the leader's broker fails, one of the in-sync followers is promoted to leader (leader election), so the partition stays available.

---

## 🔹 Replication

**Replication** is how Kafka achieves durability and fault tolerance: each partition's data is copied to multiple brokers, controlled by the topic's **replication factor**.

- **Replication factor = N** means each partition has N total copies (1 leader + N−1 followers) spread across N different brokers.
- Replication factor 3 is the common production default — it tolerates up to 2 simultaneous broker failures without data loss.
- Replication is *per-partition*, so leadership is spread evenly across brokers — no single broker leads everything.

---

## 🔹 ISR (In-Sync Replicas)

The **ISR** is the set of replicas (including the leader) that are fully caught up with the leader's log at any given time.

- A replica falls out of the ISR if it lags too far behind (based on `replica.lag.time.max.ms`).
- Only replicas in the ISR are eligible to be elected leader — this prevents data loss from promoting a replica that's missing recent messages.
- With `acks=all`, a producer's write is only acknowledged once **all replicas in the ISR** have received it — this is what guarantees durability.

```
ISR for Partition 0 = { Broker 1 (leader), Broker 2, Broker 3 }
                        ↑ all caught up — safe to elect any as new leader
```

---

## 🔹 Controller

The **controller** is a special broker responsible for cluster-level coordination:

- Tracks which brokers are alive.
- Handles **leader election** when a broker fails or a partition is created.
- Propagates metadata changes (new topics, partition reassignments) to all brokers.

In modern Kafka (**KRaft mode**, replacing Zookeeper), the controller role is handled by a quorum of dedicated controller nodes using the Raft consensus protocol, rather than an external Zookeeper ensemble — improving scalability and simplifying operations (covered in Module 03).

---

## 🔹 Message Flow

Putting it all together — the full path of a message from producer to consumer:

```
 1. Producer                     2. Broker (Partition Leader)
 ┌──────────┐   sends record     ┌───────────────────────────┐
 │ Producer │ ─────────────────► │ Leader appends to log,     │
 └──────────┘                    │ assigns next offset        │
                                  └──────────────┬──────────────┘
                                                 │
                                  3. Replication │ (to followers in ISR)
                                                 ▼
                         ┌────────────┐   ┌────────────┐
                         │ Follower B │   │ Follower C │
                         └────────────┘   └────────────┘
                                                 │
                                  4. Acknowledgment (per acks config)
                                                 ▼
                                  ┌───────────────────────────┐
                                  │ Producer receives ack       │
                                  └───────────────────────────┘

 5. Consumer                     6. Consumer reads from leader,
 ┌──────────┐   polls/fetches    tracks its own offset per partition
 │ Consumer │ ◄───────────────── Partition Leader
 └──────────┘
```

**Step by step:**
1. Producer sends a record to a topic (optionally with a key).
2. The partition leader appends it to its log and assigns the next offset.
3. Follower replicas fetch and copy the new record from the leader.
4. Once the required replicas acknowledge (per `acks` setting), the producer gets confirmation.
5. A consumer polls the partition leader for new records.
6. The consumer processes the record and commits its offset, marking its progress.

---

## 📝 Exercise

**Task:** Draw a three-broker Kafka cluster containing one topic with three partitions and replication factor three.

### Sample Answer

<details>
<summary>💡 Click to reveal a sample solution</summary>

With **replication factor 3** and **3 brokers**, every partition has a replica on *every* broker — but leadership is spread out so no single broker does all the work.

```
                          Topic: "orders"  (3 partitions, replication factor 3)

        ┌───────────────────┐   ┌───────────────────┐   ┌───────────────────┐
        │      Broker 1      │   │      Broker 2      │   │      Broker 3      │
        ├───────────────────┤   ├───────────────────┤   ├───────────────────┤
        │ P0  [LEADER]       │   │ P0  [Follower]     │   │ P0  [Follower]     │
        │ P1  [Follower]     │   │ P1  [LEADER]       │   │ P1  [Follower]     │
        │ P2  [Follower]     │   │ P2  [Follower]     │   │ P2  [LEADER]       │
        └───────────────────┘   └───────────────────┘   └───────────────────┘

Legend:
  P0, P1, P2  = Partition 0, 1, 2
  [LEADER]    = handles all reads/writes for that partition
  [Follower]  = replicates from the leader, on standby for failover
```

**Why it's arranged this way:**
- Each of the 3 partitions (P0, P1, P2) has exactly 3 replicas — one per broker — satisfying replication factor 3.
- Each broker leads **one** partition and follows the other two, so write/read load is balanced evenly across the cluster instead of piling onto one broker.
- If **Broker 2** fails: P1 loses its leader. Kafka promotes an in-sync replica (P1 on Broker 1 or Broker 3) to leader, and the cluster keeps serving reads/writes for P1 without data loss.
- The cluster can tolerate **up to 2** broker failures simultaneously before any partition becomes unavailable (since replication factor is 3).

</details>

---

**◀ Previous:** [01 — Introduction to Kafka](../01-Introduction-to-Kafka/README.md) · **Next ▶** [03 — Installation & Setup](../03-Installation-and-Setup/README.md)
