# 03 — Installation and Setup

> 🎯 **Goal of this module:** Get a working Kafka installation running locally using KRaft mode, understand what's inside the distribution, and learn the basic CLI workflow for starting Kafka and managing topics.

---

## 📚 Topics

- [Kafka with KRaft](#-kafka-with-kraft)
- [Kafka installation](#-kafka-installation)
- [Kafka directory structure](#-kafka-directory-structure)
- [Configuration](#-configuration)
- [Starting and stopping Kafka](#-starting-and-stopping-kafka)
- [Single-broker setup](#-single-broker-setup)
- [Multi-broker setup](#-multi-broker-setup)
- [Kafka CLI](#-kafka-cli)
- [Kafka logs](#-kafka-logs)

---

## 🔹 Kafka with KRaft

**KRaft** (Kafka Raft) is Kafka's modern consensus mechanism that replaces the old **Zookeeper** dependency. Since Kafka 3.x+ (and mandatory from Kafka 4.0 onward), Kafka manages its own cluster metadata — broker membership, partition leadership, topic configs — using the **Raft consensus protocol**, without needing a separate Zookeeper ensemble.

**Why it matters:**

| Zookeeper mode (legacy) | KRaft mode (modern) |
|---|---|
| Requires a separate Zookeeper cluster | Metadata is managed by Kafka brokers themselves |
| Two systems to deploy, monitor, and secure | One system to operate |
| Slower controller failover | Faster leader/controller failover |
| More moving parts, more ops overhead | Simpler architecture, fewer moving parts |

In KRaft mode, one or more brokers are designated as **controllers** (the `process.roles` config decides this — `broker`, `controller`, or both in small setups). These controller nodes replicate metadata via Raft, elect a leader controller, and push metadata to the rest of the cluster.

```
KRaft Cluster (combined mode — common for dev/test)

┌─────────────────────────────────────────────┐
│  Node (process.roles=broker,controller)      │
│  - Serves topic data (broker role)           │
│  - Participates in metadata quorum (controller role) │
└─────────────────────────────────────────────┘
```

In production, it's common to run **dedicated controller nodes** separate from broker nodes for stability at scale.

---

## 🔹 Kafka Installation

Kafka ships as a binary distribution (Scala/Java based) that requires a JVM to run.

**Typical steps:**
1. Install a compatible **Java runtime** (JDK 11 or 17+ depending on the Kafka version).
2. Download the Kafka binary distribution (`kafka_<scala-version>-<kafka-version>.tgz`) from the official Apache Kafka site.
3. Extract it:
   ```bash
   tar -xzf kafka_2.13-3.7.0.tgz
   cd kafka_2.13-3.7.0
   ```
4. (KRaft mode) Generate a cluster ID and format storage before first start:
   ```bash
   KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
   bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
   ```
5. Start the broker (see [Starting and stopping Kafka](#-starting-and-stopping-kafka) below).

> ⚠️ Always check the release notes for the specific Kafka version you're installing — script names, default config paths (`config/server.properties` vs `config/kraft/server.properties`), and Java version requirements shift between major releases.

---

## 🔹 Kafka Directory Structure

After extracting the distribution, you'll find:

```text
kafka_2.13-3.7.0/
├── bin/                # Shell scripts (Linux/macOS) to run Kafka & tools
│   ├── kafka-server-start.sh
│   ├── kafka-server-stop.sh
│   ├── kafka-topics.sh
│   ├── kafka-console-producer.sh
│   ├── kafka-console-consumer.sh
│   ├── kafka-consumer-groups.sh
│   └── kafka-storage.sh
├── bin/windows/        # Equivalent .bat scripts for Windows
├── config/             # Configuration templates
│   ├── server.properties          # Broker config (Zookeeper mode)
│   ├── kraft/server.properties    # Broker config (KRaft mode)
│   └── ...
├── libs/                # Kafka's Java/Scala jar dependencies
├── logs/                # Broker runtime logs (created after first start)
└── site-docs/           # Bundled documentation
```

**Key distinction:** `config/` holds *broker configuration files*; `logs/` (created at runtime) holds both **broker application logs** and, by default, the actual **topic/partition data files** (unless `log.dirs` is pointed elsewhere).

---

## 🔹 Configuration

Kafka's behavior is controlled through properties files, most importantly `server.properties`. Key settings to know at this stage:

| Property | Purpose |
|----------|---------|
| `process.roles` | Defines whether this node is a `broker`, `controller`, or both (KRaft mode) |
| `node.id` | Unique ID for this node in the cluster |
| `listeners` | Network addresses/ports the broker listens on (e.g. `PLAINTEXT://localhost:9092`) |
| `log.dirs` | Filesystem path(s) where partition data is stored |
| `controller.quorum.voters` | List of controller nodes participating in the KRaft metadata quorum |
| `num.partitions` | Default partition count for auto-created topics |
| `offsets.topic.replication.factor` | Replication factor for the internal `__consumer_offsets` topic |

For a single-node dev setup, the default `config/kraft/server.properties` usually works out of the box. For multi-broker setups, each broker needs a **unique `node.id`** and **unique `listeners` port**, all pointing to the same `controller.quorum.voters`.

---

## 🔹 Starting and Stopping Kafka

**Starting:**
```bash
# Start Kafka using the distribution's configured KRaft setup
bin/kafka-server-start.sh config/kraft/server.properties
```

Run it in the foreground for dev/testing (logs stream to your terminal), or with `-daemon` to background it:
```bash
bin/kafka-server-start.sh -daemon config/kraft/server.properties
```

**Stopping:**
```bash
bin/kafka-server-stop.sh
```

**Checking it's alive:**
```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```
If this returns (even with an empty list) without a connection error, the broker is up and reachable.

---

## 🔹 Single-Broker Setup

The simplest way to learn Kafka: one node acting as both broker and controller.

```
┌─────────────────────────────────────┐
│         Single Node (KRaft)          │
│  process.roles = broker,controller   │
│  listeners = PLAINTEXT://:9092       │
└─────────────────────────────────────┘
```

**Steps:**
1. Format storage with a generated cluster ID (one-time step, see [Installation](#-kafka-installation)).
2. Start the broker: `bin/kafka-server-start.sh config/kraft/server.properties`.
3. Create and use topics against `localhost:9092`.

**Limitations:** no replication is meaningfully possible (only 1 broker to place replicas on), so it's suitable only for learning and local development — never production.

---

## 🔹 Multi-Broker Setup

For anything resembling production, you run **multiple broker processes** (on separate machines, VMs, or containers), sharing the same KRaft controller quorum.

**What changes per broker:**
- Unique `node.id` for each broker.
- Unique `listeners` (e.g. different ports if running on the same host: `9092`, `9093`, `9094`).
- Same `controller.quorum.voters` list across all nodes, so they agree on cluster metadata.
- Shared `log.dirs` paths *per broker* (not shared storage — each broker has its own local disk).

```
┌────────────┐   ┌────────────┐   ┌────────────┐
│  Broker 1   │   │  Broker 2   │   │  Broker 3   │
│  node.id=1  │   │  node.id=2  │   │  node.id=3  │
│  :9092      │   │  :9093      │   │  :9094      │
└─────┬───────┘   └─────┬───────┘   └─────┬───────┘
      │                 │                 │
      └────────── controller.quorum.voters ──────────┘
              (shared KRaft metadata quorum)
```

This setup is what makes **replication factor > 1** meaningful — topics can now be spread and replicated across genuinely separate broker processes.

---

## 🔹 Kafka CLI

Kafka ships with a rich set of shell scripts under `bin/` for operating the cluster without writing code:

| Script | Purpose |
|--------|---------|
| `kafka-topics.sh` | Create, list, describe, alter, delete topics |
| `kafka-console-producer.sh` | Interactively produce messages from the terminal |
| `kafka-console-consumer.sh` | Interactively consume messages from the terminal |
| `kafka-consumer-groups.sh` | Inspect/manage consumer groups and offsets |
| `kafka-configs.sh` | View/alter dynamic broker and topic configs |
| `kafka-storage.sh` | Format storage directories for KRaft (`random-uuid`, `format`) |
| `kafka-reassign-partitions.sh` | Move partitions between brokers |

Every CLI tool that talks to the cluster needs `--bootstrap-server <host:port>` pointing at one or more live brokers.

---

## 🔹 Kafka Logs

"Logs" in Kafka means two different things — don't confuse them:

1. **Application/broker logs** — standard operational logs (startup messages, errors, warnings) written to the `logs/` directory, controlled by `config/log4j.properties` (or `log4j2.properties` in newer versions). These are for *you*, the operator, to debug the broker itself.

2. **Partition logs (the commit log)** — the actual **topic data**, stored as segment files under whatever `log.dirs` points to (e.g. `/tmp/kraft-combined-logs`). This is Kafka's core data structure — every partition is physically an append-only log file split into segments (covered in depth in Module 08).

```
log.dirs/
└── demo-0/                      # topic "demo", partition 0
    ├── 00000000000000000000.log     # actual message data
    ├── 00000000000000000000.index   # offset → file position index
    └── 00000000000000000000.timeindex
```

---

## 🧪 Basic Workflow

```bash
# Start Kafka using the distribution's configured KRaft setup
bin/kafka-server-start.sh config/server.properties

# Create a topic
bin/kafka-topics.sh --create \
  --topic demo \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# List topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

> Commands can differ slightly between Kafka releases. Always check the version-specific documentation shipped with the distribution.

### Extending the workflow (try it yourself)

Once the topic exists, try producing and consuming to see the log in action end-to-end:

```bash
# Describe the topic — see partition count, leader, replicas, ISR
bin/kafka-topics.sh --describe --topic demo --bootstrap-server localhost:9092

# Produce a few messages interactively (Ctrl+C to stop)
bin/kafka-console-producer.sh --topic demo --bootstrap-server localhost:9092

# In a separate terminal, consume from the beginning
bin/kafka-console-consumer.sh --topic demo --bootstrap-server localhost:9092 --from-beginning

# Stop the broker when done
bin/kafka-server-stop.sh
```

<details>
<summary>💡 What you should observe</summary>

- `--describe` shows 3 partitions, each with a `Leader` broker ID, `Replicas`, and `Isr` — since replication factor is 1, leader and replica list will be identical (no fault tolerance here, by design, for this exercise).
- Messages typed into the producer terminal appear almost instantly in the consumer terminal.
- If you stop and restart the consumer *without* `--from-beginning`, it will only show *new* messages — because by default a fresh consumer group starts reading from the latest offset, not the start of the log.
- Try opening a second consumer terminal in the same group (`--group demo-group` on both) — you'll see Kafka split the 3 partitions between the two consumers instead of both reading everything.

</details>

---

**◀ Previous:** [02 — Kafka Architecture](../02-Kafka-Architecture/README.md) · **Next ▶** [04 — Topics & Partitions](../04-Topics-and-Partitions/README.md)
