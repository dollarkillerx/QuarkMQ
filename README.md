# QuarkMQ

A lightweight, Kafka-compatible message queue written in Rust. QuarkMQ speaks the Kafka binary protocol, so standard Kafka CLI tools (`kafka-topics.sh`, `kafka-console-producer.sh`, `kafka-console-consumer.sh`) work out of the box.

## Features

- **Kafka binary protocol** — wire-compatible with Apache Kafka clients and CLI tools
- **Persistent storage** — append-only commit log with segment rolling, sparse index, and time index
- **Automatic recovery** — rebuilds partition state from on-disk segments on startup
- **TOML configuration** — file config, CLI args, and environment variables (in that priority order)
- **Single binary** — no JVM, no ZooKeeper, just `quarkmq`

## Quick Start

### Build

```bash
cargo build --release
```

### Run

```bash
# With defaults (binds to 0.0.0.0:9092, data in /tmp/quarkmq/data)
./target/release/quarkmq

# With custom options
./target/release/quarkmq --bind 127.0.0.1:9092 --data-dir ./data
```

### Verify with Kafka CLI tools

```bash
# Create a topic
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test --partitions 3

# Produce messages
echo "hello quarkmq" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test

# Consume messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

# List topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Delete a topic
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic test
```

## Supported Kafka APIs

| API | Key | Versions |
|-----|-----|----------|
| ApiVersions | 18 | v0–v3 |
| Metadata | 3 | v1 |
| CreateTopics | 19 | v2 |
| DeleteTopics | 20 | v1 |
| Produce | 0 | v3 |
| Fetch | 1 | v4 |
| ListOffsets | 2 | v1 |

Unsupported but known API keys return `UNSUPPORTED_VERSION` (error code 35). Unknown API keys return an error response without closing the connection.

## Architecture

QuarkMQ is a Rust workspace with four crates:

```
crates/
  quarkmq-protocol/   # Kafka binary protocol framing, request parsing, response encoding
  quarkmq-storage/    # Append-only commit log: Segment (.log + .index + .timeindex)
  quarkmq-broker/     # Topic and partition management, produce/fetch logic
  quarkmq-server/     # TCP listener, per-connection task dispatch, config loading
```

**Data model:** Topic → Partition → CommitLog → Segment

Each partition maps to a directory (`{data_dir}/{topic}-{partition_id}/`) containing segment files that auto-roll at `segment_bytes` (default 1 GB).

## Configuration

QuarkMQ loads configuration in this order (later sources override earlier ones):

1. **TOML config file** (default: `config/quarkmq.toml`)
2. **Environment variables**
3. **CLI arguments**

### Config file

```toml
[broker]
node_id = 0
data_dir = "/var/lib/quarkmq/data"

[server]
bind = "0.0.0.0:9092"

[log]
segment_bytes = 1073741824      # 1 GB
index_interval_bytes = 4096

[defaults]
num_partitions = 1
```

### CLI arguments

```
quarkmq [OPTIONS]

Options:
  -c, --config <PATH>       Path to config file [default: config/quarkmq.toml]
      --bind <ADDR>          Bind address (e.g. 127.0.0.1:9092)
      --data-dir <PATH>      Data directory
      --node-id <ID>         Node ID
```

### Environment variables

| Variable | Description |
|----------|-------------|
| `QUARKMQ_BIND` | Bind address |
| `QUARKMQ_DATA_DIR` | Data directory |
| `QUARKMQ_NODE_ID` | Node ID |

## Known Limitations

This is **Milestone 1** (M1) — a single-node, functional Kafka-compatible broker.

- **Single node only** — no replication, no clustering
- **No consumer groups** — clients must manage their own offsets
- **No SASL/ACL** — no authentication or authorization
- **No transactions** — no transactional produce/consume
- **No long-polling** — Fetch requests return immediately even when no data is available
- **No compression** — RecordBatch compression is not yet handled

## Roadmap

| Milestone | Focus |
|-----------|-------|
| **M1** (current) | Single-node Kafka-compatible broker with core APIs |
| **M2** | Consumer groups (GroupCoordinator, OffsetCommit/Fetch, JoinGroup, SyncGroup, Heartbeat) |
| **M3** | Clustering and replication (Raft-based leader election, ISR) |
| **M4** | SASL authentication and ACL authorization |
| **M5** | Transactions, compression, and performance tuning |

## Development

### Run all tests

```bash
cargo test --workspace
```

116 tests across the workspace: 45 storage, 23 protocol, 32 broker, 4 server, 12 integration.

### Build

```bash
cargo build --release
```

## License

See [LICENSE](LICENSE) for details.
