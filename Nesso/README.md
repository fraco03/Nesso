# Nesso

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

> **Fast, zero-dependency, crash-resilient persistent message queue written in Rust.**

Nesso is an embeddable library and standalone local daemon engineered for strict data durability, predictable priority ordering, and high concurrent throughput. It features a segmented Write-Ahead Log (WAL), two-phase non-blocking online compaction, group-commit fsync batching, lease-based task consumption, graceful shutdown, and an HTTP REST interface with asynchronous long-polling.

---

## Why Nesso?

If you need a durable job queue and reach for SQLite or Redis, you end up reimplementing the same mechanics by hand every time: leases with timeouts, retry counters, dead-letter routing, and crash-safe recovery. Nesso gives you those semantics natively, as a single embeddable library or lightweight daemon with zero external dependencies and no external database server to operate.

It is **not** a Kafka or RabbitMQ replacement — those are distributed streaming platforms solving a different problem at cluster scale. Nesso targets the much more common case: a single-node application or service that needs a persistent, crash-safe task queue without operating a cluster.

---

## Where Nesso Fits

| Feature / Dimension | Kafka | RabbitMQ | Redis | SQLite-as-queue | **Nesso** |
|---|---|---|---|---|---|
| **Deployment** | Multi-node cluster | Dedicated broker cluster | Dedicated server | Embedded library | **Embedded library or local daemon** |
| **Native Queue Semantics** (lease, retry, DLQ) | Partial | Yes | No — build it yourself | No — build it yourself | **Yes, built-in** |
| **Operational Complexity** | High (ZooKeeper/KRaft) | Medium-High (Erlang VM) | Low-Medium | None | **None / Minimal** |
| **Crash Safety** (real `kill -9` tested) | Cluster quorum | Broker journaling | None (snapshot/AOF trade-offs) | WAL recovery | **Yes, sub-process `kill -9` verified** |
| **Target Scale** | Millions msg/s, distributed | Tens of thousands msg/s | High throughput, volatile | Thousands msg/s, single-node | **Up to ~920k in-mem / ~2.4k durable ops/s** |

---

## 60-Second Demo

Start the standalone daemon, push a task from your terminal, and consume it from Python (or any HTTP client) — no specialized SDK required:

```bash
# 1. Start the server (binds 127.0.0.1:8080)
cargo run --release --bin nesso &

# 2. Push a task with priority 5
curl -s -X POST http://localhost:8080/v1/queues/demo/push \
  -H "Content-Type: application/json" \
  -d '{"payload":"aGVsbG8=","priority":5}'
```

```python
import requests, base64

# 3. Pop & lease task with 30-second timeout
response = requests.post(
    "http://localhost:8080/v1/queues/demo/pop",
    json={"consumer_id": 1, "lease_secs": 30}
)
task = response.json()
print("Consumed payload:", base64.b64decode(task["payload"]).decode())  # "hello"

# 4. Acknowledge task completion
requests.post(
    f"http://localhost:8080/v1/queues/demo/tasks/{task['id']}/ack",
    json={"consumer_id": 1}
)
```

---

## Performance

Benchmarked against SQLite (WAL mode) used as a persistent queue, on identical hardware (Apple Silicon, APFS NVMe SSD), with **disk-verified data integrity per run** (not just client counters).

Full methodology, raw per-run reproducible data, and known platform limitations are documented in [`BENCHMARK.md`](./BENCHMARK.md).

| Scenario | Nesso (In-Process) | SQLite (In-Process) | Nesso (HTTP) |
|---|---|---|---|
| **Push**, no fsync, 1 thread | **~929k ops/s** (p99: 1 µs) | ~58k ops/s (p99: 37 µs) | ~23k ops/s (p99: 73 µs) |
| **Push**, no fsync, 16 threads | **~277k ops/s** (p99: 0.7 ms) | ~18k ops/s (p99: 0.2 ms) | ~107k ops/s (p99: 0.4 ms) |
| **Push**, fsync, 1 thread (Adaptive) | **~247 ops/s** (p50: **4.0 ms**, p99: 5.4 ms) | ~12k ops/s (p99: 0.2 ms) | **~246 ops/s** (p50: **4.0 ms**, p99: 5.2 ms) |
| **Push**, fsync, 16 threads | **~3,372 ops/s** (p99: **23.2 ms**) | ~2,151 ops/s (p99: **75.3 ms**) | **~2,591 ops/s** (p99: **19.0 ms**) |
| **Pop+Ack**, fsync, 16 threads | **~1,079 ops/s** (p99: **36.2 ms**) | ~1,722 ops/s (p99: **91.9 ms**) | **~984 ops/s** (p99: **43.5 ms**) |

> [!IMPORTANT]
> **Transparent Analysis: Low Concurrency vs. High Concurrency Durability Trade-Offs**
> - **At Low Concurrency (1 Thread, `sync=true`)**: SQLite outperforms Nesso (~12,485 ops/s vs. 247 ops/s, a ~50x gap). Nesso pays the full physical cost of an isolated, hardware-level `fsync` ($\sim 4\text{ms}$ on SSD) for every individual operation, guaranteeing immediate disk durability before returning. SQLite in WAL mode amortizes disk writes through internal checkpoint policies, providing a different durability trade-off when isolated.
> - **At High Concurrency (16 Threads, `sync=true`)**: The paradigm reverses. SQLite suffers from severe database-level file lock contention (`busy_timeout`), collapsing by 82% to 2,151 ops/s with p99 tail latency exploding to **75.3 ms** (push) and **91.9 ms** (pop+ack). In contrast, Nesso's group commit amortizes the physical fsync cost across concurrent operations without lock contention, scaling to **3,372 ops/s** with p99 latency bounded at **23.2 ms** (**+56% higher throughput, 69% lower tail latency than SQLite**).
> 
> See [`BENCHMARK.md`](./BENCHMARK.md) for full reproducible numbers, hardware details, and architecture breakdown.

---

## Key Highlights

- **Zero External Dependencies in Storage Core**: The storage engine (`record.rs`, `wal.rs`, `engine.rs`, `group_commit.rs`) relies strictly on the Rust standard library (`std`).
- **Zero `unsafe` Code**: 100% safe Rust with robust mutex poison recovery and failure isolation.
- **Crash Durability & Real Recovery**: Tested with real subprocess `kill -9` injection. Survives ungraceful termination without data loss or WAL corruption.
- **High-Throughput Group Commit with Adaptive Early Commit**: Dynamic cooperative batching of `fsync` operations with sub-millisecond idle detection, scaling single-writer durable commits up to physical disk limits without idle latency penalties.
- **Two-Phase Non-Blocking Background Compaction**: Phase 1 (heavy segment scanning, deduplication, `.compacting` writing, `fsync`) runs completely outside the engine lock. Phase 2 (atomic rename and memory remapping) executes under lock in $< 1\text{ms}$. Client writes never stall.
- **Concurrent Readers / Single Writer**: Fast sequential disk appends under lock; non-blocking read-only lookups executed concurrently outside the lock through an internal LRU file cache.
- **Graceful Shutdown**: Intercepts `SIGINT` and `SIGTERM`, forces immediate flush of pending group-commit batches, terminates background lease-expiration threads cleanly, and shuts down Axum HTTP connections.
- **RESTful HTTP Interface**: Built on Axum/Tokio with long-polling (`wait_secs`) featuring provable missed-wakeup prevention.

---

## Architecture Overview

```
                        +---------------------------------------------+
                        |               HTTP REST API                 |
                        |      Axum / Tokio (Long Polling)            |
                        +---------------------------------------------+
                                       |              ^
                        push/pop/ack/compact     read_at (non-blocking)
                                       v              |
+-----------------------------------------------------+-----------------------+
|  EngineState (Mutex)                                |  WalReader (LRU Cache)|
|  - in-memory BinaryHeap PriorityQueue (ready_queue) |  - Mutex<Vec<(id, File)>>
|  - Active Leases (leased)                           |  - Concurrent I/O     |
|  - Task Indices (index, data_index)                 +-----------------------+
|  - Group Commit Coordinator (fsync batching)                                |
+-----------------------------------------------------------------------------+
                                       |
                     Single-Writer Append / Atomic Compaction
                                       v
+-----------------------------------------------------------------------------+
|  Segmented WAL on Filesystem                                                |
|  - nesso.00001.wal (Compacted survivor records)                             |
|  - nesso.00002.wal (Closed segment)                                         |
|  - nesso.00003.wal (Active segment - append only, never compacted)          |
+-----------------------------------------------------------------------------+
```

### 1. Storage & State Machine
- **Record Structure**: 19-byte binary header (`MAGIC_BYTE`, `ID`, `OpType`, `Priority`, `CRC32`, `PayloadLen`) followed by the raw payload.
- **State Transitions**: `Created` $\rightarrow$ `Leased` $\rightarrow$ `Acked` (terminal) or `Nacked`/`Expired` (re-enqueued with retry counter) or `DeadLettered` (terminal, max retries reached).
- **Index Separation**: 
  - `index`: Tracks `(segment_id, offset)` of the *latest* state event for each task.
  - `data_index`: Permanently maps `(segment_id, offset)` of the immutable task payload record (`Created`).

### 2. Group Commit Batching
When durable writes are requested (`?sync=true`), Nesso coordinates threads to share `fsync` cost:
- Writes (`write_all`) are flushed to kernel page cache under the state lock.
- Threads join an active sync epoch; a designated leader performs a single blocking `fsync` (`sync_data`) for the whole batch.
- **Adaptive Early Commit**: Rather than waiting blindly for the entire `batch_window`, the leader commits immediately if no new arrivals occur for `idle_commit_threshold` (default: 250µs). This eliminates artificial idle latency under low concurrency without degrading high-concurrency batching.
- The batch closes on the first of:
  1. Reaching `max_batch_size`.
  2. Expiration of `batch_window` (maximum upper bound).
  3. No arrivals for `idle_commit_threshold` (adaptive early commit).
  4. Explicit graceful shutdown flush.
- Maximum sustainable throughput follows:
  $$\text{Throughput}_{\max} = \frac{\text{batch\_size}}{\min(\text{batch\_window}, \text{idle\_commit\_threshold}) + \text{fsync\_latency}}$$

#### Tuning Group Commit for Your Hardware
Group Commit parameters default to:
- `batch_window`: **2ms** (maximum upper bound).
- `max_batch_size`: **64** (maximum batch capacity).
- `idle_commit_threshold`: **250µs** (adaptive early commit threshold).

1. **Measure your disk's physical sync latency**:
   ```bash
   # Linux (direct I/O + sync flush)
   dd if=/dev/zero of=testfile bs=4k count=1 oflag=direct,sync
   
   # macOS
   dd if=/dev/zero of=testfile bs=4k count=1
   ```
2. **Calculate `batch_window`**: Set to approximately half your measured fsync latency ($\text{fsync} / 2$).
3. **Calculate `max_batch_size`**: $\text{Target\_Throughput} \times \text{fsync\_latency}$ (e.g. $10{,}000 \times 0.004 = 40 \implies 64$).
4. **Calculate `idle_commit_threshold`**: Set to 5–10% of fsync latency (e.g. 200–300µs) to eliminate idle wait without sacrificing burst batching.

Configure these via CLI or in code:
```bash
cargo run --release --bin nesso -- \
  --batch-window-ms 2 \
  --max-batch-size 64 \
  --idle-commit-threshold-us 250
```
Or via Rust API:
```rust
let config = GroupCommitConfig {
    batch_window: Duration::from_millis(2),
    max_batch_size: 64,
    idle_commit_threshold: Duration::from_micros(250),
};
let engine = Engine::open_with_config("./my_queue", config)?;
```

### 3. Two-Phase Non-Blocking Online Compaction
- **Phase 1 (Outside Engine Lock)**: Scans closed segments ($1 \dots N-1$), discards terminal tasks (`Acked`, `DeadLettered`), deduplicates intermediate states, writes surviving records to `nesso.00001.compacting`, and calls `sync_data()`. Normal client operations proceed uninterrupted.
- **Phase 2 (Under Engine Lock, $< 1\text{ms}$)**: Atomically renames `nesso.00001.compacting` $\rightarrow$ `nesso.00001.wal`, unlinks obsolete closed segments, remaps in-memory indexes with tombstone protection, and clears the `WalReader` LRU cache.

---

## Current Limitations & Roadmap

We believe in being upfront about what Nesso is and is not:

- **Single-Node Only**: Nesso is designed for embedded single-node applications or local microservice task queues. It does not provide distributed Raft consensus, multi-node clustering, or cross-node replication.
- **Client-Supplied Consumer ID**: `consumer_id` is an application-provided integer. Nesso does not implement authentication, RBAC, or cryptographic multi-tenant isolation.
- **Platform Verification**: Benchmarks were conducted on macOS Darwin (APFS); automated Linux CI benchmarks (`fdatasync` on ext4/XFS) are currently being set up.
- **Compaction Trigger**: Compaction is triggered on-demand via API (`/compact`) or embedded call (`engine.compact()`). An automatic heuristic background scheduler is on the roadmap.

---

## Embedded Rust Usage

Add Nesso to your project or use it directly as an embedded storage engine:

```rust
use nesso::storage::engine::Engine;

fn main() -> std::io::Result<()> {
    // Open or create queue directory (uses default GroupCommitConfig)
    let engine = Engine::open("./my_queue_data", None)?;

    // Push task (payload: Vec<u8>, priority: u8)
    let task_id = engine.push(b"Process payment #1234".to_vec(), 5)?;

    // Lease task to consumer #42 for 15 seconds
    if let Some((record, retries)) = engine.pop_and_lease(42, 15)? {
        println!("Received task #{}: {:?}", record.id(), record.payload());
        
        // Acknowledge task completion
        engine.ack(record.id(), 42)?;
    }

    // Consolidate closed log segments on disk (non-blocking)
    engine.compact()?;

    Ok(())
}
```

---

## Quickstart

### 1. Build and Test

```bash
# Build release binaries
cargo build --release

# Run entire test suite (74 integration, unit, and concurrency tests)
cargo test
```

### 2. Run the Server

```bash
# Start Nesso server with default settings (127.0.0.1:8080, ./nesso_data)
cargo run --release --bin nesso

# Or with custom tuning:
cargo run --release --bin nesso -- \
  --http-workers 4 \
  --batch-window-ms 2 \
  --max-batch-size 64 \
  --idle-commit-threshold-us 250 \
  --data-dir ./nesso_data \
  --bind 127.0.0.1:8080
```

#### CLI Options
| Flag | Default | Description |
|---|---|---|
| `--http-workers <N>` | CPU cores | Tokio runtime worker threads for HTTP transport concurrency. |
| `--batch-window-ms <N>` | `2` | Group commit batch window in milliseconds. |
| `--max-batch-size <N>` | `64` | Maximum operations per physical fsync batch. |
| `--idle-commit-threshold-us <N>` | `250` | Microseconds of inactivity before leader commits an under-filled batch early. |
| `--data-dir <PATH>` | `./nesso_data` | Storage directory for queue WAL and state files. |
| `--bind <ADDR>` | `127.0.0.1:8080` | Network socket address to bind the HTTP server to. |

> [!NOTE]
> **Understanding `--http-workers`**: This flag configures the Tokio multi-threaded worker pool handling HTTP network I/O (socket accepts, JSON serialization, event loop polling). It enables parallel request processing across **multiple independent queues**, high-throughput concurrent consumer polling, and hundreds of non-blocking long-polling listeners. It does **not** bypass or alter the single-writer per queue model, which is bounded by disk WAL serialization and fsync latency.

### 3. Run the Embedded Worker Pool Example

```bash
cargo run --example worker_pool
```

---

## HTTP REST API Reference

All requests and responses use JSON. Payloads are Base64-encoded strings.

### Base Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Healthcheck endpoint (`200 OK`, body `"OK"`). |
| `GET` | `/v1/queues` | Returns list of all existing queue names. |
| `GET` | `/v1/queues/{queue}/status` | Returns queue depth and active lease count. |
| `POST` | `/v1/queues/{queue}/push` | Enqueues a new task with a given priority. |
| `POST` | `/v1/queues/{queue}/pop` | Leases the next highest-priority task (supports long-polling). |
| `POST` | `/v1/queues/{queue}/tasks/{id}/ack` | Acknowledges task completion (removes task). |
| `POST` | `/v1/queues/{queue}/tasks/{id}/nack` | Rejects task and re-enqueues for retry. |
| `POST` | `/v1/queues/{queue}/compact` | Triggers background online compaction of closed WAL segments. |

> **Durability Option**: Adding `?sync=true` to `/push`, `/pop`, `/ack`, and `/nack` triggers durable group-committed fsync before responding.

---

## Verification & Test Coverage

Nesso includes a comprehensive 70-test suite covering fault injection, crash resilience, and concurrency:

- **`compaction_tests.rs` (18 tests)**: Inode remapping, multi-segment deduplication, active segment isolation, non-blocking concurrent writes, tombstone non-resurrection, multi-rotation handling, repeated multi-generation cycles.
- **`concurrency_tests.rs` (5 tests)**: Multi-producer multi-consumer chaos stress, mutex poison recovery, concurrent segment rotation, double delivery prevention.
- **`crash_recovery_test.rs` (2 tests)**: Out-of-process `kill -9` crash injection during active writes with full replay and CRC32 verification.
- **`group_commit_config_tests.rs` (4 tests)**: Batch window / capacity customization, batch count comparison under staggered arrivals, crash recovery across config variations.
- **`http_workers_tests.rs` (2 tests)**: Deadlock-freedom verification with `--http-workers 1` under concurrent long-polling, pushes, and multi-queue loads.
- **`engine_tests.rs` (12 tests)**: Priority ordering, FIFO tie-breaking, lease expiration, retries and dead-letter queue bounds.
- **`shutdown_tests.rs` (4 tests)**: Graceful shutdown on SIGINT/SIGTERM, pending group-commit flush, long-polling 503 wakeup, worker thread join.
- **`wal_segmentation_tests.rs` (8 tests)**: Rotation boundaries, cross-segment iteration, corrupted magic byte recovery.
- **`record_tests.rs` & `wal_tests.rs` (9 tests)**: Frame encoding, CRC32 checksum validation, random payload roundtrips.
- **`server_tests.rs` (6 tests)**: HTTP REST CRUD flow, long-polling timeout, wakeup on push/nack/expiration, non-blocking compaction isolation.

---

## License

MIT License. Developed for high-performance, fault-tolerant systems engineering in Rust.
