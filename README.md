# Nesso

<div align="center">

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange.svg)](https://www.rust-lang.org)
[![Tests](https://img.shields.io/badge/tests-87%20passed-brightgreen.svg)]()
[![Safety](https://img.shields.io/badge/unsafe-0%25-success.svg)]()
[![No Dependencies](https://img.shields.io/badge/storage%20core-0%20external%20deps-informational.svg)]()

**Fast, zero-dependency, crash-resilient persistent message queue written in 100% safe Rust.**

*Native lease-based task consumption, $O(1)$ hardware priority scheduling, cooperative group-commit fsync, and non-blocking two-phase compaction.*

[Why Nesso?](#why-nesso) • [Architecture](#architecture-overview) • [Benchmarks](#performance-benchmarks) • [60-Second Demo](#60-second-demo) • [Embedded API](#embedded-rust-usage) • [HTTP API](#http-rest-api-reference)

</div>

---

## Why Nesso?

When a single-node application or microservice needs a durable background task queue, engineers almost always reach for **Redis** or **SQLite**:

- **With Redis**: You trade memory safety for speed. Key expiration doesn't provide true lease semantics; atomicity requires complex Lua scripts; persistence (AOF/RDB) introduces durability trade-offs, and memory bounds risk `OOM` crashes under unexpected queue backpressure.
- **With SQLite (WAL mode)**: SQLite is a world-class relational database, but using it as a job queue suffers from severe **database-level file lock contention** (`busy_timeout`) under concurrent writers. Furthermore, you must hand-roll lease timers, poll loops, retry counters, and dead-letter cleanup tables.
- **With Kafka or RabbitMQ**: Operating a distributed cluster (with ZooKeeper/KRaft or Erlang VMs) introduces massive operational complexity when all you need is reliable persistence for a single service.

**Nesso solves this problem natively.** It provides a persistent, embeddable queue (or lightweight daemon) with **zero external dependencies** in its storage engine, built-in task leases, automatic retry escalation, dead-letter routing, and verifiable crash durability tested against subprocess `kill -9` termination.

---

## Where Nesso Fits

| Feature / Dimension | Kafka | RabbitMQ | Redis | SQLite (WAL queue) | **Nesso** |
|---|---|---|---|---|---|
| **Architecture** | Distributed cluster | Broker cluster | In-memory key-value | Embedded relational DB | **Embedded engine or local daemon** |
| **Native Queue Semantics** | Partial (offsets) | Yes (AMQP) | No (custom Lua/Streams) | No (custom SQL tables) | **Built-in (Lease, TTL, NACK, DLQ)** |
| **Operational Overhead** | High (JVM, KRaft) | Medium-High (Erlang) | Low-Medium | Zero | **Zero (single binary / Rust crate)** |
| **Storage Dependencies** | External | External | Memory + Disk | None (C library) | **0 external crates in storage core (`std` only)** |
| **Crash Safety Verification** | Quorum replication | Journaling | Periodic snapshot / AOF | WAL rollback | **Subprocess `kill -9` tested & verified** |
| **Max Throughput** | Millions msg/s (cluster) | Tens of thousands msg/s | ~100k msg/s (volatile) | ~20k sync / ~60k async | **~57k durable sync / ~900k async ops/s** |

---

## Performance Benchmarks

Benchmarked against **SQLite (WAL mode)** on identical hardware: Apple Silicon (macOS Darwin arm64, APFS NVMe SSD). 

Every single benchmark run verifies **bit-exact disk-level data integrity** against atomic client counters (`Integrity: ✅`). Both engines were tested at equivalent durability: standard POSIX `fsync` (`SyncMode::Standard` for Nesso, `PRAGMA synchronous = FULL` for SQLite).

> Reproducible via `cargo run --release --bin bench`. Full raw data and per-run variance are documented in [`BENCHMARK.md`](./BENCHMARK.md).

### 1. Synchronous Durability (`sync=true`, POSIX `fsync` on every operation)

| Scenario | Nesso (In-Process) | SQLite (In-Process) | Nesso (HTTP) | Speedup vs SQLite |
|---|---|---|---|---|
| **Push, 1 Thread** | **56,985 ops/s** (p50: **0.017 ms**) | 19,030 ops/s (p50: 0.046 ms) | **17,964 ops/s** (p50: 0.052 ms) | **3.0x faster** (63% lower latency) |
| **Pop+Ack, 1 Thread** | **30,157 ops/s** (p50: **0.031 ms**) | 16,654 ops/s (p50: 0.055 ms) | **9,203 ops/s** (p50: 0.106 ms) | **1.8x faster** (44% lower latency) |
| **Push, 4 Threads** | **46,032 ops/s** (p99: **0.096 ms**) | 12,518 ops/s (p99: 0.181 ms) | **42,081 ops/s** (p99: 0.161 ms) | **3.7x faster** |
| **Pop+Ack, 4 Threads** | **23,231 ops/s** (p99: **0.342 ms**) | 12,070 ops/s (p99: 0.096 ms) | **19,311 ops/s** (p99: 0.324 ms) | **1.9x faster** |
| **Push, 16 Threads** | **29,674 ops/s** (p99: **3.715 ms**) | 2,466 ops/s (p99: **74.169 ms**) | **35,947 ops/s** (p99: **1.656 ms**) | **12.0x faster** (20x lower p99) |
| **Pop+Ack, 16 Threads** | **20,259 ops/s** (p99: **4.950 ms**) | 1,639 ops/s (p99: **103.961 ms**) | **17,569 ops/s** (p99: **2.605 ms**) | **12.4x faster** (21x lower p99) |

### 2. Unsynchronized Mode (`sync=false`, OS Page Cache Buffered)

| Scenario | Nesso (In-Process) | SQLite (In-Process) | Nesso (HTTP) | Speedup vs SQLite |
|---|---|---|---|---|
| **Push, 1 Thread** | **899,860 ops/s** (p99: 1 µs) | 57,906 ops/s (p99: 32 µs) | **22,980 ops/s** (p99: 76 µs) | **15.5x faster** |
| **Pop+Ack, 1 Thread** | **475,978 ops/s** (p99: 3 µs) | 66,480 ops/s (p99: 20 µs) | **11,901 ops/s** (p99: 112 µs) | **7.2x faster** |
| **Push, 16 Threads** | **272,055 ops/s** (p99: 0.65 ms) | 19,529 ops/s (p99: 0.17 ms) | **100,524 ops/s** (p99: 0.46 ms) | **13.9x faster** |
| **Pop+Ack, 16 Threads** | **173,808 ops/s** (p99: 0.69 ms) | 23,543 ops/s (p99: 0.72 ms) | **51,657 ops/s** (p99: 0.54 ms) | **7.4x faster** |

> [!TIP]
> **Why Nesso Outperforms SQLite:**
> 1. **At Low Concurrency (1 Thread)**: Nesso uses a compact 19-byte binary frame with direct sequential disk appends, an in-memory hot-path payload cache, and zero-sleep immediate commits, executing an append + POSIX `fsync` in only **17 microseconds** median latency (vs 46 µs for SQLite).
> 2. **At High Concurrency (16 Threads)**: SQLite's single active writer design causes heavy file lock contention (`busy_timeout`), collapsing throughput to ~2.4k ops/s and ballooning tail latency to **74–104 ms**. In contrast, Nesso's **Group Commit** coordinator amortizes a single serial `fsync` across the batch while subsequent writers enqueue in RAM, maintaining **~30,000 ops/s** with p99 latency bounded under **4 ms**.

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

# 3. Pop & lease task with 30-second timeout (supports long-polling with wait_secs)
response = requests.post(
    "http://localhost:8080/v1/queues/demo/pop",
    json={"consumer_id": 1, "lease_secs": 30, "wait_secs": 5}
)
task = response.json()
print("Consumed payload:", base64.b64decode(task["payload"]).decode())  # "hello"

# 4. Acknowledge task completion (removes from WAL on next compaction)
requests.post(
    f"http://localhost:8080/v1/queues/demo/tasks/{task['id']}/ack",
    json={"consumer_id": 1}
)
```

---

## Architecture Overview

```
                        +---------------------------------------------+
                        |               HTTP REST API                 |
                        |      Axum / Tokio (Long Polling)            |
                        +---------------------------------------------+
                                       |              ^
                         push / pop / ack / compact   read_at (non-blocking)
                                       v              |
+-----------------------------------------------------+-----------------------+
|  EngineState (Mutex)                                |  WalReader (LRU Cache)|
|  - O(1) PriorityBucketQueue (256 discrete queues)   |  - Mutex<Vec<(id, File)>>
|  - Hot-Path PayloadCache (shared buffer pool)       |  - Fallback disk read |
|  - Active Leases (leased)                           +-----------------------+
|  - Task Indices (index, data_index)                                         |
|  - Group Commit Coordinator (adaptive serial fsync)                         |
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

### Core Components

1. **Segmented Write-Ahead Log (WAL)**:
   - Every operation is encoded with a 19-byte binary header: `[MAGIC_BYTE (1B) | ID (8B) | OpType (1B) | Priority (1B) | CRC32 (4B) | PayloadLen (4B)]` followed by payload bytes.
   - Segments roll over automatically when reaching `max_segment_size` (default: 64MB).
   - Only closed segments ($< \text{active\_segment\_id}$) are eligible for background compaction.

2. **$O(1)$ Hardware-Accelerated Priority Bucket Queue**:
   - Priorities are 8-bit integers (`u8`, 0..=255).
   - Rather than paying $O(\log n)$ comparison and heap rebalancing overhead, Nesso uses **256 discrete FIFO queues** (`VecDeque`) backed by a 256-bit bitmap (`[u64; 4]`).
   - The next highest-priority task is found in $\le 4$ operations using CPU hardware bit-scan (`clz` / `leading_zeros()`), preserving strict FIFO ordering within identical priorities.

3. **In-Memory Hot Path Payload Cache**:
   - Holds recently pushed payloads (`Arc<Vec<u8>>`) in a bounded buffer pool.
   - On `pop_and_lease`, hot tasks return payload bytes directly from RAM in nanoseconds, eliminating disk seeks and read-lock contention on `WalReader`.
   - The WAL remains the sole durable ground truth: cold tasks (or post-restart tasks) seamlessly fall back to non-blocking disk reads.

4. **Cooperative Group Commit with Adaptive Early Commit**:
   - Writers append to the WAL buffer in RAM, release the engine mutex, and join the active sync epoch.
   - A single designated committer executes the physical `fsync`, strictly serial with respect to the previous batch.
   - **Adaptive Zero-Wait Commit**: When a thread runs in isolation, it executes `fsync` immediately without artificial sleep delays. Under concurrent bursts, requests naturally coalesce behind the running `fsync`.

5. **Two-Phase Non-Blocking Background Compaction**:
   - **Phase 1 (Outside Engine Lock)**: Reads closed segments, deduplicates task state history, discards terminal tasks (`Acked`, `DeadLettered`), and writes surviving records to `nesso.00001.compacting`. Client writes proceed at full speed.
   - **Phase 2 (Under Engine Lock, $< 1\text{ms}$)**: Atomically renames the file over segment 1, unlinks obsolete segments, remaps index offsets, and invalidates stale LRU cache entries.

6. **Configurable Synchronization Modes (`SyncMode`)**:
   - `SyncMode::Standard` (Default): Uses standard POSIX `fsync(fd)`. Flushes dirty OS pages to the drive controller. 100% crash durability against application crashes and `kill -9` (~15–30 µs latency).
   - `SyncMode::FullHardware`: Invokes `fcntl(fd, F_FULLFSYNC)` on macOS (via `File::sync_data()`), forcing the SSD controller write cache to flush to physical non-volatile NAND cells (~4.3 ms latency).

---

## Embedded Rust Usage

Add Nesso directly to your Rust application as an in-process persistent queue:

```rust
use nesso::storage::engine::{Engine, GroupCommitConfig, SyncMode};
use std::time::Duration;

fn main() -> std::io::Result<()> {
    // 1. Configure durable group commit options
    let config = GroupCommitConfig {
        batch_window: Duration::from_millis(2),
        max_batch_size: 64,
        idle_commit_threshold: Duration::ZERO, // Immediate commit when isolated
        sync_mode: SyncMode::Standard,
    };

    // 2. Open or recover the queue directory
    let engine = Engine::open_with_config("./queue_data", config)?;

    // 3. Enqueue a task (payload: Vec<u8>, priority: 0..=255)
    let task_id = engine.push(b"Process invoice #4582".to_vec(), 10)?;
    engine.sync()?; // Wait for durable fsync barrier

    // 4. Pop and lease to consumer #1 for 30 seconds
    if let Some((record, retries)) = engine.pop_and_lease(1, 30)? {
        println!("Worker received task #{}: {:?}", record.id(), record.payload());

        // 5. Acknowledge successful completion
        engine.ack(record.id(), 1)?;
        engine.sync()?;
    }

    // 6. Consolidate closed log segments on disk (non-blocking)
    engine.compact()?;

    Ok(())
}
```

---

## Quickstart

### 1. Build and Test

```bash
# Build optimized release binaries
cargo build --release

# Run entire test suite (87 tests covering crash recovery, concurrency, and compactions)
cargo test
```

### 2. Run the Standalone Daemon

```bash
# Start Nesso server with default settings (127.0.0.1:8080, ./nesso_data)
cargo run --release --bin nesso

# Or with custom tuning:
cargo run --release --bin nesso -- \
  --http-workers 4 \
  --batch-window-ms 2 \
  --max-batch-size 64 \
  --idle-commit-threshold-us 0 \
  --sync-mode standard \
  --data-dir ./nesso_data \
  --bind 127.0.0.1:8080
```

#### CLI Options
| Flag | Default | Description |
|---|---|---|
| `--http-workers <N>` | CPU cores | Tokio runtime worker threads for HTTP transport concurrency. |
| `--batch-window-ms <N>` | `2` | Group commit batch window in milliseconds. |
| `--max-batch-size <N>` | `64` | Maximum operations accumulated per physical fsync batch. |
| `--idle-commit-threshold-us <N>` | `0` | Microseconds of inactivity before committing early (0 = immediate). |
| `--sync-mode <standard\|full>` | `standard` | Durability mode: `standard` (POSIX `fsync`), `full` (`F_FULLFSYNC`). |
| `--data-dir <PATH>` | `./nesso_data` | Storage directory for queue WAL and state files. |
| `--bind <ADDR>` | `127.0.0.1:8080` | Network socket address to bind the HTTP server to. |

### 3. Run the Embedded Worker Pool Example

```bash
# Run the multi-threaded embedded worker pool example
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
| `POST` | `/v1/queues/{queue}/push` | Enqueues a new task with priority (`0..=255`). |
| `POST` | `/v1/queues/{queue}/pop` | Leases next task (supports long-polling via `wait_secs`). |
| `POST` | `/v1/queues/{queue}/tasks/{id}/ack` | Acknowledges task completion. |
| `POST` | `/v1/queues/{queue}/tasks/{id}/nack` | Rejects task and re-enqueues with incremented retry count. |
| `POST` | `/v1/queues/{queue}/compact` | Triggers background online compaction of closed segments. |

> **Durability Option**: Adding `?sync=true` to any mutating endpoint (`/push`, `/pop`, `/ack`, `/nack`) blocks until the physical `fsync` barrier completes before responding.

---

## Verification & Test Coverage (87 Tests Passing)

Nesso includes an extensive test suite verifying failure resilience, memory safety, and concurrency:

- **`compaction_tests.rs` (18 tests)**: Inode remapping, multi-segment global deduplication, active segment isolation, non-blocking concurrent writes, tombstone protection, multi-generation compaction cycles.
- **`concurrency_tests.rs` (5 tests)**: Multi-producer multi-consumer chaos stress, mutex poison recovery, concurrent segment rotation, double-delivery prevention.
- **`crash_recovery_test.rs` (2 tests)**: Out-of-process `kill -9` crash injection during active writes with cold reboot replay and CRC32 verification.
- **`group_commit_config_tests.rs` (9 tests)**: Batch window/capacity customization, adaptive early commit, staggered arrival coalescing, `SyncMode::Standard` vs `SyncMode::FullHardware` durability execution.
- **`http_workers_tests.rs` (2 tests)**: Deadlock-freedom verification with `--http-workers 1` under concurrent long-polling, pushes, and multi-queue loads.
- **`payload_cache_tests.rs` (3 tests)**: In-memory cache hit validation, cold-read fallback after reboot, bounded eviction on terminal ACK/dead-letter.
- **`priority_queue_integration_tests.rs` (2 tests)**: Strict priority dispatch ordering and concurrent MPMC scheduling across all 256 priority levels.
- **`engine_tests.rs` (12 tests)**: Priority ordering, FIFO tie-breaking, lease expiration, retries and dead-letter queue bounds.
- **`shutdown_tests.rs` (4 tests)**: Graceful shutdown on SIGINT/SIGTERM, pending group-commit flush, long-polling 503 wakeup, worker thread join.
- **`wal_segmentation_tests.rs` (8 tests)**: Rotation boundaries, cross-segment iteration, corrupted magic byte recovery.
- **`record_tests.rs` & `wal_tests.rs` (9 tests)**: Frame encoding, CRC32 checksum validation, random payload roundtrips.
- **`server_tests.rs` (6 tests)**: HTTP REST CRUD flow, long-polling timeout, wakeup on push/nack/expiration, non-blocking compaction isolation.
- **Storage Unit Tests (7 tests)**: Bitmap hardware bit-scan indexing, cache boundary eviction, and FIFO queues.

---

## Current Limitations & Scope

We believe in being transparent about what Nesso is and what it is not:

- **Single-Node Architecture**: Nesso is engineered for single-node embedded usage or local microservice task queues. It does not provide distributed Raft consensus, multi-node clustering, or cross-datacenter replication.
- **Memory Footprint**: The task index (`index: HashMap<u64, (u64, u64)>`) lives in RAM, requiring approximately ~40 bytes per active non-compacted task.
- **Authentication**: `consumer_id` is an application-provided integer. Nesso does not implement user authentication, RBAC, or cryptographic multi-tenant isolation; it assumes a trusted internal network or embedded application context.

---

## License

MIT License. Designed and engineered for high-performance, fault-tolerant systems in Rust.

