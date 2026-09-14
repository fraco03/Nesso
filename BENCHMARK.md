# Nesso vs. SQLite Benchmark Report

This document presents a reproducible, empirical performance benchmark comparing **Nesso** (an embedded, zero-dependency persistent queue written in 100% safe Rust) against **SQLite** (WAL mode, the industry standard embedded database often used as a makeshift job queue).

Benchmarks were conducted on Apple Silicon (macOS Darwin arm64) using NVMe storage (APFS). Every individual run verifies **disk-level data integrity** against atomic client counters.

---

## 1. Executive Summary

With the introduction of the **In-Memory Hot Path Payload Cache**, **$O(1)$ Priority Bucket Queue**, **Zero-Allocation Sync Descriptors**, and **Zero-Sleep Adaptive Immediate Commit**, Nesso outperforms SQLite across every concurrency tier and durability configuration.

Both engines are benchmarked on identical, fair durability footing: standard POSIX `fsync` (`SyncMode::Standard` in Nesso, `PRAGMA synchronous = FULL` in SQLite WAL mode), guaranteeing 100% crash durability against process crashes, segfaults, and `kill -9` termination.

### High-Level Comparison Summary

| Metric / Scenario | Nesso (In-Process) | SQLite (In-Process) | Advantage |
|---|---|---|---|
| **Push (`sync=true`, 1 Thread)** | **56,985 ops/s** (p50: **0.017 ms**) | 19,030 ops/s (p50: 0.046 ms) | **3.0x faster** (63% lower latency) |
| **Pop+Ack (`sync=true`, 1 Thread)** | **30,157 ops/s** (p50: **0.031 ms**) | 16,654 ops/s (p50: 0.055 ms) | **1.8x faster** (44% lower latency) |
| **Push (`sync=true`, 4 Threads)** | **46,032 ops/s** (p99: **0.096 ms**) | 12,518 ops/s (p99: 0.181 ms) | **3.7x faster** (47% lower p99) |
| **Push (`sync=true`, 16 Threads)** | **29,674 ops/s** (p99: **3.715 ms**) | 2,466 ops/s (p99: **74.169 ms**) | **12.0x faster** (20x lower tail latency) |
| **Pop+Ack (`sync=true`, 16 Threads)** | **20,259 ops/s** (p99: **4.950 ms**) | 1,639 ops/s (p99: **103.961 ms**) | **12.4x faster** (21x lower tail latency) |
| **Push (`sync=false`, 1 Thread)** | **899,860 ops/s** (p99: **1 µs**) | 57,906 ops/s (p99: 32 µs) | **15.5x faster** |
| **Pop+Ack (`sync=false`, 1 Thread)** | **475,978 ops/s** (p99: **3 µs**) | 66,480 ops/s (p99: 20 µs) | **7.2x faster** |
| **Push (`sync=false`, 16 Threads)** | **272,055 ops/s** (p99: **0.65 ms**) | 19,529 ops/s (p99: 0.17 ms) | **13.9x faster** |

---

## 2. Test Environment & Methodology

- **OS**: macOS Darwin (arm64, Apple Silicon)
- **Filesystem**: APFS (NVMe Solid State Drive)
- **Rust Toolchain**: `rustc` 1.85+ (profile: `release`, optimized)
- **SQLite Version**: `rusqlite` 0.40.2 (SQLite 3.45+) configured with:
  - `PRAGMA journal_mode = WAL;`
  - `PRAGMA synchronous = FULL;` (for `sync=true`) or `NORMAL;` (for `sync=false`)
  - `PRAGMA busy_timeout = 30000;`
- **Harness Execution**: 1 warmup run + 5 measured runs per configuration.

### Comparison Architectures

1. **Nesso In-Process**: Native Rust `Engine` invoked directly in-process via function calls. Zero serialization or network overhead. **Direct architectural apples-to-apples comparison with SQLite.**
2. **SQLite In-Process**: Standard SQLite database invoked via `rusqlite` in-process.
3. **Nesso HTTP**: Full standalone daemon architecture (`axum` HTTP server, TCP loopback, JSON serialization, Base64 payload encoding, Tokio async runtime).

### Data Integrity Verification & Task Dispatch

Unlike benchmarks that rely solely on client-side success counters, Nesso's benchmark harness verifies physical disk integrity after every individual run:
- Producers and consumers increment an `AtomicU64` counter for every confirmed operation.
- At the end of the run, the database is queried directly:
  - SQLite: `SELECT count(*) FROM queue WHERE state = ...`
  - Nesso: Cold engine inspection verifying total active entries in segment logs.
- A run is marked with `Integrity: ✅` only if the confirmed counter matches the physical records on disk bit-for-bit. **All runs achieved 100% integrity verification.**

---

## 3. Comprehensive Benchmark Results

### 3.1 Unsynchronized Mode (`sync=false`, Page Cache Flushed)

In memory/page-cache mode, writes are committed to the OS kernel page cache without waiting for physical drive sync barriers:

| System | Threads | Op | Mean Throughput | Min Throughput | Max Throughput | p50 Latency | p99 Latency |
|---|---|---|---|---|---|---|---|
| **Nesso (In-Process)** | 1 | Push | **899,860 ops/s** | 889,564 | 933,115 | 0.001 ms | 0.001 ms |
| **Nesso (In-Process)** | 1 | Pop+Ack | **475,978 ops/s** | 465,196 | 487,848 | 0.002 ms | 0.003 ms |
| SQLite (In-Process) | 1 | Push | 57,906 ops/s | 52,734 | 59,543 | 0.013 ms | 0.032 ms |
| SQLite (In-Process) | 1 | Pop+Ack | 66,480 ops/s | 65,889 | 66,923 | 0.012 ms | 0.020 ms |
| Nesso (HTTP) | 1 | Push | 22,980 ops/s | 22,494 | 23,395 | 0.042 ms | 0.076 ms |
| Nesso (HTTP) | 1 | Pop+Ack | 11,901 ops/s | 11,818 | 12,038 | 0.083 ms | 0.112 ms |
| **Nesso (In-Process)** | 4 | Push | **340,845 ops/s** | 291,669 | 356,553 | 0.002 ms | 0.113 ms |
| **Nesso (In-Process)** | 4 | Pop+Ack | **180,938 ops/s** | 176,529 | 183,441 | 0.005 ms | 0.137 ms |
| SQLite (In-Process) | 4 | Push | 51,262 ops/s | 49,341 | 53,440 | 0.012 ms | 0.038 ms |
| SQLite (In-Process) | 4 | Pop+Ack | 55,157 ops/s | 51,446 | 59,432 | 0.012 ms | 0.024 ms |
| Nesso (HTTP) | 4 | Push | 62,286 ops/s | 61,169 | 63,670 | 0.060 ms | 0.118 ms |
| Nesso (HTTP) | 4 | Pop+Ack | 31,309 ops/s | 29,380 | 32,586 | 0.122 ms | 0.236 ms |
| **Nesso (In-Process)** | 16 | Push | **272,055 ops/s** | 248,139 | 298,567 | 0.003 ms | 0.650 ms |
| **Nesso (In-Process)** | 16 | Pop+Ack | **173,808 ops/s** | 170,257 | 176,106 | 0.005 ms | 0.694 ms |
| SQLite (In-Process) | 16 | Push | 19,529 ops/s | 16,709 | 20,558 | 0.014 ms | 0.170 ms |
| SQLite (In-Process) | 16 | Pop+Ack | 23,543 ops/s | 19,875 | 35,793 | 0.014 ms | 0.719 ms |
| Nesso (HTTP) | 16 | Push | 100,524 ops/s | 93,101 | 104,105 | 0.138 ms | 0.457 ms |
| Nesso (HTTP) | 16 | Pop+Ack | 51,657 ops/s | 50,895 | 52,953 | 0.296 ms | 0.540 ms |

---

### 3.2 Synchronized Mode (`sync=true`, POSIX `fsync` Durability)

In synchronous durability mode, operations are confirmed only after standard POSIX `fsync` has flushed modified pages from the operating system cache to the drive controller:

| System | Threads | Op | Mean Throughput | Min Throughput | Max Throughput | p50 Latency | p99 Latency |
|---|---|---|---|---|---|---|---|
| **Nesso (In-Process)** | 1 | Push | **56,985 ops/s** | 55,955 | 59,019 | **0.017 ms** | **0.027 ms** |
| **Nesso (In-Process)** | 1 | Pop+Ack | **30,157 ops/s** | 28,905 | 31,431 | **0.031 ms** | **0.047 ms** |
| SQLite (In-Process) | 1 | Push | 19,030 ops/s | 16,923 | 19,822 | 0.046 ms | 0.105 ms |
| SQLite (In-Process) | 1 | Pop+Ack | 16,654 ops/s | 16,466 | 16,815 | 0.055 ms | 0.083 ms |
| Nesso (HTTP) | 1 | Push | 17,964 ops/s | 15,529 | 18,732 | 0.052 ms | 0.096 ms |
| Nesso (HTTP) | 1 | Pop+Ack | 9,203 ops/s | 8,932 | 9,392 | 0.106 ms | 0.148 ms |
| **Nesso (In-Process)** | 4 | Push | **46,032 ops/s** | 41,119 | 50,054 | **0.066 ms** | **0.096 ms** |
| **Nesso (In-Process)** | 4 | Pop+Ack | **23,231 ops/s** | 20,849 | 24,485 | **0.139 ms** | **0.342 ms** |
| SQLite (In-Process) | 4 | Push | 12,518 ops/s | 12,323 | 12,915 | 0.047 ms | 0.181 ms |
| SQLite (In-Process) | 4 | Pop+Ack | 12,070 ops/s | 11,921 | 12,284 | 0.055 ms | 0.096 ms |
| Nesso (HTTP) | 4 | Push | 42,081 ops/s | 41,447 | 42,477 | 0.090 ms | 0.161 ms |
| Nesso (HTTP) | 4 | Pop+Ack | 19,311 ops/s | 19,023 | 19,556 | 0.201 ms | 0.324 ms |
| **Nesso (In-Process)** | 16 | Push | **29,674 ops/s** | 25,468 | 40,529 | 0.372 ms | **3.715 ms** |
| **Nesso (In-Process)** | 16 | Pop+Ack | **20,259 ops/s** | 19,625 | 21,063 | 0.604 ms | **4.950 ms** |
| SQLite (In-Process) | 16 | Push | 2,466 ops/s | 1,687 | 3,723 | 0.068 ms | **74.169 ms** |
| SQLite (In-Process) | 16 | Pop+Ack | 1,639 ops/s | 1,256 | 2,082 | 0.100 ms | **103.961 ms** |
| Nesso (HTTP) | 16 | Push | **35,947 ops/s** | 31,171 | 37,365 | 0.333 ms | **1.656 ms** |
| Nesso (HTTP) | 16 | Pop+Ack | **17,569 ops/s** | 15,137 | 18,276 | 0.768 ms | **2.605 ms** |

---

## 4. Architectural Analysis & Deep Dive

### 4.1 Single-Threaded Isolation (1 Thread)
At 1 thread with `sync=true`:
- **Nesso In-Process**: **56,985 ops/s** (median latency: **17 microseconds**).
- **SQLite In-Process**: 19,030 ops/s (median latency: 46 microseconds).
- **Why Nesso wins by 3.0x**:
  1. **Compact Fixed Header Format**: Nesso writes a lean 19-byte binary header followed by raw payload bytes, compared to SQLite's B-Tree node encoding and WAL frame page format.
  2. **In-Memory Payload Cache**: Recent payloads reside in an `Arc<Vec<u8>>` cache, eliminating disk seeks and read operations during `pop_and_lease`.
  3. **Zero-Sleep Immediate Commit**: With `idle_commit_threshold: Duration::ZERO`, an isolated leader does not sleep; it syncs immediately to the storage controller.
  4. **True POSIX `fsync`**: Invoking standard POSIX `fsync` flushes operating system page cache to the drive controller in ~15–30µs, matching SQLite's exact durability boundary.

### 4.2 High Concurrency (16 Threads)
At 16 threads with `sync=true`:
- **SQLite collapses under lock contention**: SQLite's multi-process file locking model allows only a single active writer at any time. The remaining 15 threads block, spin, and retry under `busy_timeout`. As a result, throughput drops by 87% (from 19k to 2.4k ops/s) and tail latency explodes to **74–104 milliseconds**.
- **Nesso leverages cooperative Group Commit**: Rather than acquiring database-wide file locks, concurrent threads append their operations to the in-memory WAL buffer under an uncontended mutex, then coalesce their physical disk flush into a single serial `fsync`. While batch $N$ is syncing, batch $N+1$ naturally accumulates pending writes. As a result, Nesso delivers **29,674 ops/s** (push) and **20,259 ops/s** (pop+ack) with p99 latency tightly bounded at **3.7–4.95 ms** (**12.0x to 12.4x higher throughput, 20x lower tail latency than SQLite**).

---

## 5. Platform Notes on Durability (`SyncMode::Standard` vs `SyncMode::FullHardware`)

Nesso provides two configurable synchronization modes via `GroupCommitConfig::sync_mode`:

1. **`SyncMode::Standard` (Default)**:
   - Invokes standard POSIX `fsync(fd)` on Unix/macOS.
   - Flushes dirty pages from the operating system kernel cache to the storage controller cache.
   - **100% crash durability**: Fully protects against application crashes, segfaults, OS aborts, and brutal `kill -9` process termination (verified by `tests/crash_recovery_test.rs`).
   - Equivalent to the default durability level of PostgreSQL (`fsync = on`), MySQL InnoDB (`innodb_flush_log_at_trx_commit = 1`), RocksDB, and SQLite WAL (`PRAGMA synchronous = FULL`).
   - Latency on NVMe APFS: ~15–30 microseconds.

2. **`SyncMode::FullHardware`**:
   - On macOS, invokes `fcntl(fd, F_FULLFSYNC)` (via `File::sync_data()`).
   - Forces the drive controller's volatile hardware write cache to flush to physical non-volatile NAND cells.
   - Protects against sudden unbuffered host power loss (e.g. pulling the power cord without battery backup).
   - Incurs ~4.35 milliseconds latency per sync barrier on Apple Silicon NVMe drives.
