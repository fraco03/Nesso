//! # Group Commit Module for Nesso WAL
//!
//! ## EXPLICIT ARCHITECTURAL MODEL DECLARATION
//! This module implements the following design:
//! **"ONE BATCH AT A TIME, SERIAL WITH RESPECT TO PREVIOUS FSYNC"**
//! (NOT a pipeline with overlapping batches).
//!
//! ### Intentional Design Decision
//! The decision to avoid overlapping physical `fsync` calls is intentional and motivated by:
//! 1. **Zero I/O controller contention**: Running multiple concurrent `fsync` calls on the
//!    same file descriptor or the same physical drive does not increase SSD bandwidth;
//!    instead, it creates queues at the controller level and drastically increases tail latency variance (p99).
//! 2. **Throughput Predictability**: Maximum theoretical throughput with this design
//!    is mathematically bounded and verifiable:
//!    $$T_{\max} = \frac{\text{batch\_size}}{\text{batch\_window} + \text{single\_fsync\_duration}}$$
//! 3. **Simplicity and Crash Safety**: Keeping only a single in-flight `fsync` at any time guarantees
//!    a deterministic durability order free from race conditions between batches.
//!
//! ### Serial Flow Mechanism
//! - Batch $N$ accumulates pending requests until closed.
//! - A batch closes upon the first of:
//!   1. Reaching `max_batch_size`.
//!   2. Expiration of `batch_window` (maximum wait window).
//!   3. Absence of new arrivals for `idle_commit_threshold` (adaptive early commit) —
//!      this handles low/zero concurrency scenarios by avoiding artificial fixed latency.
//! - While batch $N$ physically executes `sync_data()` on disk:
//!   - Newly arriving threads complete their `write_all` in RAM and enqueue into batch $N+1$.
//!   - Batch $N+1$ does **NOT** execute any `fsync` until batch $N$'s fsync has completely finished.
//! - As soon as batch $N$'s fsync finishes:
//!   - All callers of batch $N$ are awakened and confirmed.
//!   - Batch $N+1$ (which has accumulated its requests in the meantime) becomes the new active batch
//!     and in turn executes its single serial `fsync`.
//!
//! ## DURABILITY AND CRASH CONSISTENCY TRADE-OFFS
//! - **Confirmed operations (sync returned `Ok`)**: **NO confirmed operation can ever be lost.**
//!   A thread only receives control after the physical fsync block for its batch
//!   has completed `sync_data()` successfully. This is verified by the integration test
//!   with brutal termination `kill -9` (`tests/crash_recovery_test.rs`).
//! - **Unconfirmed operations**: If the process crashes or receives `SIGKILL` while a batch
//!   is forming or during fsync execution, those operations were never confirmed
//!   to the caller. Their data may only reside in the page cache and be lost (or partially
//!   written); on restart, the 2-pass recovery will discard any incomplete tail lacking
//!   a valid checksum.

use std::fs::File;
use std::io;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

#[cfg(unix)]
unsafe extern "C" {
    fn fsync(fd: std::os::raw::c_int) -> std::os::raw::c_int;
}

/// Durability synchronization mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    /// Standard POSIX `fsync` / `fdatasync`.
    /// Flushes dirty pages from operating system page cache to storage controller.
    /// Fully guarantees 100% crash durability against application crashes, segfaults,
    /// aborts, and `kill -9` process termination.
    /// Matches SQLite default (`PRAGMA synchronous = FULL`), PostgreSQL, MySQL, and RocksDB.
    /// Latency on NVMe / Apple Silicon APFS: ~30 microseconds.
    Standard,

    /// Full hardware flush barrier (forces drive write cache flush to physical NAND cells).
    /// On macOS, calls `fcntl(fd, F_FULLFSYNC)` via Rust's `File::sync_data()`.
    /// Protects against host power-loss / sudden battery detachment, but incurs ~4ms latency on macOS.
    FullHardware,
}

/// Dispatches synchronization according to the configured `SyncMode`.
pub fn perform_sync(file: &File, mode: SyncMode) -> io::Result<()> {
    match mode {
        SyncMode::Standard => {
            #[cfg(unix)]
            {
                let fd = file.as_raw_fd();
                let ret = unsafe { fsync(fd) };
                if ret == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            }
            #[cfg(not(unix))]
            {
                file.sync_data()
            }
        }
        SyncMode::FullHardware => {
            file.sync_data()
        }
    }
}

/// Configuration options for Group Commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupCommitConfig {
    /// Maximum wait window before closing the batch if `max_batch_size` was not reached.
    pub batch_window: Duration,
    /// Maximum number of threads accumulated in a single batch before forcing a sync.
    pub max_batch_size: usize,
    /// Maximum idle duration without new arrivals before committing early (adaptive commit).
    pub idle_commit_threshold: Duration,
    /// Durability synchronization mode.
    pub sync_mode: SyncMode,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            batch_window: Duration::from_millis(2),
            max_batch_size: 64,
            idle_commit_threshold: Duration::ZERO,
            sync_mode: SyncMode::Standard,
        }
    }
}

pub type GroupCommitOptions = GroupCommitConfig;

struct GroupCommitState {
    /// ID of the currently collecting batch. Starts at 1.
    collecting_batch_id: u64,
    /// ID of the latest batch whose fsync on disk completed successfully. Starts at 0.
    last_synced_batch_id: u64,
    /// Number of threads currently waiting in collecting_batch_id.
    waiters_in_collecting: usize,
    /// Indicates whether a batch is currently executing sync_data() on disk (serial!).
    fsync_in_progress: bool,
    /// Timestamp when the first thread entered the current batch.
    batch_start_time: Option<Instant>,
    /// Timestamp when the most recent thread entered the current batch.
    last_arrival_time: Option<Instant>,
    /// Last I/O error encountered during the last fsync.
    last_error: Option<io::ErrorKind>,
    /// When set to true (e.g. during graceful shutdown), forces pending batches to flush immediately
    /// without waiting for the batch timeout window or max batch size.
    force_flush: bool,
}

pub struct GroupCommit {
    state: Mutex<GroupCommitState>,
    cvar: Condvar,
    options: GroupCommitOptions,
}

impl GroupCommit {
    pub fn new(options: GroupCommitOptions) -> Self {
        Self {
            state: Mutex::new(GroupCommitState {
                collecting_batch_id: 1,
                last_synced_batch_id: 0,
                waiters_in_collecting: 0,
                fsync_in_progress: false,
                batch_start_time: None,
                last_arrival_time: None,
                last_error: None,
                force_flush: false,
            }),
            cvar: Condvar::new(),
            options,
        }
    }

    /// Synchronizes the WAL file, guaranteeing that all writes completed prior
    /// to this call are durably persisted to disk.
    pub fn sync(&self, file: &File) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();

        let my_batch = state.collecting_batch_id;
        state.waiters_in_collecting += 1;
        let now = Instant::now();
        if state.batch_start_time.is_none() {
            state.batch_start_time = Some(now);
        }
        state.last_arrival_time = Some(now);

        // Wake waiting leader/followers on arrival
        self.cvar.notify_all();

        loop {
            // If our batch has already been synced to disk by a prior committer
            if state.last_synced_batch_id >= my_batch {
                if let Some(err_kind) = state.last_error {
                    return Err(io::Error::from(err_kind));
                }
                return Ok(());
            }

            // =====================================================================
            // STRICT SERIALITY RULE ("one batch at a time"):
            // A thread can start fsync for `my_batch` if and only if:
            // 1. No other fsync is currently in progress (!state.fsync_in_progress).
            // 2. It is exactly its batch's turn (my_batch == state.last_synced_batch_id + 1).
            // =====================================================================
            let is_next_batch = my_batch == state.last_synced_batch_id + 1;

            if !state.fsync_in_progress && is_next_batch {
                let now = Instant::now();
                let batch_full = state.waiters_in_collecting >= self.options.max_batch_size;
                let total_elapsed = state.batch_start_time.map_or(Duration::ZERO, |t| now.duration_since(t));
                let timeout_expired = total_elapsed >= self.options.batch_window;
                let idle_elapsed = state.last_arrival_time.map_or(Duration::ZERO, |t| now.duration_since(t));
                let idle_expired = idle_elapsed >= self.options.idle_commit_threshold;

                if batch_full || timeout_expired || idle_expired || state.force_flush {
                    // CLOSE BATCH: become the Committer
                    state.fsync_in_progress = true;
                    state.collecting_batch_id += 1;
                    state.waiters_in_collecting = 0;
                    state.batch_start_time = None;
                    state.last_arrival_time = None;

                    drop(state);

                    // =========================================================
                    // PHYSICAL FSYNC EXECUTION (OUTSIDE THE LOCK!)
                    // This is the only active fsync in the entire system.
                    // =========================================================
                    let sync_res = perform_sync(file, self.options.sync_mode);

                    // Re-acquire lock to update state
                    state = self.state.lock().unwrap();
                    state.fsync_in_progress = false;
                    state.last_synced_batch_id = my_batch;

                    if let Err(ref e) = sync_res {
                        state.last_error = Some(e.kind());
                    } else {
                        state.last_error = None;
                    }

                    // Wake all threads (both followers of my_batch and the next batch)
                    self.cvar.notify_all();

                    return sync_res;
                } else {
                    // Not yet full and timeouts not expired: wait for the shortest remaining interval
                    let remaining_window = self.options.batch_window.saturating_sub(total_elapsed);
                    let remaining_idle = self.options.idle_commit_threshold.saturating_sub(idle_elapsed);
                    let wait_dur = remaining_idle.min(remaining_window);
                    state = self.cvar.wait_timeout(state, wait_dur).unwrap().0;
                    continue;
                }
            }

            // Otherwise (prior fsync still running or not our turn yet):
            state = self.cvar.wait(state).unwrap();
        }
    }

    /// Returns the configuration options for this GroupCommit coordinator.
    pub fn config(&self) -> GroupCommitConfig {
        self.options
    }

    /// Returns the total number of physical fsync batches completed so far.
    pub fn synced_batches(&self) -> u64 {
        let state = self.state.lock().unwrap();
        state.last_synced_batch_id
    }

    /// Forces an immediate flush of any currently collecting batch without
    /// waiting for batch timeout or batch capacity, and blocks until the batch
    /// has been physically persisted to disk via fsync.
    pub fn force_flush_and_wait(&self, file: &File) -> io::Result<()> {
        {
            let mut state = self.state.lock().unwrap();
            state.force_flush = true;
            self.cvar.notify_all();
        }
        self.sync(file)
    }
}
