use std::collections::HashMap;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::io;
use std::thread;
use std::path::Path;

use super::record::{OpType, Record};
use super::wal::{Wal, WalReader};
use super::priority_queue::PriorityBucketQueue;
use super::payload_cache::{PayloadCache, DEFAULT_PAYLOAD_CACHE_CAPACITY};
pub use super::group_commit::{GroupCommit, GroupCommitConfig, GroupCommitOptions, SyncMode, perform_sync};

pub struct ExpirationWorker {
    pub shutdown_flag: Arc<AtomicBool>,
    pub shutdown_cond: Arc<(Mutex<bool>, Condvar)>,
    pub handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl ExpirationWorker {
    pub fn new() -> Self {
        Self {
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_cond: Arc::new((Mutex::new(false), Condvar::new())),
            handle: Mutex::new(None),
        }
    }

    pub fn stop(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        {
            let (lock, cvar) = &*self.shutdown_cond;
            let mut guard = lock.lock().unwrap();
            *guard = true;
            cvar.notify_all();
        }
        let handle = self.handle.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

impl Default for ExpirationWorker {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ExpirationWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Debug, Clone)]
pub struct TaskRef {
    pub id: u64,
    pub priority: u8,
    pub retries: u8,
}

impl PartialEq for TaskRef {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.priority == other.priority
    }
}
impl Eq for TaskRef {}

impl PartialOrd for TaskRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
            .then_with(|| other.id.cmp(&self.id))
    }
}

#[derive(Debug, Clone)]
pub struct LeaseInfo {
    pub consumer_id: u32,
    pub expire_timestamp: u64,
    pub retries: u8,
    pub priority: u8,
}

const MAX_RETRIES: u8 = 3;

pub struct EngineState {
    pub wal: Wal,
    pub next_id: u64,
    pub index: HashMap<u64, (u64, u64)>,      // ID -> (segment_id, offset)
    pub data_index: HashMap<u64, (u64, u64)>, // ID -> (segment_id, offset)
    pub ready_queue: PriorityBucketQueue,
    pub leased: HashMap<u64, LeaseInfo>,
    pub payload_cache: PayloadCache,
    pub on_task_ready: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[derive(Clone)]
pub struct Engine {
    pub inner: Arc<Mutex<EngineState>>,
    pub reader: Arc<WalReader>,
    pub group_commit: Arc<GroupCommit>,
    pub expiration_worker: Arc<ExpirationWorker>,
    pub compaction_lock: Arc<Mutex<()>>,
}

impl Engine {
    pub fn new(state: EngineState, reader: Arc<WalReader>, group_commit: Arc<GroupCommit>) -> Self {
        let engine = Self {
            inner: Arc::new(Mutex::new(state)),
            reader,
            group_commit,
            expiration_worker: Arc::new(ExpirationWorker::new()),
            compaction_lock: Arc::new(Mutex::new(())),
        };
        engine.start_expiration_thread();
        engine
    }

    pub fn open(dir: impl AsRef<Path>, group_commit_config: Option<GroupCommitConfig>) -> io::Result<Self> {
        let config = group_commit_config.unwrap_or_default();
        Self::open_with_config(dir, config)
    }

    pub fn open_with_config(dir: impl AsRef<Path>, config: GroupCommitConfig) -> io::Result<Self> {
        let dir = dir.as_ref();
        let wal = Wal::open(dir, None)?;
        let reader = Arc::new(WalReader::new(dir.to_path_buf()));
        
        let mut state = EngineState::new(wal);

        state.recover(&reader)?;

        let engine = Self {
            inner: Arc::new(Mutex::new(state)),
            reader,
            group_commit: Arc::new(GroupCommit::new(config)),
            expiration_worker: Arc::new(ExpirationWorker::new()),
            compaction_lock: Arc::new(Mutex::new(())),
        };

        engine.start_expiration_thread();
        Ok(engine)
    }

    pub fn open_with_options(dir: impl AsRef<Path>, options: GroupCommitOptions) -> io::Result<Self> {
        Self::open_with_config(dir, options)
    }

    pub fn set_on_task_ready<F>(&self, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let mut state = self.inner.lock().unwrap();
        state.on_task_ready = Some(Arc::new(cb));
    }

    pub fn push(&self, payload: Vec<u8>, priority: u8) -> io::Result<u64> {
        let (id, cb) = {
            let mut state = self.inner.lock().unwrap();
            let id = state.next_id;
            state.next_id += 1;

            let payload_arc = Arc::new(payload);
            let record = Record::new(id, OpType::Created, priority, (*payload_arc).clone());
            let (seg, off) = state.wal.append(&record)?;

            state.index.insert(id, (seg, off));
            state.data_index.insert(id, (seg, off));
            state.payload_cache.insert(id, payload_arc);
            state.ready_queue.push(TaskRef { id, priority, retries: 0 });

            (id, state.on_task_ready.clone())
        };

        if let Some(cb) = cb {
            cb();
        }

        Ok(id)
    }

    pub fn pop_and_lease(&self, consumer_id: u32, lease_ttl_secs: u64) -> io::Result<Option<(Record, u8)>> {
        let id;
        let priority;
        let retries;
        let data_seg;
        let data_off;
        let cached_payload;
        
        {
            let mut state = self.inner.lock().unwrap();
            
            let task_ref = match state.ready_queue.pop() {
                Some(t) => t,
                None => return Ok(None),
            };

            id = task_ref.id;
            priority = task_ref.priority;
            retries = task_ref.retries;

            let expire_timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() + lease_ttl_secs;

            let lease_info = LeaseInfo {
                consumer_id,
                expire_timestamp,
                retries,
                priority,
            };

            let mut payload = Vec::with_capacity(13);
            payload.extend_from_slice(&consumer_id.to_be_bytes());
            payload.extend_from_slice(&expire_timestamp.to_be_bytes());
            payload.push(retries);

            let leased_record = Record::new(id, OpType::Leased, priority, payload);
            let (seg, off) = state.wal.append(&leased_record)?;
            
            state.index.insert(id, (seg, off));
            state.leased.insert(id, lease_info);

            let &(seg_data, off_data) = state.data_index.get(&id)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Missing data offset"))?;
                
            data_seg = seg_data;
            data_off = off_data;

            // Check in-memory hot-path payload cache
            cached_payload = state.payload_cache.get(id);
        } 

        // If payload was cached in memory, return immediately without touching disk!
        let original_record = if let Some(payload_arc) = cached_payload {
            Record::new(id, OpType::Created, priority, (*payload_arc).clone())
        } else {
            // Read-only I/O executed concurrently outside the lock as fallback
            self.reader.read_at(data_seg, data_off)?
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Record not found at offset"))?
        };

        Ok(Some((original_record, retries)))
    }

    pub fn ack(&self, id: u64, consumer_id: u32) -> io::Result<()> {
        let mut state = self.inner.lock().unwrap();

        if let Some(lease) = state.leased.get(&id) {
            if lease.consumer_id != consumer_id {
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, "Task leased to another consumer"));
            }
            
            let ack_record = Record::new(id, OpType::Acked, lease.priority, vec![]);
            let (seg, off) = state.wal.append(&ack_record)?;
            
            state.index.insert(id, (seg, off));
            state.leased.remove(&id);
            state.data_index.remove(&id);
            state.payload_cache.remove(id);
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Task not found or not leased"))
        }
    }

    pub fn nack(&self, id: u64, consumer_id: u32) -> io::Result<()> {
        let cb = {
            let mut state = self.inner.lock().unwrap();

            let lease = state.leased.get(&id)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Task not found or not leased"))?;

            if lease.consumer_id != consumer_id {
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, "Task leased to another consumer"));
            }

            let re_enqueued = Self::process_task_failure(&mut state, id, lease.priority, lease.retries, OpType::Nacked)?;
            if re_enqueued {
                state.on_task_ready.clone()
            } else {
                None
            }
        };

        if let Some(cb) = cb {
            cb();
        }

        Ok(())
    }

    fn process_task_failure(state: &mut EngineState, id: u64, priority: u8, current_retries: u8, failure_type: OpType) -> io::Result<bool> {
        let new_retries = current_retries + 1;
        let payload = vec![new_retries];
        
        let op = if new_retries >= MAX_RETRIES {
            OpType::DeadLettered
        } else {
            failure_type
        };

        let record = Record::new(id, op, priority, payload);
        let (seg, off) = state.wal.append(&record)?;
        
        state.index.insert(id, (seg, off));
        state.leased.remove(&id);

        if op == OpType::DeadLettered {
            state.data_index.remove(&id);
            state.payload_cache.remove(id);
            Ok(false)
        } else {
            state.ready_queue.push(TaskRef { id, priority, retries: new_retries });
            Ok(true)
        }
    }

    fn start_expiration_thread(&self) {
        let engine = Arc::clone(&self.inner);
        let shutdown_flag = Arc::clone(&self.expiration_worker.shutdown_flag);
        let shutdown_cond = Arc::clone(&self.expiration_worker.shutdown_cond);

        let handle = thread::spawn(move || {
            let (lock, cvar) = &*shutdown_cond;
            while !shutdown_flag.load(Ordering::Relaxed) {
                {
                    let guard = lock.lock().unwrap();
                    if *guard || shutdown_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    let (g, _) = cvar.wait_timeout(guard, Duration::from_secs(1)).unwrap();
                    if *g || shutdown_flag.load(Ordering::Relaxed) {
                        break;
                    }
                }

                let mut expired_tasks = Vec::new();

                {
                    let state = match engine.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => {
                            eprintln!("Warning: Engine Mutex was poisoned, attempting recovery for background expiration.");
                            poisoned.into_inner()
                        }
                    };

                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

                    for (&id, lease) in state.leased.iter() {
                        if now >= lease.expire_timestamp {
                            expired_tasks.push((id, lease.priority, lease.retries));
                        }
                    }
                }

                if !expired_tasks.is_empty() {
                    let mut re_enqueued_count = 0;
                    let cb = {
                        let mut state = engine.lock().unwrap();
                        for (id, priority, retries) in expired_tasks {
                            if let Ok(re_enqueued) = Self::process_task_failure(&mut state, id, priority, retries, OpType::Expired) {
                                if re_enqueued {
                                    re_enqueued_count += 1;
                                }
                            }
                        }
                        state.on_task_ready.clone()
                    };

                    if let Some(cb) = cb {
                        for _ in 0..re_enqueued_count {
                            cb();
                        }
                    }
                }
            }
        });

        *self.expiration_worker.handle.lock().unwrap() = Some(handle);
    }

    pub fn status(&self) -> (usize, usize) {
        let state = self.inner.lock().unwrap();
        (state.ready_queue.len(), state.leased.len())
    }

    pub fn sync(&self) -> io::Result<()> {
        let active_file = {
            let state = self.inner.lock().unwrap();
            state.wal.active_file_arc()
        };

        // =========================================================================
        // CRITICAL POINT 3 (Sequentiality Verification):
        // No thread can join a GroupCommit batch BEFORE its write operation
        // (write_all) has already completed and been confirmed.
        //
        // This guarantee is absolute and structurally verified:
        // 1. Every operation that writes data to the WAL (e.g. `push`, `ack`, `nack`)
        //    acquires `self.inner` (the exclusive lock on `EngineState`) and runs
        //    `state.wal.append(&record)`, which performs a physical `write_all`
        //    to the WAL file descriptor, updating the segment size.
        // 2. Only AFTER `write_all` returns `Ok`, the `EngineState` lock is
        //    released, ensuring data is present in the OS page cache.
        // 3. Only then the thread calls `sync()`, entering this block
        //    and joining the Group Commit coordinator.
        //
        // Result: it is impossible for a thread to participate in an fsync batch
        // for a record that has not already been entirely written to disk at the kernel level.
        // =========================================================================
        self.group_commit.sync(&active_file)
    }

    pub fn compact(&self) -> io::Result<bool> {
        // Serialize concurrent compaction executions without blocking normal engine operations.
        let _compaction_guard = self.compaction_lock.lock().unwrap();

        let (dir, active_seg) = {
            let state = self.inner.lock().unwrap();
            (state.wal.dir().to_path_buf(), state.wal.active_segment_id())
        };

        let closed_segments = match Wal::plan_compaction(&dir, active_seg)? {
            Some(segs) => segs,
            None => return Ok(false),
        };

        // Phase 1: Heavy I/O (full scan of closed segments, deduplication in memory,
        // writing nesso.00001.compacting and sync_data) runs completely WITHOUT holding self.inner lock!
        // Client requests (push, pop, ack, nack, active segment rotation) proceed uninterrupted.
        let (_stats, new_offsets) = Wal::execute_compaction_phase1(&dir, &closed_segments)?;

        // Phase 2: Atomic swap and index remapping under self.inner lock (< 1ms)
        {
            let mut state = self.inner.lock().unwrap();

            // Atomic rename nesso.00001.compacting -> nesso.00001.wal and unlinks of obsolete closed segments
            Wal::execute_compaction_phase2(&dir, &closed_segments)?;

            // Update in-memory indexes only for tasks whose latest state still belongs to one of the compacted segments.
            // If a task was acked, dead-lettered, or modified in the active segment (or newer segments) while
            // Phase 1 was running, it will have either been removed from state.data_index or point to a segment
            // outside closed_segments. In that case, we MUST NOT overwrite or resurrect it.
            for (id, offset) in new_offsets {
                if let Some(&(seg, _)) = state.data_index.get(&id) {
                    if closed_segments.contains(&seg) {
                        state.data_index.insert(id, (1, offset));
                    }
                }
                if let Some(&(seg, _)) = state.index.get(&id) {
                    if closed_segments.contains(&seg) {
                        state.index.insert(id, (1, offset));
                    }
                }
            }

            self.reader.clear_cache();
        }

        Ok(true)
    }

    /// Forces any pending group commit batch to fsync immediately and blocks
    /// until confirmed to disk.
    pub fn force_flush_group_commit(&self) -> io::Result<()> {
        let active_file = {
            let state = self.inner.lock().unwrap();
            state.wal.active_file_arc()
        };
        self.group_commit.force_flush_and_wait(&active_file)
    }

    /// Signals the background expiration worker thread to stop and waits
    /// for it to finish and join.
    pub fn stop_expiration_thread(&self) {
        self.expiration_worker.stop();
    }

    /// Returns the GroupCommitConfig used by this Engine.
    pub fn config(&self) -> GroupCommitConfig {
        self.group_commit.config()
    }

    /// Returns the total number of physical fsync batches completed so far.
    pub fn synced_batches(&self) -> u64 {
        self.group_commit.synced_batches()
    }

    /// Returns the (hits, misses, evictions) statistics of the in-memory payload cache.
    pub fn payload_cache_stats(&self) -> (u64, u64, u64) {
        let state = self.inner.lock().unwrap();
        state.payload_cache.stats()
    }

    /// Complete shutdown of the Engine: forces group commit flush to disk,
    /// then stops and joins the background expiration worker.
    pub fn shutdown(&self) -> io::Result<()> {
        self.force_flush_group_commit()?;
        self.stop_expiration_thread();
        Ok(())
    }
}

impl EngineState {
    pub fn new(wal: Wal) -> Self {
        Self::new_with_cache_capacity(wal, DEFAULT_PAYLOAD_CACHE_CAPACITY)
    }

    pub fn new_with_cache_capacity(wal: Wal, cache_capacity: usize) -> Self {
        Self {
            wal,
            next_id: 1,
            index: HashMap::new(),
            data_index: HashMap::new(),
            ready_queue: PriorityBucketQueue::new(),
            leased: HashMap::new(),
            payload_cache: PayloadCache::new(cache_capacity),
            on_task_ready: None,
        }
    }

    pub fn recover(&mut self, reader: &WalReader) -> io::Result<()> {
        for result in self.wal.iter_all()? {
            let (segment_id, offset, record) = result?;
            
            if record.id() >= self.next_id {
                self.next_id = record.id() + 1;
            }

            self.index.insert(record.id(), (segment_id, offset));
            
            if record.op_type() == OpType::Created {
                self.data_index.insert(record.id(), (segment_id, offset));
            }
        }

        let mut sorted_ids: Vec<u64> = self.index.keys().copied().collect();
        sorted_ids.sort_unstable();

        for id in sorted_ids {
            let &(segment, offset) = match self.index.get(&id) {
                Some(loc) => loc,
                None => continue,
            };
            if let Some(record) = reader.read_at(segment, offset)? {
                match record.op_type() {
                    OpType::Created | OpType::Nacked | OpType::Expired => {
                        let retries = if record.op_type() == OpType::Created {
                            0
                        } else {
                            record.payload().first().copied().unwrap_or(0)
                        };
                        self.ready_queue.push(TaskRef { id, priority: record.priority(), retries });
                    }
                    OpType::Leased => {
                        let payload = record.payload();
                        if payload.len() >= 13 {
                            let consumer_id = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                            let expire_timestamp = u64::from_be_bytes(payload[4..12].try_into().unwrap());
                            let retries = payload[12];
                            
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                            if expire_timestamp > now {
                                self.leased.insert(id, LeaseInfo {
                                    consumer_id,
                                    expire_timestamp,
                                    retries,
                                    priority: record.priority(),
                                });
                            } else {
                                self.ready_queue.push(TaskRef { id, priority: record.priority(), retries });
                            }
                        } else {
                            self.ready_queue.push(TaskRef { id, priority: record.priority(), retries: 0 });
                        }
                    }
                    OpType::Acked | OpType::DeadLettered => {
                        self.data_index.remove(&id);
                        self.index.remove(&id);
                    }
                }
            }
        }
        Ok(())
    }
}
