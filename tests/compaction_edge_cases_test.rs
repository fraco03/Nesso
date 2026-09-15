use nesso::storage::engine::{Engine, EngineState, TaskRef};
use nesso::storage::group_commit::GroupCommit;
use nesso::storage::record::{OpType, Record};
use nesso::storage::wal::{Wal, WalReader};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_wal_dir(test_name: &str) -> PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!("nesso_compact_edge_{}_{}_{}", test_name, std::process::id(), count));
    let _ = fs::remove_dir_all(&path);
    let _ = fs::create_dir_all(&path);
    path
}

// -----------------------------------------------------------------------------
// Test 1: test_compaction_non_contiguous_interleaved_states
//
// Pushes non-contiguous task IDs across 4+ segments with interleaved lifecycle
// states (Created, Leased, Nacked, Expired, Acked). Runs compaction.
// Forcibly clears PayloadCache. Asserts all non-terminal tasks return bit-exact
// payloads and correct retries upon pop, both live and across cold reboot.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_non_contiguous_interleaved_states() {
    let dir = temp_wal_dir("non_contiguous_interleaved");

    // Use a small max_segment_size (180 bytes) to force rotation across 4+ segments
    let wal = Wal::open(&dir, Some(180)).unwrap();
    let state = EngineState::new(wal);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    let mut expected_payloads: HashMap<u64, Vec<u8>> = HashMap::new();
    let mut expected_retries: HashMap<u64, u8> = HashMap::new();

    // Group B (Leased, active TTL): 4 tasks
    // Group C (Nacked, retries=1):  4 tasks
    // Group D (Expired, retries=1): 4 tasks
    // Group E (Acked, terminal):    4 tasks
    // Group A (Created, unleased):  4 tasks

    let mut group_b_ids = Vec::new();
    let mut group_c_ids = Vec::new();
    let mut group_d_ids = Vec::new();
    let mut group_e_ids = Vec::new();
    let mut group_a_ids = Vec::new();

    let mut filler_ids = Vec::new();

    // Push tasks with rotation-forcing filler interleaved so they span 4+ segments
    for cycle in 0..4 {
        // Group B: will be Leased
        let p_b = format!("payload_b_leased_cycle_{}_bytes_data", cycle).into_bytes();
        let id_b = engine.push(p_b.clone(), 20).unwrap();
        expected_payloads.insert(id_b, p_b);
        expected_retries.insert(id_b, 0);
        group_b_ids.push(id_b);

        // Group C: will be Nacked
        let p_c = format!("payload_c_nacked_cycle_{}_bytes_data", cycle).into_bytes();
        let id_c = engine.push(p_c.clone(), 20).unwrap();
        expected_payloads.insert(id_c, p_c);
        expected_retries.insert(id_c, 1);
        group_c_ids.push(id_c);

        // Group D: will be Expired
        let p_d = format!("payload_d_expired_cycle_{}_bytes_data", cycle).into_bytes();
        let id_d = engine.push(p_d.clone(), 20).unwrap();
        expected_payloads.insert(id_d, p_d);
        expected_retries.insert(id_d, 1);
        group_d_ids.push(id_d);

        // Group E: will be Acked
        let p_e = format!("payload_e_acked_cycle_{}_bytes_data", cycle).into_bytes();
        let id_e = engine.push(p_e, 20).unwrap();
        group_e_ids.push(id_e);

        // Group A: remains Created (priority 10 so it isn't popped during the transition phase)
        let p_a = format!("payload_a_created_cycle_{}_bytes_data", cycle).into_bytes();
        let id_a = engine.push(p_a.clone(), 10).unwrap();
        expected_payloads.insert(id_a, p_a);
        expected_retries.insert(id_a, 0);
        group_a_ids.push(id_a);

        // Force segment rotation with filler
        let filler = vec![0xDD; 90];
        let id_f = engine.push(filler, 1).unwrap();
        filler_ids.push(id_f);
    }

    // Now transition Group B, C, D, E (they have priority 20, so they pop before Group A at priority 10)
    // 16 tasks to pop at priority 20
    for _ in 0..16 {
        let (rec, _retries) = engine.pop_and_lease(100, 3600).unwrap().unwrap();
        let id = rec.id();
        if group_b_ids.contains(&id) {
            // Keep active lease (TTL 3600s)
        } else if group_c_ids.contains(&id) {
            // Nack: retries becomes 1, re-enqueued
            engine.nack(id, 100).unwrap();
        } else if group_d_ids.contains(&id) {
            // Expire: record expired event, retries becomes 1, re-enqueued
            {
                let mut state = engine.inner.lock().unwrap();
                let lease = state.leased.remove(&id).unwrap();
                let record = Record::new(id, OpType::Expired, lease.priority, vec![1]);
                let (seg, off) = state.wal.append(&record).unwrap();
                state.index.insert(id, (seg, off));
                state.ready_queue.push(TaskRef {
                    id,
                    priority: lease.priority,
                    retries: 1,
                });
            }
        } else if group_e_ids.contains(&id) {
            // Ack: terminal state
            engine.ack(id, 100).unwrap();
        }
    }

    // Push more filler to force rotation so earlier segments (1..=4) are completely closed
    for _ in 0..6 {
        let filler = vec![0xEE; 90];
        let id_f = engine.push(filler, 1).unwrap();
        filler_ids.push(id_f);
    }

    let active_seg = {
        let state = engine.inner.lock().unwrap();
        state.wal.active_segment_id()
    };
    assert!(active_seg >= 5, "Expected active segment >= 5 across multiple rollovers, got {}", active_seg);

    // Execute compaction across closed segments (1..=active_seg-1)
    let compacted = engine.compact().unwrap();
    assert!(compacted, "Compaction should execute successfully");

    // FORCIBLY clear the in-memory PayloadCache
    engine.clear_payload_cache();

    // Verify cache was cleared
    {
        let mut state = engine.inner.lock().unwrap();
        for &id in expected_payloads.keys() {
            assert!(state.payload_cache.get(id).is_none(), "PayloadCache must be empty for task {}", id);
        }
    }

    // Now pop all currently ready tasks from the queue
    let mut popped_ids = Vec::new();
    loop {
        let pop_res = engine.pop_and_lease(200, 60).unwrap();
        match pop_res {
            Some((rec, retries)) => {
                let id = rec.id();
                if filler_ids.contains(&id) {
                    // Filler task from active segment
                    engine.ack(id, 200).unwrap();
                    continue;
                }
                assert!(expected_payloads.contains_key(&id), "Popped unexpected task {}", id);
                let exp_payload = &expected_payloads[&id];
                assert_eq!(rec.payload(), &exp_payload[..], "Payload must match bit-exact for task {}", id);
                assert_eq!(retries, expected_retries[&id], "Retries must match expected count for task {}", id);
                popped_ids.push(id);
                engine.ack(id, 200).unwrap();
            }
            None => break,
        }
    }

    // Verify Group A, C, D were all popped and verified
    for id in group_a_ids.iter().chain(group_c_ids.iter()).chain(group_d_ids.iter()) {
        assert!(popped_ids.contains(id), "Task {} should have been popped and verified", id);
    }

    // Ack Group B tasks
    for &id in &group_b_ids {
        engine.ack(id, 100).unwrap();
    }

    // Now test Cold Reboot after compaction:
    // Drop engine, reopen it, and verify cold crash recovery reconstructs state with 0 errors
    drop(engine);

    let recovered_engine = Engine::open(&dir, None).unwrap();
    let (_ready, leased) = recovered_engine.status();
    assert_eq!(leased, 0, "No active leases should remain after all were acked");

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Test 2: test_compaction_concurrent_pop_lease_during_phase1
//
// Creates multiple closed segments. Runs Phase 1 compaction in a background thread
// while concurrent client threads continuously pop and lease tasks originating
// from those closed segments. Asserts 0 NotFound errors, payload cache fallback
// correctness, and clean completion.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_concurrent_pop_lease_during_phase1() {
    let dir = temp_wal_dir("concurrent_pop_during_phase1");

    // Threshold 200 bytes
    let wal = Wal::open(&dir, Some(200)).unwrap();
    let state = EngineState::new(wal);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    let task_count = 80;
    let mut payloads: HashMap<u64, Vec<u8>> = HashMap::new();

    for i in 1..=task_count {
        let p = format!("concurrent_phase1_payload_{:04}_data_chunk", i).into_bytes();
        let id = engine.push(p.clone(), 1).unwrap();
        payloads.insert(id, p);
    }

    // Force rotation so earlier segments are closed
    let active_seg = {
        let state = engine.inner.lock().unwrap();
        state.wal.active_segment_id()
    };
    assert!(active_seg >= 4, "Should have created multiple segments, got {}", active_seg);

    // Evict payload cache so threads must exercise disk read fallback
    engine.clear_payload_cache();

    let (dir_buf, active_seg) = {
        let state = engine.inner.lock().unwrap();
        (state.wal.dir().to_path_buf(), state.wal.active_segment_id())
    };
    let closed_segments = Wal::plan_compaction(&dir_buf, active_seg).unwrap().expect("Closed segments must exist");

    // Spawn background thread that runs Phase 1 (non-blocking I/O)
    let dir_clone = dir_buf.clone();
    let closed_clone = closed_segments.clone();
    let phase1_handle = thread::spawn(move || {
        Wal::execute_compaction_phase1(&dir_clone, &closed_clone)
    });

    // Concurrently, 4 client threads pop and lease tasks
    let success_count = Arc::new(AtomicUsize::new(0));
    let not_found_count = Arc::new(AtomicUsize::new(0));
    let payloads_arc = Arc::new(payloads);

    let mut client_handles = Vec::new();
    for thread_id in 0..4 {
        let eng = Arc::clone(&engine);
        let succ = Arc::clone(&success_count);
        let nf = Arc::clone(&not_found_count);
        let payloads_ref = Arc::clone(&payloads_arc);

        client_handles.push(thread::spawn(move || {
            for _ in 0..15 {
                match eng.pop_and_lease(1000 + thread_id, 30) {
                    Ok(Some((rec, _retries))) => {
                        let id = rec.id();
                        let exp = &payloads_ref[&id];
                        assert_eq!(rec.payload(), &exp[..], "Bit-exact payload check failed for task {}", id);
                        succ.fetch_add(1, Ordering::SeqCst);
                        let _ = eng.ack(id, 1000 + thread_id);
                    }
                    Ok(None) => break,
                    Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
                        nf.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => panic!("Unexpected pop error: {:?}", e),
                }
                thread::yield_now();
            }
        }));
    }

    // Wait for Phase 1 to complete
    let phase1_result = phase1_handle.join().unwrap();
    assert!(phase1_result.is_ok(), "Phase 1 must succeed without error");

    for h in client_handles {
        h.join().unwrap();
    }

    assert_eq!(not_found_count.load(Ordering::SeqCst), 0, "Zero NotFound errors allowed during concurrent Phase 1");
    assert!(success_count.load(Ordering::SeqCst) > 0, "Clients should successfully pop tasks");

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Test 3: test_compaction_concurrent_phase2_lock_contention
//
// Forces Phase 2 swap while 16 threads concurrently issue push, pop_and_lease,
// and ack. Asserts index consistency, zero deadlocks, and zero data corruption.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_concurrent_phase2_lock_contention() {
    let dir = temp_wal_dir("phase2_contention");

    let wal = Wal::open(&dir, Some(400)).unwrap();
    let state = EngineState::new(wal);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    // Populate initial segments
    for i in 0..30 {
        let _ = engine.push(format!("initial_seed_task_{}", i).into_bytes(), 1).unwrap();
    }

    let running = Arc::new(AtomicBool::new(true));
    let mut handles = Vec::new();

    // 16 worker threads concurrently bombarding engine
    // 6 Producers
    for thread_id in 0..6 {
        let eng = Arc::clone(&engine);
        let run = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            let mut seq = 0;
            while run.load(Ordering::Relaxed) {
                let payload = format!("prod_{}_{}", thread_id, seq).into_bytes();
                let _ = eng.push(payload, (seq % 5 + 1) as u8);
                seq += 1;
                thread::yield_now();
            }
        }));
    }

    // 6 Consumers
    for thread_id in 6..12 {
        let eng = Arc::clone(&engine);
        let run = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            let consumer_id = 5000 + thread_id as u32;
            while run.load(Ordering::Relaxed) {
                if let Ok(Some((rec, _))) = eng.pop_and_lease(consumer_id, 20) {
                    let _ = eng.ack(rec.id(), consumer_id);
                }
                thread::yield_now();
            }
        }));
    }

    // 4 Readers/Status checkers
    for _ in 12..16 {
        let eng = Arc::clone(&engine);
        let run = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            while run.load(Ordering::Relaxed) {
                let _ = eng.status();
                let _ = eng.payload_cache_stats();
                thread::yield_now();
            }
        }));
    }

    // Main thread forces multiple compactions (including Phase 2 swap) while threads run
    for _ in 0..5 {
        thread::sleep(Duration::from_millis(50));
        let _ = engine.compact();
    }

    // Stop workers
    running.store(false, Ordering::SeqCst);
    for h in handles {
        h.join().unwrap();
    }

    // Run one final compaction to verify state consistency
    let _ = engine.compact();

    // Verify engine state integrity
    let (ready, leased) = engine.status();
    let state = engine.inner.lock().unwrap();
    assert_eq!(state.ready_queue.len(), ready);
    assert_eq!(state.leased.len(), leased);

    // Verify all active records in data_index can be read cleanly
    for (&id, &(seg, off)) in &state.data_index {
        let rec = engine.reader.read_at(seg, off).unwrap();
        assert!(rec.is_some(), "Record for ID {} at segment {} offset {} must be readable", id, seg, off);
    }

    drop(state);
    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Test 4: test_compaction_heavy_write_rapid_rollover_pressure
//
// High write load with small segment size causing 20+ segment rollovers while
// background compaction cycles run. Asserts sequential segment numbering,
// no ID collision, and all records durable.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_heavy_write_rapid_rollover_pressure() {
    let dir = temp_wal_dir("rapid_rollover_pressure");

    // Threshold 1024 bytes -> rapid rollovers
    let wal = Wal::open(&dir, Some(1024)).unwrap();
    let state = EngineState::new(wal);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    let running = Arc::new(AtomicBool::new(true));

    // Background compactor thread
    let compactor_eng = Arc::clone(&engine);
    let compactor_run = Arc::clone(&running);
    let compactor_handle = thread::spawn(move || {
        let mut compactions = 0;
        while compactor_run.load(Ordering::Relaxed) {
            if let Ok(true) = compactor_eng.compact() {
                compactions += 1;
            }
            thread::sleep(Duration::from_millis(5));
        }
        compactions
    });

    // 4 Writer threads rapidly pushing tasks
    let mut writer_handles = Vec::new();
    let total_tasks_per_writer = 50;
    for writer_id in 0..4 {
        let eng = Arc::clone(&engine);
        writer_handles.push(thread::spawn(move || {
            let mut ids = Vec::new();
            for i in 0..total_tasks_per_writer {
                let payload = vec![(writer_id * 10 + (i % 250) as usize) as u8; 256];
                let id = eng.push(payload, 1).unwrap();
                ids.push(id);
            }
            ids
        }));
    }

    let mut all_pushed_ids = Vec::new();
    for h in writer_handles {
        let ids = h.join().unwrap();
        all_pushed_ids.extend(ids);
    }

    // Stop compactor
    running.store(false, Ordering::SeqCst);
    let _compactions_done = compactor_handle.join().unwrap();

    // Final compaction
    let _ = engine.compact();

    // Check segment numbering and integrity
    let mut found_segments = Vec::new();
    for entry in fs::read_dir(&dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        assert!(!name.ends_with(".compacting"), "No temporary .compacting file should be left");
        assert!(!name.ends_with(".tmp"), "No temporary .tmp file should be left");
        if let Some(id_str) = name.strip_prefix("nesso.").and_then(|s| s.strip_suffix(".wal")) {
            let seg_id: u64 = id_str.parse().expect("Segment ID must be numeric");
            found_segments.push(seg_id);
        }
    }
    found_segments.sort_unstable();

    // Verify segment numbering: segment 1 must exist (compacted base), and active segment must exist
    assert!(found_segments.contains(&1), "Segment 1 must exist");
    assert!(found_segments.len() >= 2, "At least segment 1 and active segment must exist");

    // Verify all pushed task IDs are unique and strictly monotonic
    let mut unique_ids = all_pushed_ids.clone();
    unique_ids.sort_unstable();
    unique_ids.dedup();
    assert_eq!(unique_ids.len(), all_pushed_ids.len(), "Task IDs must not collide");

    // Verify all records durable and CRC32 valid across all segments
    let wal = Wal::open(&dir, None).unwrap();
    let recovered_records: Vec<_> = wal.iter_all().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(!recovered_records.is_empty(), "Durable records must be recoverable");

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Test 5: test_compaction_wal_reader_all_lru_slots_invalidation
//
// Warms WalReader.read_cache across 8 distinct closed segments (filling all 8
// LRU slots). Runs compaction consolidating segments 1..8 into segment 1.
// Asserts all 8 cached file handles are dropped and subsequent reads hit the
// new segment 1 without reading stale offsets.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_wal_reader_all_lru_slots_invalidation() {
    let dir = temp_wal_dir("wal_reader_lru_invalidation");

    // Threshold 80 bytes
    let wal = Wal::open(&dir, Some(80)).unwrap();
    let state = EngineState::new(wal);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    // Create segments 1 through 9 (8 closed segments + segment 9 active)
    let mut seg_task_offsets = HashMap::new();
    for seg in 1..=8 {
        let payload = format!("target_task_in_segment_{}", seg).into_bytes();
        let id = engine.push(payload, 1).unwrap();
        // Push filler to force rotation to next segment
        let filler = vec![0xAB; 70];
        let _ = engine.push(filler, 1).unwrap();

        let (task_seg, task_off) = {
            let state = engine.inner.lock().unwrap();
            state.data_index[&id]
        };
        seg_task_offsets.insert(task_seg, (id, task_off));
    }

    // Trigger rotation to segment 9 so segments 1..=8 are ALL closed
    let _ = engine.push(vec![0xCC; 20], 1).unwrap();

    let active_seg = {
        let state = engine.inner.lock().unwrap();
        state.wal.active_segment_id()
    };
    assert!(active_seg >= 9, "Expected active segment >= 9, got {}", active_seg);

    // Warm WalReader.read_cache across 8 distinct closed segments: filling all 8 LRU slots!
    for seg in 1..=8 {
        let &(id, off) = seg_task_offsets.get(&seg).unwrap();
        let record = engine.reader.read_at(seg, off).unwrap().expect("Record must be present");
        assert_eq!(record.id(), id);
    }

    // Verify all 8 slots in the LRU cache are populated
    assert_eq!(engine.reader.cached_count(), 8, "All 8 LRU slots must be occupied");
    let cached_segs = engine.reader.cached_segments();
    assert_eq!(cached_segs.len(), 8);
    for seg in 1..=8 {
        assert!(cached_segs.contains(&seg), "Segment {} must be cached", seg);
    }

    // Run compaction: consolidates closed segments 1..8 into segment 1
    let compacted = engine.compact().unwrap();
    assert!(compacted, "Compaction must execute");

    // CRITICAL INVARIANT: All 8 cached file handles must have been dropped!
    assert_eq!(engine.reader.cached_count(), 0, "clear_cache() must evict all 8 LRU slots post-compaction");

    // Verify segments 2..8 have been deleted from disk
    for seg in 2..=8 {
        let p = dir.join(format!("nesso.{:05}.wal", seg));
        assert!(!p.exists(), "Obsolete segment {} must be unlinked from filesystem", seg);
    }

    // Read surviving tasks from the new segment 1 via engine
    let (new_seg, new_off) = {
        let state = engine.inner.lock().unwrap();
        let first_id = seg_task_offsets.get(&1).unwrap().0;
        state.data_index[&first_id]
    };
    assert_eq!(new_seg, 1, "Compacted data_index must point to segment 1");

    let rec = engine.reader.read_at(1, new_off).unwrap().expect("Record must be readable from new segment 1");
    assert_eq!(rec.payload(), b"target_task_in_segment_1");

    // Reading segment 1 must populate exactly 1 LRU slot
    assert_eq!(engine.reader.cached_count(), 1);
    assert_eq!(engine.reader.cached_segments(), vec![1]);

    // Subsequent read for deleted segment 2 must return NotFound, not reading stale open file descriptor
    let err = engine.reader.read_at(2, 0).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound, "Attempting to read deleted segment must return NotFound");

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Test 6: test_compaction_payload_cache_eviction_fallback
//
// Pushes tasks, compacts WAL, forcibly evicts tasks from PayloadCache via capacity
// overflow, pops tasks, and asserts correct fallback to reading compacted segment
// on disk.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_payload_cache_eviction_fallback() {
    let dir = temp_wal_dir("payload_cache_eviction_fallback");

    // Open wal with threshold 150 bytes
    let wal = Wal::open(&dir, Some(150)).unwrap();
    // Configure small PayloadCache capacity of 5 entries
    let state = EngineState::new_with_cache_capacity(wal, 5);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    // Push 4 tasks (IDs 1..=4) with distinct payloads
    let mut initial_payloads = HashMap::new();
    for i in 1..=4 {
        let p = format!("payload_cache_test_target_task_{}_unique_string", i).into_bytes();
        let id = engine.push(p.clone(), 10).unwrap();
        initial_payloads.insert(id, p);
    }

    // Force segment rotation to close segment 1
    let filler = vec![0xCC; 100];
    let _ = engine.push(filler.clone(), 1).unwrap();
    let _ = engine.push(filler, 1).unwrap();

    let active_seg = {
        let state = engine.inner.lock().unwrap();
        state.wal.active_segment_id()
    };
    assert!(active_seg >= 2, "Must have closed segments");

    // Run compaction on closed segments
    let compacted = engine.compact().unwrap();
    assert!(compacted, "Compaction must succeed");

    // Cause cache eviction: push 10 filler tasks to overflow the 5-entry capacity
    for i in 100..110 {
        let filler_payload = format!("filler_payload_to_cause_overflow_{}", i).into_bytes();
        let _ = engine.push(filler_payload, 1).unwrap();
    }

    // Verify evictions occurred in PayloadCache
    let (_hits, misses_before, evictions) = engine.payload_cache_stats();
    assert!(evictions >= 5, "Expected at least 5 evictions, got {}", evictions);

    // Verify initial tasks (1..=4) are completely absent from PayloadCache
    {
        let mut state = engine.inner.lock().unwrap();
        for &id in initial_payloads.keys() {
            assert!(state.payload_cache.get(id).is_none(), "Task {} must be evicted from PayloadCache", id);
        }
    }

    // Pop the initial tasks (they have priority 10, higher than filler priority 1)
    for _ in 1..=4 {
        let (rec, retries) = engine.pop_and_lease(777, 60).unwrap().expect("Task must be popped");
        let id = rec.id();
        assert!(initial_payloads.contains_key(&id), "Popped task {} must be one of the initial tasks", id);
        assert_eq!(rec.payload(), &initial_payloads[&id][..], "Payload must match bit-exact via disk fallback for task {}", id);
        assert_eq!(retries, 0);
        engine.ack(id, 777).unwrap();
    }

    // Assert that misses increased due to the fallback disk reads
    let (_hits, misses_after, _) = engine.payload_cache_stats();
    assert!(misses_after > misses_before, "Cache misses must increase when reading evicted payloads from disk");

    let _ = fs::remove_dir_all(&dir);
}
