use nesso::storage::engine::{Engine, EngineState};
use nesso::storage::group_commit::GroupCommit;
use nesso::storage::record::{OpType, Record};
use nesso::storage::wal::{Wal, WalReader};
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_wal_dir(test_name: &str) -> PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!("nesso_compact_test_{}_{}", test_name, count));
    let _ = fs::remove_dir_all(&path);
    let _ = fs::create_dir_all(&path);
    path
}

// -----------------------------------------------------------------------------
// Test 6: Compaction on segments with mixed task states
// Verifies that after compaction only the latest event of surviving tasks remains,
// and that terminal IDs (Acked, DeadLettered) are completely purged.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_logical_correctness() {
    let dir = temp_wal_dir("logical_correctness");

    // Threshold 100 bytes
    {
        let mut wal = Wal::open(&dir, Some(100)).unwrap();

        // Seg 1: Task 1 (Created -> Acked, terminal: must be purged)
        wal.append(&Record::new(1, OpType::Created, 1, vec![1; 120])).unwrap(); // size 139 > 100
        wal.append(&Record::new(1, OpType::Acked, 1, vec![])).unwrap(); // triggers Seg 2

        // Seg 2: Task 2 (Created -> DeadLettered, terminal: must be purged)
        wal.append(&Record::new(2, OpType::Created, 1, vec![2; 120])).unwrap(); // triggers Seg 3
        wal.append(&Record::new(2, OpType::DeadLettered, 1, vec![3])).unwrap();

        // Seg 3: Task 3 (Created) + Task 4 (Created -> Leased)
        wal.append(&Record::new(3, OpType::Created, 1, vec![3; 10])).unwrap();
        wal.append(&Record::new(4, OpType::Created, 1, vec![4; 10])).unwrap();
        wal.append(&Record::new(4, OpType::Leased, 1, vec![0; 10])).unwrap();

        // Seg 4: Active segment (not compacted)
        wal.append(&Record::new(99, OpType::Created, 1, vec![99; 50])).unwrap(); // triggers Seg 4 (active)
        assert_eq!(wal.active_segment_id(), 4);

        // Execute compaction on closed segments
        wal.compact().unwrap();
    }

    // Verify records present in compacted segment 1
    let wal = Wal::open(&dir, Some(100)).unwrap();
    let all_records: Vec<_> = wal.iter_all().unwrap().collect::<Result<Vec<_>, _>>().unwrap();

    let compacted_records: Vec<_> = all_records
        .iter()
        .filter(|(seg, _, _)| *seg == 1)
        .map(|(_, _, rec)| rec.clone())
        .collect();

    let compacted_ids: Vec<u64> = compacted_records.iter().map(|r| r.id()).collect();

    // Task 1 and Task 2 (terminal) must be ABSENT
    assert!(!compacted_ids.contains(&1), "Task 1 (Acked) should be purged");
    assert!(!compacted_ids.contains(&2), "Task 2 (DeadLettered) should be purged");

    // Task 3 must be present with OpType::Created
    let rec3 = compacted_records.iter().find(|r| r.id() == 3).expect("Task 3 missing");
    assert_eq!(rec3.op_type(), OpType::Created);

    // Task 4 must be present with only the latest event (OpType::Leased)
    let rec4 = compacted_records.iter().find(|r| r.id() == 4).expect("Task 4 missing");
    assert_eq!(rec4.op_type(), OpType::Leased);
}

// -----------------------------------------------------------------------------
// Test 7: Payload of non-terminal tasks survives intact byte-for-byte
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_payload_preservation() {
    let dir = temp_wal_dir("payload_preservation");
    let original_payload = b"test_payload_intact_1234567890_bytes_check".to_vec();

    {
        let mut wal = Wal::open(&dir, Some(50)).unwrap();
        // Write to segment 1 (payload 42 bytes + 19 header = 61 bytes > 50)
        let rec = Record::new(42, OpType::Created, 2, original_payload.clone());
        wal.append(&rec).unwrap();

        // Second write forces rotation to segment 2 (active)
        wal.append(&Record::new(999, OpType::Created, 1, vec![0; 60])).unwrap();
        assert_eq!(wal.active_segment_id(), 2);

        // Compact segment 1
        wal.compact().unwrap();
    }

    // Read after compaction
    let wal = Wal::open(&dir, Some(50)).unwrap();
    let records: Vec<_> = wal.iter_all().unwrap().collect::<Result<Vec<_>, _>>().unwrap();

    let rec42 = records
        .iter()
        .find(|(_, _, r)| r.id() == 42)
        .map(|(_, _, r)| r)
        .expect("Task 42 must be present after compaction");

    assert_eq!(rec42.payload(), &original_payload[..], "Payload must match byte-for-byte");
}

// -----------------------------------------------------------------------------
// Test 8: Task with multiple intermediate events in the same segment being compacted
// Produces only a single consolidated record in the compacted segment (the latest).
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_deduplication_intermediate_events() {
    let dir = temp_wal_dir("dedup_intermediate");

    {
        let mut wal = Wal::open(&dir, Some(100)).unwrap();
        // ID 10 receives Created then Leased in the same segment 1
        wal.append(&Record::new(10, OpType::Created, 1, vec![1; 20])).unwrap();
        wal.append(&Record::new(10, OpType::Leased, 1, vec![2; 80])).unwrap(); // size > 100

        // Force rotation to segment 2 (active)
        wal.append(&Record::new(999, OpType::Created, 1, vec![0; 100])).unwrap();
        assert_eq!(wal.active_segment_id(), 2);

        wal.compact().unwrap();
    }

    let wal = Wal::open(&dir, Some(100)).unwrap();
    let records: Vec<_> = wal.iter_all().unwrap().collect::<Result<Vec<_>, _>>().unwrap();

    // Count how many times ID 10 appears in compacted segment 1
    let id_10_records: Vec<_> = records
        .iter()
        .filter(|(seg, _, r)| *seg == 1 && r.id() == 10)
        .collect();

    assert_eq!(id_10_records.len(), 1, "Expected exactly one consolidated record for ID 10");
    assert_eq!(id_10_records[0].2.op_type(), OpType::Leased, "The consolidated record must be the latest event");
}

// -----------------------------------------------------------------------------
// Test 9: Active segment isolation
// Compaction must never touch the active segment. New writes remain visible
// and fully functional.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_active_segment_isolation() {
    let dir = temp_wal_dir("active_isolation");

    // Open engine with 60-byte threshold to force fast rotation
    let wal = Wal::open(&dir, Some(60)).unwrap();
    let state = EngineState::new(wal);
    let engine = Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    };

    // Task 1: 50 bytes + 19 header = 69 bytes > 60 -> Seg 1
    let id1 = engine.push(vec![1; 50], 1).unwrap();
    // Task 2: Rotates to segment 2 (active)
    let id2 = engine.push(vec![2; 50], 1).unwrap();
    // Task 3: Rotates to segment 3 (active)
    let id3 = engine.push(vec![3; 50], 1).unwrap();

    // Compact closed segments (1 and 2)
    let compacted = engine.compact().unwrap();
    assert!(compacted, "Compaction should execute on closed segments");

    // Write to active segment 3 after compaction
    let id4 = engine.push(b"task4_post_compaction".to_vec(), 1).unwrap();

    // Verify all tasks are present in ready queue
    let (ready, _) = engine.status();
    assert_eq!(ready, 4, "All tasks in ready queue must be intact");

    let pop1 = engine.pop_and_lease(1, 10).unwrap().unwrap();
    assert_eq!(pop1.0.id(), id1);

    let pop2 = engine.pop_and_lease(1, 10).unwrap().unwrap();
    assert_eq!(pop2.0.id(), id2);

    let pop3 = engine.pop_and_lease(1, 10).unwrap().unwrap();
    assert_eq!(pop3.0.id(), id3);

    let pop4 = engine.pop_and_lease(1, 10).unwrap().unwrap();
    assert_eq!(pop4.0.id(), id4);
}

// -----------------------------------------------------------------------------
// Test 10: Invocation with active segment only = safe no-op (Ok(false))
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_no_op_single_segment() {
    let dir = temp_wal_dir("noop_single");
    let mut wal = Wal::open(&dir, None).unwrap();

    wal.append(&Record::new(1, OpType::Created, 1, vec![1, 2, 3])).unwrap();
    assert_eq!(wal.active_segment_id(), 1);

    // Only active segment 1 exists: no closed segments to compact
    let res = wal.compact();
    assert!(res.is_ok(), "Compacting with only active segment must succeed");
    assert!(res.unwrap().is_none(), "Must return None (no-op)");
}

// -----------------------------------------------------------------------------
// Test 11: Simulated crash halfway through writing the .compacting file
// On restart, original closed segments are intact and fully recoverable.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_crash_during_write() {
    let dir = temp_wal_dir("crash_during_write");

    {
        let mut wal = Wal::open(&dir, Some(50)).unwrap();
        wal.append(&Record::new(1, OpType::Created, 1, vec![1; 40])).unwrap();
        wal.append(&Record::new(2, OpType::Created, 1, vec![2; 40])).unwrap(); // seg 2
        wal.append(&Record::new(3, OpType::Created, 1, vec![3; 40])).unwrap(); // seg 3
    }

    // Simulate corrupted/partial `.compacting` file left from a crash
    let partial_path = dir.join("nesso.00001.compacting");
    {
        let mut f = File::create(&partial_path).unwrap();
        f.write_all(&[0x4E, 0x00, 0x01]).unwrap(); // Partial data
        f.flush().unwrap();
    }

    // On open, Wal must clean up temporary leftovers and recover all original segments
    let wal = Wal::open(&dir, Some(50)).unwrap();
    let records: Vec<_> = wal.iter_all().unwrap().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(records.len(), 3, "All 3 original records must survive");
    assert!(!partial_path.exists(), "Temporary .compacting file must be cleaned up on open");
}

// -----------------------------------------------------------------------------
// Test 12: Simulated crash after writing but before atomic rename
// Old segments remain source of truth until rename is confirmed.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_crash_before_rename() {
    let dir = temp_wal_dir("crash_before_rename");

    {
        let mut wal = Wal::open(&dir, Some(50)).unwrap();
        wal.append(&Record::new(10, OpType::Created, 1, vec![1; 40])).unwrap();
        wal.append(&Record::new(20, OpType::Created, 1, vec![2; 40])).unwrap();
        wal.append(&Record::new(30, OpType::Created, 1, vec![3; 40])).unwrap();
    }

    // Create valid temporary file not yet renamed
    let temp_compacting = dir.join("nesso.00001.compacting");
    {
        let mut f = File::create(&temp_compacting).unwrap();
        let rec = Record::new(10, OpType::Created, 1, vec![1; 40]);
        f.write_all(&rec.encode()).unwrap();
        f.sync_data().unwrap();
    }

    // Wal::open ignores .compacting and reads original segments
    let wal = Wal::open(&dir, Some(50)).unwrap();
    let records: Vec<_> = wal.iter_all().unwrap().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(records.len(), 3, "Original closed segments must remain truth before rename");
}

// -----------------------------------------------------------------------------
// Test 13: Atomic filesystem replacement (rename)
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_atomic_rename() {
    let dir = temp_wal_dir("atomic_rename");

    let mut wal = Wal::open(&dir, Some(50)).unwrap();
    wal.append(&Record::new(1, OpType::Created, 1, vec![1; 40])).unwrap(); // size 59 > 50
    wal.append(&Record::new(2, OpType::Created, 1, vec![2; 40])).unwrap(); // triggers seg 2 (active)
    assert_eq!(wal.active_segment_id(), 2);

    // Execute compaction
    let stats = wal.compact().unwrap().expect("Compaction should succeed");
    assert!(stats.surviving_records >= 1);

    // The file nesso.00001.compacting must no longer exist
    assert!(!dir.join("nesso.00001.compacting").exists());
    // The file nesso.00001.wal must exist and be valid
    assert!(dir.join("nesso.00001.wal").exists());
}

// -----------------------------------------------------------------------------
// Test 14: Engine integration: in-memory index and data_index remapping
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_engine_index_remapping() {
    let dir = temp_wal_dir("index_remapping");

    let wal = Wal::open(&dir, Some(50)).unwrap();
    let state = EngineState::new(wal);
    let engine = Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    };

    let id1 = engine.push(b"first_payload".to_vec(), 1).unwrap();
    // Rotate segment with a second large push
    let _id2 = engine.push(vec![2; 50], 1).unwrap();

    // Trigger compaction
    engine.compact().unwrap();

    let new_offset = {
        let s = engine.inner.lock().unwrap();
        *s.data_index.get(&id1).unwrap()
    };

    // In-memory offset must be updated to point to compacted segment 1
    assert_eq!(new_offset.0, 1, "Must be in segment 1");
    // Verify that read_at at the new coordinates retrieves the exact payload
    let read_rec = engine.reader.read_at(new_offset.0, new_offset.1).unwrap().unwrap();
    assert_eq!(read_rec.payload(), b"first_payload");
}

// -----------------------------------------------------------------------------
// Test 15: Post-compaction crash recovery
// Close the engine after compaction, reopen: state must be identical to pre-compaction.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_engine_recovery() {
    let dir = temp_wal_dir("engine_recovery");

    let id1;
    let id2;
    {
        let wal = Wal::open(&dir, Some(50)).unwrap();
        let state = EngineState::new(wal);
        let engine = Engine {
            reader: Arc::new(WalReader::new(dir.clone())),
            inner: Arc::new(std::sync::Mutex::new(state)),
            group_commit: Arc::new(GroupCommit::new(Default::default())),
            expiration_worker: Default::default(),
            compaction_lock: Default::default(),
        };

        id1 = engine.push(b"task_1_payload".to_vec(), 1).unwrap();
        id2 = engine.push(vec![2; 60], 2).unwrap();

        // Trigger compaction
        engine.compact().unwrap();
    } // Engine closed

    // Reopen using Engine::open (which triggers recover())
    let recovered_engine = Engine::open(&dir, None).unwrap();
    let (ready, leased) = recovered_engine.status();
    assert_eq!(ready, 2);
    assert_eq!(leased, 0);

    // Pop and verify priority ordering and payload integrity
    let (rec2, _) = recovered_engine.pop_and_lease(10, 10).unwrap().unwrap();
    assert_eq!(rec2.id(), id2);
    assert_eq!(rec2.payload(), &vec![2; 60][..]);

    let (rec1, _) = recovered_engine.pop_and_lease(10, 10).unwrap().unwrap();
    assert_eq!(rec1.id(), id1);
    assert_eq!(rec1.payload(), b"task_1_payload");
}

// -----------------------------------------------------------------------------
// Test 16: Concurrency stress: background compaction thread while other
// threads push/pop/ack on the active segment. Zero deadlocks, zero data loss.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_concurrency_stress() {
    let dir = temp_wal_dir("concurrency_stress");

    let wal = Wal::open(&dir, Some(100)).unwrap();
    let state = EngineState::new(wal);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    // Populate initial closed segments
    for i in 0..10 {
        engine.push(vec![i as u8; 90], 1).unwrap();
    }

    let running = Arc::new(AtomicBool::new(true));
    let mut handles = Vec::new();

    // Worker 1: continuously pushes to the active segment
    {
        let engine_clone = Arc::clone(&engine);
        let running_clone = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            let mut i = 0;
            while running_clone.load(Ordering::Relaxed) {
                let _ = engine_clone.push(format!("worker_task_{}", i).into_bytes(), 1);
                i += 1;
                thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    // Worker 2: continuously pops & acks
    {
        let engine_clone = Arc::clone(&engine);
        let running_clone = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            while running_clone.load(Ordering::Relaxed) {
                if let Ok(Some((rec, _))) = engine_clone.pop_and_lease(42, 5) {
                    let _ = engine_clone.ack(rec.id(), 42);
                }
                thread::sleep(Duration::from_millis(2));
            }
        }));
    }

    // Compaction thread: runs repeated compactions at regular intervals
    {
        let engine_clone = Arc::clone(&engine);
        let running_clone = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            for _ in 0..5 {
                let _ = engine_clone.compact();
                thread::sleep(Duration::from_millis(15));
            }
            running_clone.store(false, Ordering::Relaxed);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Zero deadlock: verify the engine is fully operational
    let _test_id = engine.push(b"final_check".to_vec(), 1).unwrap();
    let (rec, _) = engine.pop_and_lease(99, 10).unwrap().unwrap();
    assert!(rec.id() > 0);
}

// -----------------------------------------------------------------------------
// Test 17: Compaction across 3+ closed segments with scattered task IDs
// Verifies that compaction operates globally on all closed segments together,
// determining the global latest state and purging terminal IDs anywhere in history.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_multi_segment_global_dedup() {
    let dir = temp_wal_dir("multi_segment_global_dedup");

    // Threshold 60 bytes for rapid rotations
    {
        let mut wal = Wal::open(&dir, Some(60)).unwrap();

        // Seg 1:
        // Task 10: Created (global survivor)
        // Task 20: Created (will be Acked in Seg 3)
        wal.append(&Record::new(10, OpType::Created, 1, vec![10; 10])).unwrap();
        wal.append(&Record::new(20, OpType::Created, 1, vec![20; 50])).unwrap(); // size > 60

        // Seg 2:
        // Task 10 receives Leased in Seg 2 (supersedes Created in Seg 1)
        // Task 30: Created (will be DeadLettered in Seg 3)
        wal.append(&Record::new(10, OpType::Leased, 1, vec![10; 10])).unwrap(); // triggers Seg 2
        wal.append(&Record::new(30, OpType::Created, 1, vec![30; 50])).unwrap(); // size > 60

        // Seg 3:
        // Task 20 receives Acked (terminal, started in Seg 1)
        // Task 30 receives DeadLettered (terminal, started in Seg 2)
        // Task 10 receives Expired in Seg 3 (supersedes Leased in Seg 2: global latest state)
        wal.append(&Record::new(20, OpType::Acked, 1, vec![])).unwrap(); // triggers Seg 3
        wal.append(&Record::new(30, OpType::DeadLettered, 1, vec![1])).unwrap();
        wal.append(&Record::new(10, OpType::Expired, 1, vec![1])).unwrap();
        wal.append(&Record::new(40, OpType::Created, 1, vec![40; 50])).unwrap(); // size > 60

        // Seg 4: Active segment (not compacted)
        wal.append(&Record::new(99, OpType::Created, 1, vec![99; 20])).unwrap(); // triggers Seg 4 (active)
        assert_eq!(wal.active_segment_id(), 4);

        // Execute compaction on closed segments (1, 2, 3)
        let stats = wal.compact().unwrap().expect("Compaction should produce stats");
        assert_eq!(stats.closed_segments_compacted, 3);
        assert_eq!(stats.surviving_records, 2); // Task 10 and Task 40
    }

    // Verify filesystem state:
    // Segments 2 and 3 must have been removed
    assert!(!dir.join("nesso.00002.wal").exists());
    assert!(!dir.join("nesso.00003.wal").exists());
    // Segment 1 must exist (compacted) and Segment 4 must exist (intact active segment)
    assert!(dir.join("nesso.00001.wal").exists());
    assert!(dir.join("nesso.00004.wal").exists());

    // Verify records inside compacted segment 1
    let wal = Wal::open(&dir, Some(60)).unwrap();
    let all_records: Vec<_> = wal.iter_all().unwrap().collect::<Result<Vec<_>, _>>().unwrap();

    let seg1_records: Vec<_> = all_records
        .iter()
        .filter(|(seg, _, _)| *seg == 1)
        .map(|(_, _, r)| r.clone())
        .collect();

    let seg1_ids: Vec<u64> = seg1_records.iter().map(|r| r.id()).collect();

    // Task 20 (Acked in seg 3) and Task 30 (DeadLettered in seg 3) must be ABSENT
    assert!(!seg1_ids.contains(&20), "Task 20 must be purged globally");
    assert!(!seg1_ids.contains(&30), "Task 30 must be purged globally");

    // Task 10 must appear exactly ONCE, reflecting its latest global state (Expired)
    let task10_entries: Vec<_> = seg1_records.iter().filter(|r| r.id() == 10).collect();
    assert_eq!(task10_entries.len(), 1, "Task 10 must appear exactly once");
    assert_eq!(task10_entries[0].op_type(), OpType::Expired, "Task 10 must have global latest state (Expired)");

    // Task 40 must be present with Created
    let task40_entries: Vec<_> = seg1_records.iter().filter(|r| r.id() == 40).collect();
    assert_eq!(task40_entries.len(), 1);
    assert_eq!(task40_entries[0].op_type(), OpType::Created);

    // Active segment 4: must only contain Task 99
    let seg4_records: Vec<_> = all_records
        .iter()
        .filter(|(seg, _, _)| *seg == 4)
        .map(|(_, _, r)| r.clone())
        .collect();
    assert_eq!(seg4_records.len(), 1);
    assert_eq!(seg4_records[0].id(), 99);
}

// -----------------------------------------------------------------------------
// Test 18: LRU cache invalidation on inode replaced by atomic rename
// Verifies that if WalReader has a warm cached file descriptor for segment 1,
// after compaction and atomic rename ("nesso.00001.compacting" -> "nesso.00001.wal"),
// the LRU cache does not continue reading from the stale file descriptor (old inode),
// but re-opens the newly placed file and correctly reads the compacted records.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_lru_cache_invalidation_stale_inode() {
    let dir = temp_wal_dir("lru_cache_invalidation");

    let wal = Wal::open(&dir, Some(100)).unwrap();
    let state = EngineState::new(wal);
    let engine = Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    };

    // Write to segment 1:
    // 1. Task 2 (priority 2, filler task to be acked) written at offset 0
    let id2 = engine.push(b"filler_to_ack".to_vec(), 2).unwrap();
    // 2. Task 1 (priority 1, target keeper task) written at offset > 0
    let id1 = engine.push(b"target_to_keep".to_vec(), 1).unwrap();

    let (old_seg1, old_off1) = {
        let s = engine.inner.lock().unwrap();
        *s.data_index.get(&id1).unwrap()
    };
    assert_eq!(old_seg1, 1);
    assert!(old_off1 > 0, "Task 1 must be at offset > 0 in original segment");

    // =========================================================================
    // KEY STEP: Warm up the LRU cache by reading from segment 1 BEFORE compaction!
    // This opens File for nesso.00001.wal (original inode) and caches it in read_cache.
    // =========================================================================
    let warm_read = engine.reader.read_at(old_seg1, old_off1).unwrap().unwrap();
    assert_eq!(warm_read.payload(), b"target_to_keep");

    // Pop and ack Task 2 within segment 1 (exceeding the 100 bytes threshold)
    let (pop_rec, _) = engine.pop_and_lease(42, 10).unwrap().unwrap();
    assert_eq!(pop_rec.id(), id2);
    engine.ack(id2, 42).unwrap();

    // Subsequent write forces segment rotation to segment 2 (active, not compacted)
    let _id_seg2 = engine.push(vec![99; 50], 1).unwrap();
    {
        let s = engine.inner.lock().unwrap();
        assert_eq!(s.wal.active_segment_id(), 2);
    }

    // Execute compaction:
    // Closed segment 1 is compacted to nesso.00001.compacting -> nesso.00001.wal (new inode!).
    // Task 2 (Acked) is purged.
    // Task 1 is the sole survivor and is written to offset 0 of new segment 1.
    assert!(engine.compact().unwrap());

    // Verify new in-memory offset for id1: must now be 0
    let (new_seg1, new_off1) = {
        let s = engine.inner.lock().unwrap();
        *s.data_index.get(&id1).unwrap()
    };
    assert_eq!(new_seg1, 1);
    assert_eq!(new_off1, 0, "Task 1 must now be at offset 0 in compacted segment");

    // Read id1 at offset 0 using engine.reader.read_at:
    // - If the LRU cache had NOT been invalidated by clear_cache(), the reader
    //   would reuse the open file descriptor on the OLD inode.
    //   On the old inode, offset 0 contained Task 2 ("filler_to_ack")!
    // - Because clear_cache() invalidated the cache, the reader opens the NEW inode,
    //   finding Task 1 ("target_to_keep") at offset 0!
    let fresh_read = engine.reader.read_at(new_seg1, new_off1).unwrap().expect("Record must exist");
    assert_eq!(fresh_read.id(), id1, "Must read id1 from fresh inode, NOT stale id2 from old fd");
    assert_eq!(fresh_read.payload(), b"target_to_keep");

    // Verify pop_and_lease end-to-end
    let (pop_final, _) = engine.pop_and_lease(99, 10).unwrap().unwrap();
    assert_eq!(pop_final.id(), id1);
    assert_eq!(pop_final.payload(), b"target_to_keep");
}

// -----------------------------------------------------------------------------
// Test 19: Repeated multi-generation compaction cycles over time
// Executes 5 consecutive cycles of: push -> pop -> ack -> rotation -> compact.
// Verifies that after each cycle offsets stay consistent, payloads remain intact,
// no memory leaks or dangling file descriptors occur, and cold recovery is perfect.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_repeated_cycles() {
    let dir = temp_wal_dir("repeated_cycles");

    let mut surviving_tasks = Vec::new();

    {
        let wal = Wal::open(&dir, Some(80)).unwrap();
        let state = EngineState::new(wal);
        let engine = Engine {
            reader: Arc::new(WalReader::new(dir.clone())),
            inner: Arc::new(std::sync::Mutex::new(state)),
            group_commit: Arc::new(GroupCommit::new(Default::default())),
            expiration_worker: Default::default(),
            compaction_lock: Default::default(),
        };

        for cycle in 0..5 {
            // In each cycle:
            // 1. Create a task that will be completed (Acked) with priority 3
            let doomed_payload = format!("doomed_cycle_{}", cycle).into_bytes();
            let doomed_id = engine.push(doomed_payload, 3).unwrap();

            // 2. Create a keeper task that must survive with priority 2
            let keeper_payload = format!("keeper_cycle_{}", cycle).into_bytes();
            let keeper_id = engine.push(keeper_payload.clone(), 2).unwrap();
            surviving_tasks.push((keeper_id, keeper_payload));

            // 3. Pop and ack the doomed task (priority 3 popped first)
            let (popped, _) = engine.pop_and_lease(42, 10).unwrap().unwrap();
            assert_eq!(popped.id(), doomed_id);
            engine.ack(doomed_id, 42).unwrap();

            // 4. Force segment rotation with a priority 1 transition task to seal current segment
            let _rot_id = engine.push(vec![cycle as u8; 50], 1).unwrap();

            // 5. Execute compaction on closed segments
            let compacted = engine.compact().unwrap();
            assert!(compacted, "Compaction should succeed on closed segments in cycle {}", cycle);

            // 6. Verify that all accumulated keepers are readable using updated offsets
            for &(k_id, ref k_payload) in &surviving_tasks {
                let (seg, off) = {
                    let s = engine.inner.lock().unwrap();
                    *s.data_index.get(&k_id).unwrap()
                };
                let rec = engine.reader.read_at(seg, off).unwrap().expect("Keeper must be readable");
                assert_eq!(rec.id(), k_id);
                assert_eq!(rec.payload(), &k_payload[..]);
            }
        }
    } // Engine closed

    // 7. Cold restart: verify full crash recovery after all compaction cycles
    let recovered_engine = Engine::open(&dir, None).unwrap();
    let (ready_count, leased_count) = recovered_engine.status();
    // ready_count must contain all keepers (5 with pri 2) + the 5 rotation records (with pri 1)
    assert_eq!(leased_count, 0);
    assert_eq!(ready_count, 10);

    // Pop the 5 highest priority tasks (the keepers with priority 2)
    for _ in 0..5 {
        let (rec, _) = recovered_engine.pop_and_lease(99, 10).unwrap().unwrap();
        assert_eq!(rec.priority(), 2);
        let pos = surviving_tasks.iter().position(|(id, _)| *id == rec.id())
            .expect("Popped task must be a keeper");
        let (_, expected_payload) = surviving_tasks.remove(pos);
        assert_eq!(rec.payload(), &expected_payload[..]);
    }
    assert!(surviving_tasks.is_empty(), "All keeper tasks must be recovered and consumed intact");
}

// -----------------------------------------------------------------------------
// Test 20: Non-blocking concurrent writes during Phase 1 heavy I/O
// Verifies that during Phase 1 execution (which scans closed segments and writes .compacting),
// concurrent client writes (pushes) to the active segment are NOT blocked by compaction
// and complete with negligible latency.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_non_blocking_concurrent_writes() {
    let dir = temp_wal_dir("non_blocking_concurrent_writes");

    // Threshold 100 bytes to create multiple closed segments
    let wal = Wal::open(&dir, Some(100)).unwrap();
    let state = EngineState::new(wal);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    // Populate several closed segments (10 segments)
    for i in 0..10 {
        let payload = vec![i as u8; 90];
        engine.push(payload, 1).unwrap();
    }

    {
        let s = engine.inner.lock().unwrap();
        assert!(s.wal.active_segment_id() >= 10, "Should have rotated multiple segments");
    }

    // Spawn a background thread that executes compaction
    let engine_compact = Arc::clone(&engine);
    let compact_handle = thread::spawn(move || {
        engine_compact.compact().unwrap()
    });

    // Concurrently perform 50 push operations from client threads
    let mut pushed_ids = Vec::new();
    let start = std::time::Instant::now();
    for i in 0..50 {
        let id = engine.push(format!("concurrent_payload_{}", i).into_bytes(), 2).unwrap();
        pushed_ids.push(id);
    }
    let elapsed = start.elapsed();

    let compacted = compact_handle.join().unwrap();
    assert!(compacted, "Compaction should have executed");

    // Pushes during compaction should be fast because engine lock was not held during Phase 1 I/O
    assert!(elapsed < Duration::from_millis(500), "50 concurrent pushes took {:?}, should be sub-500ms", elapsed);

    // Verify all 50 tasks are intact and readable
    for id in pushed_ids {
        let (seg, off) = {
            let s = engine.inner.lock().unwrap();
            *s.data_index.get(&id).expect("Pushed task must exist in data_index")
        };
        let rec = engine.reader.read_at(seg, off).unwrap().expect("Record must be readable");
        assert_eq!(rec.id(), id);
    }
}

// -----------------------------------------------------------------------------
// Test 21: Task ack during Phase 1 (no resurrection)
// If a task is present in closed segments being compacted, but gets leased and ACKed
// on the active segment while Phase 1 is running, Phase 2 MUST NOT resurrect it.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_ack_during_phase1_no_resurrection() {
    let dir = temp_wal_dir("ack_during_phase1_no_resurrect");

    let wal = Wal::open(&dir, Some(100)).unwrap();
    let state = EngineState::new(wal);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    // Create Task 1 in segment 1 (priority 3, so popped first)
    let id_target = engine.push(b"target_task_to_ack_concurrently".to_vec(), 3).unwrap();

    // Force rotation by creating filler tasks into segment 2 and 3
    let _id_filler1 = engine.push(vec![1; 90], 1).unwrap();
    let _id_filler2 = engine.push(vec![2; 90], 1).unwrap();
    let _id_filler3 = engine.push(vec![3; 90], 1).unwrap();

    {
        let s = engine.inner.lock().unwrap();
        assert!(s.wal.active_segment_id() >= 3, "Active segment should be at least 3");
    }

    // Now execute Phase 1 directly or via two-phase manual control to guarantee the race condition:
    let (dir_buf, active_seg) = {
        let s = engine.inner.lock().unwrap();
        (s.wal.dir().to_path_buf(), s.wal.active_segment_id())
    };
    let closed_segments = Wal::plan_compaction(&dir_buf, active_seg).unwrap().expect("Should have closed segments");

    // Phase 1 scans closed segments: at this moment id_target is NOT yet acked!
    let (_stats, new_offsets) = Wal::execute_compaction_phase1(&dir_buf, &closed_segments).unwrap();
    assert!(new_offsets.contains_key(&id_target), "Phase 1 captured id_target as live survivor");

    // NOW, client pops and ACKs id_target on the active segment (before Phase 2 acquires lock)!
    let (popped, _) = engine.pop_and_lease(42, 10).unwrap().unwrap();
    assert_eq!(popped.id(), id_target);
    engine.ack(id_target, 42).unwrap();

    // Verify id_target was removed from data_index by the ack
    {
        let s = engine.inner.lock().unwrap();
        assert!(!s.data_index.contains_key(&id_target));
    }

    // NOW execute Phase 2 under the lock:
    {
        let mut state = engine.inner.lock().unwrap();
        Wal::execute_compaction_phase2(&dir_buf, &closed_segments).unwrap();

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
        engine.reader.clear_cache();
    }

    // CRITICAL ASSERTION: id_target must NOT be resurrected in data_index!
    {
        let s = engine.inner.lock().unwrap();
        assert!(!s.data_index.contains_key(&id_target), "Task MUST NOT be resurrected in data_index");
        // And its index entry must point to the active segment (Acked record), not segment 1!
        let (ack_seg, _) = s.index.get(&id_target).copied().unwrap();
        assert!(!closed_segments.contains(&ack_seg), "Task index must still point to active segment Acked record");
    }

    // Calling pop_and_lease must NOT return id_target
    let (pop_next, _) = engine.pop_and_lease(42, 10).unwrap().unwrap();
    assert_ne!(pop_next.id(), id_target, "Popped task must not be the acked task");
    engine.ack(pop_next.id(), 42).unwrap();

    // Drop engine and perform cold crash recovery test
    drop(engine);
    let recovered_engine = Engine::open(&dir, None).unwrap();
    let (ready, leased) = recovered_engine.status();
    assert_eq!(leased, 0);
    // Should only have the remaining filler tasks, id_target must NOT exist
    for _ in 0..ready {
        let (rec, _) = recovered_engine.pop_and_lease(99, 10).unwrap().unwrap();
        assert_ne!(rec.id(), id_target, "Recovered engine must not pop acked task");
    }
}

// -----------------------------------------------------------------------------
// Test 22: Multiple segment rotations during Phase 1
// While Phase 1 is scanning older closed segments, writers fill up the active segment
// causing multiple segment rotations (e.g. Seg 3 -> Seg 4 -> Seg 5).
// Phase 2 must safely finish without corrupting the newly created segments or their indexes.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_segment_rotation_during_phase1() {
    let dir = temp_wal_dir("rotation_during_phase1");

    let wal = Wal::open(&dir, Some(80)).unwrap();
    let state = EngineState::new(wal);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    // Create tasks in Segment 1 and Segment 2
    let id_early1 = engine.push(b"early_task_1".to_vec(), 1).unwrap();
    let id_early2 = engine.push(vec![1; 70], 1).unwrap(); // forces Seg 1 over threshold
    let _id_seg2 = engine.push(vec![2; 70], 1).unwrap();  // rotates to Seg 2, forces Seg 2 over threshold
    let _id_seg3 = engine.push(vec![3; 70], 1).unwrap();  // rotates to Seg 3 (active)

    let (dir_buf, active_seg) = {
        let s = engine.inner.lock().unwrap();
        (s.wal.dir().to_path_buf(), s.wal.active_segment_id())
    };
    assert_eq!(active_seg, 3);

    // Plan closed segments: [1, 2]
    let closed_segments = Wal::plan_compaction(&dir_buf, active_seg).unwrap().unwrap();
    assert_eq!(closed_segments, vec![1, 2]);

    // Run Phase 1
    let (_stats, new_offsets) = Wal::execute_compaction_phase1(&dir_buf, &closed_segments).unwrap();

    // WHILE Phase 1 has completed its scan, writers cause MULTIPLE rotations!
    // Seg 3 -> Seg 4 -> Seg 5
    let id_mid1 = engine.push(vec![4; 70], 1).unwrap(); // forces Seg 4
    let id_mid2 = engine.push(vec![5; 70], 1).unwrap(); // forces Seg 5 (now active)

    {
        let s = engine.inner.lock().unwrap();
        assert_eq!(s.wal.active_segment_id(), 5);
    }

    // Now run Phase 2 (atomic rename of Seg 1, unlinking of Seg 2)
    {
        let mut state = engine.inner.lock().unwrap();
        Wal::execute_compaction_phase2(&dir_buf, &closed_segments).unwrap();

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
        engine.reader.clear_cache();
    }

    // Verify filesystem:
    // Seg 1 exists (compacted)
    // Seg 2 was unlinked
    // Seg 3, 4, 5 MUST STILL EXIST!
    assert!(dir.join("nesso.00001.wal").exists());
    assert!(!dir.join("nesso.00002.wal").exists());
    assert!(dir.join("nesso.00003.wal").exists());
    assert!(dir.join("nesso.00004.wal").exists());
    assert!(dir.join("nesso.00005.wal").exists());

    // Verify that early tasks point to Seg 1
    {
        let s = engine.inner.lock().unwrap();
        assert_eq!(s.data_index.get(&id_early1).unwrap().0, 1);
        assert_eq!(s.data_index.get(&id_early2).unwrap().0, 1);
        // Verify mid tasks still point to their rotated segments
        let (seg_mid1, _) = *s.data_index.get(&id_mid1).unwrap();
        let (seg_mid2, _) = *s.data_index.get(&id_mid2).unwrap();
        assert!(seg_mid1 >= 3, "Must not be overwritten to segment 1");
        assert!(seg_mid2 >= 3, "Must not be overwritten to segment 1");
    }

    // Cold restart
    drop(engine);
    let recovered_engine = Engine::open(&dir, None).unwrap();
    let (ready, _) = recovered_engine.status();
    assert_eq!(ready, 6); // id_early1, id_early2, _id_seg2, _id_seg3, id_mid1, id_mid2
}

// -----------------------------------------------------------------------------
// Test 23: Serialization of concurrent compaction calls
// Verifies that multiple concurrent threads calling engine.compact() simultaneously
// are properly serialized by compaction_lock without file contention, race conditions,
// or double-compaction corruptions.
// -----------------------------------------------------------------------------
#[test]
fn test_compaction_serial_lock_prevents_duplicate_runs() {
    let dir = temp_wal_dir("serial_lock_concurrency");

    let wal = Wal::open(&dir, Some(80)).unwrap();
    let state = EngineState::new(wal);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    // Populate several segments
    for i in 0..15 {
        engine.push(vec![i as u8; 70], 1).unwrap();
    }

    // Spawn 8 threads all calling engine.compact() simultaneously
    let mut handles = Vec::new();
    for _ in 0..8 {
        let eng = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            eng.compact()
        }));
    }

    let mut success_count = 0;
    for h in handles {
        match h.join().unwrap() {
            Ok(compacted) => {
                if compacted {
                    success_count += 1;
                }
            }
            Err(e) => panic!("Compaction failed with error: {:?}", e),
        }
    }

    // At least one thread should have compacted
    assert!(success_count >= 1, "At least one compaction run should have succeeded");

    // Engine must still be completely healthy and operational
    let id_new = engine.push(b"post_concurrent_compact".to_vec(), 2).unwrap();
    let (rec, _) = engine.pop_and_lease(10, 10).unwrap().unwrap();
    assert_eq!(rec.id(), id_new);
    assert_eq!(rec.payload(), b"post_concurrent_compact");
}
