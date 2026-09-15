use nesso::storage::engine::{Engine, EngineState, GroupCommit};
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

fn temp_test_dir(test_name: &str) -> PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!(
        "nesso_adv_test_{}_{}_{}",
        test_name,
        std::process::id(),
        count
    ));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

// -----------------------------------------------------------------------------
// Defect Reproduction Test: Empty payload corruption across compaction & cold disk fallback
// Documents and reproduces the bug where tasks with 0-byte payload return 13 bytes
// of lease metadata instead of 0 bytes when popped from disk after compaction.
// -----------------------------------------------------------------------------
#[test]
fn test_reproduce_compaction_empty_payload_defect() {
    let dir = temp_test_dir("compaction_empty_payload_defect");
    let wal = Wal::open(&dir, Some(150)).unwrap();
    let state = EngineState::new(wal);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    // Push task with EMPTY payload
    let empty_payload = Vec::new();
    let id = engine.push(empty_payload.clone(), 10).unwrap();

    // Pop and lease the task
    let (rec, _) = engine.pop_and_lease(100, 3600).unwrap().unwrap();
    assert_eq!(rec.id(), id);
    assert_eq!(rec.payload(), &empty_payload[..]);

    // Push filler to force segment rollover
    let filler = vec![0xAA; 120];
    let _ = engine.push(filler.clone(), 1).unwrap();
    let _ = engine.push(filler.clone(), 1).unwrap();

    let active_seg = engine.inner.lock().unwrap().wal.active_segment_id();
    assert!(active_seg >= 2);

    // Compact closed segments
    let compacted = engine.compact().unwrap();
    assert!(compacted);

    // Clear payload cache to force cold disk read
    engine.clear_payload_cache();

    // Nack the task: returns to ready queue
    engine.nack(id, 100).unwrap();

    // Pop the task again
    let (rec2, retries) = engine.pop_and_lease(200, 60).unwrap().unwrap();
    assert_eq!(rec2.id(), id);
    assert_eq!(retries, 1);
    // This panics because rec2.payload() is [0, 0, 0, 100, 0, 0, 0, 0, ...] (13 bytes)
    assert_eq!(
        rec2.payload(),
        &empty_payload[..],
        "Popped payload must be empty (0 bytes)"
    );

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Stress Test 1: Exhaustive byte-by-byte torn write sweep at active segment tail
// Sweeps EVERY possible truncate offset from 1 to 68 bytes of a torn frame.
// Asserts Wal::open truncates cleanly, appends proceed, and prior records survive.
// -----------------------------------------------------------------------------
#[test]
fn test_stress_torn_writes_exhaustive_byte_offsets() {
    let base_dir = temp_test_dir("torn_exhaustive_sweep");

    let rec_valid = Record::new(1, OpType::Created, 10, b"valid-seed-record-payload".to_vec());
    let valid_bytes = rec_valid.encode();

    let torn_rec = Record::new(999, OpType::Created, 10, vec![0xFE; 50]);
    let torn_encoded = torn_rec.encode(); // 19 header + 50 payload = 69 bytes
    let total_torn_len = torn_encoded.len();

    for cut_offset in 1..total_torn_len {
        let sub_dir = base_dir.join(format!("cut_{}", cut_offset));
        fs::create_dir_all(&sub_dir).unwrap();

        let wal_path = sub_dir.join("nesso.00001.wal");
        {
            let mut f = File::create(&wal_path).unwrap();
            f.write_all(&valid_bytes).unwrap();
            f.write_all(&torn_encoded[..cut_offset]).unwrap();
            f.sync_all().unwrap();
        }

        // Wal::open must detect the torn frame and truncate back to valid_bytes.len()
        let wal = Wal::open(&sub_dir, None).unwrap();
        assert_eq!(
            wal.current_size(),
            valid_bytes.len() as u64,
            "Failed at cut offset {}",
            cut_offset
        );
        drop(wal);

        // Open engine, append new task, verify durability
        let engine = Engine::open(&sub_dir, None).unwrap();
        let new_id = engine.push(b"new-task-after-torn-recovery".to_vec(), 10).unwrap();
        assert_eq!(new_id, 2);

        let (t1, _) = engine.pop_and_lease(100, 60).unwrap().unwrap();
        assert_eq!(t1.id(), 1);
        assert_eq!(t1.payload(), b"valid-seed-record-payload");

        let (t2, _) = engine.pop_and_lease(100, 60).unwrap().unwrap();
        assert_eq!(t2.id(), 2);
        assert_eq!(t2.payload(), b"new-task-after-torn-recovery");

        engine.shutdown().unwrap();
    }

    let _ = fs::remove_dir_all(&base_dir);
}

// -----------------------------------------------------------------------------
// Stress Test 2: High concurrency rapid rollover under continuous compaction
// -----------------------------------------------------------------------------
#[test]
fn test_stress_high_concurrency_rapid_rollover_compaction() {
    let dir = temp_test_dir("rapid_rollover_stress");

    // Tiny segment threshold: 256 bytes -> rapid rollovers
    let wal = Wal::open(&dir, Some(256)).unwrap();
    let state = EngineState::new(wal);
    let engine = Arc::new(Engine {
        reader: Arc::new(WalReader::new(dir.clone())),
        inner: Arc::new(std::sync::Mutex::new(state)),
        group_commit: Arc::new(GroupCommit::new(Default::default())),
        expiration_worker: Default::default(),
        compaction_lock: Default::default(),
    });

    let running = Arc::new(AtomicBool::new(true));

    // 2 background compactor threads
    let mut compactor_handles = Vec::new();
    for _ in 0..2 {
        let eng = Arc::clone(&engine);
        let run = Arc::clone(&running);
        compactor_handles.push(thread::spawn(move || {
            let mut count = 0;
            while run.load(Ordering::Relaxed) {
                if let Ok(true) = eng.compact() {
                    count += 1;
                }
                thread::sleep(Duration::from_millis(2));
            }
            count
        }));
    }

    // 6 concurrent writer threads pushing tasks
    let mut writer_handles = Vec::new();
    let tasks_per_thread = 50;
    for t in 0..6 {
        let eng = Arc::clone(&engine);
        writer_handles.push(thread::spawn(move || {
            let mut ids = Vec::new();
            for i in 0..tasks_per_thread {
                let payload = format!("payload_thread_{}_seq_{:04}", t, i).into_bytes();
                let id = eng.push(payload, (i % 5 + 1) as u8).unwrap();
                ids.push(id);
            }
            ids
        }));
    }

    // 4 concurrent consumer threads leasing and acking
    let mut consumer_handles = Vec::new();
    for c in 0..4 {
        let eng = Arc::clone(&engine);
        let run = Arc::clone(&running);
        consumer_handles.push(thread::spawn(move || {
            let consumer_id = 9000 + c as u32;
            let mut acked = 0;
            while run.load(Ordering::Relaxed) {
                if let Ok(Some((rec, _))) = eng.pop_and_lease(consumer_id, 30) {
                    let _ = eng.ack(rec.id(), consumer_id);
                    acked += 1;
                }
                thread::yield_now();
            }
            acked
        }));
    }

    // Wait for writers
    let mut all_ids = Vec::new();
    for h in writer_handles {
        all_ids.extend(h.join().unwrap());
    }

    // Allow consumers and compactors to run a bit longer
    thread::sleep(Duration::from_millis(100));
    running.store(false, Ordering::SeqCst);

    for h in compactor_handles {
        h.join().unwrap();
    }
    for h in consumer_handles {
        h.join().unwrap();
    }

    // Final compaction
    let _ = engine.compact();

    // Verify all active records in data_index can be read cleanly
    {
        let state = engine.inner.lock().unwrap();
        for (&id, &(seg, off)) in &state.data_index {
            let rec = engine.reader.read_at(seg, off).unwrap();
            assert!(
                rec.is_some(),
                "Task {} at seg {} off {} must be readable",
                id,
                seg,
                off
            );
        }
    }

    // Cold reboot recovery
    drop(engine);
    let recovered_engine = Engine::open(&dir, None).unwrap();
    let (ready, leased) = recovered_engine.status();
    assert!(
        ready + leased <= 300,
        "Total surviving tasks must be <= 300"
    );

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Stress Test 3: Corruption matrix in closed segments
// Injects bad magic bytes, corrupted CRC32, and oversized payload_len into
// intermediate closed segments, verifying iter_all and recovery skip them safely.
// -----------------------------------------------------------------------------
#[test]
fn test_stress_closed_segment_corruption_matrix() {
    let dir = temp_test_dir("closed_segment_corruptions");

    // Segment 1: 3 valid records, 1 record with invalid magic byte in middle, 1 valid record
    let seg1_path = dir.join("nesso.00001.wal");
    {
        let mut buf = Vec::new();
        for i in 1..=3 {
            let r = Record::new(i, OpType::Created, 1, format!("seg1-{}", i).into_bytes());
            buf.extend_from_slice(&r.encode());
        }
        let bad_magic_r = Record::new(991, OpType::Created, 1, b"corrupted-magic".to_vec());
        let mut bad_enc = bad_magic_r.encode();
        bad_enc[0] = 0xAA; // bad magic
        buf.extend_from_slice(&bad_enc);

        let r5 = Record::new(5, OpType::Created, 1, b"seg1-5".to_vec());
        buf.extend_from_slice(&r5.encode());

        fs::write(&seg1_path, &buf).unwrap();
    }

    // Segment 2: 2 valid records, 1 record with corrupted CRC32, 1 valid record
    let seg2_path = dir.join("nesso.00002.wal");
    {
        let mut buf = Vec::new();
        for i in 6..=7 {
            let r = Record::new(i, OpType::Created, 1, format!("seg2-{}", i).into_bytes());
            buf.extend_from_slice(&r.encode());
        }
        let bad_crc_r = Record::new(992, OpType::Created, 1, b"corrupted-crc".to_vec());
        let mut bad_crc_enc = bad_crc_r.encode();
        bad_crc_enc[1] ^= 0xFF; // flip bits in checksum
        buf.extend_from_slice(&bad_crc_enc);

        let r9 = Record::new(9, OpType::Created, 1, b"seg2-9".to_vec());
        buf.extend_from_slice(&r9.encode());

        fs::write(&seg2_path, &buf).unwrap();
    }

    // Active segment 3: 2 valid records
    let seg3_path = dir.join("nesso.00003.wal");
    {
        let mut buf = Vec::new();
        for i in 10..=11 {
            let r = Record::new(i, OpType::Created, 1, format!("seg3-{}", i).into_bytes());
            buf.extend_from_slice(&r.encode());
        }
        fs::write(&seg3_path, &buf).unwrap();
    }

    let wal = Wal::open(&dir, None).unwrap();
    let records: Vec<_> = wal
        .iter_all()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Verify corrupted records (991, 992) were not recovered
    for (_, _, rec) in &records {
        assert_ne!(rec.id(), 991, "Corrupted magic byte record must be skipped");
        assert_ne!(rec.id(), 992, "Corrupted CRC record must be skipped");
    }

    let engine = Engine::open(&dir, None).unwrap();
    let mut popped_ids = Vec::new();
    while let Some((rec, _)) = engine.pop_and_lease(1, 60).unwrap() {
        popped_ids.push(rec.id());
    }

    assert!(
        !popped_ids.contains(&991) && !popped_ids.contains(&992),
        "Corrupted records must never be popped"
    );

    engine.shutdown().unwrap();
    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Stress Test 4: Cold reboot FIFO preservation with non-contiguous IDs
// -----------------------------------------------------------------------------
#[test]
fn test_stress_cold_reboot_non_contiguous_fifo() {
    let dir = temp_test_dir("cold_reboot_non_contiguous");

    let mut pushed_order = Vec::new();
    {
        let wal = Wal::open(&dir, Some(100)).unwrap(); // force frequent rollovers
        let state = EngineState::new(wal);
        let engine = Arc::new(Engine {
            reader: Arc::new(WalReader::new(dir.clone())),
            inner: Arc::new(std::sync::Mutex::new(state)),
            group_commit: Arc::new(GroupCommit::new(Default::default())),
            expiration_worker: Default::default(),
            compaction_lock: Default::default(),
        });

        // Push 30 tasks with non-contiguous priorities
        let priorities = [10u8, 50u8, 100u8];
        for prio in priorities {
            for i in 0..10 {
                let payload = format!("task_prio_{}_idx_{}", prio, i).into_bytes();
                let id = engine.push(payload.clone(), prio).unwrap();
                pushed_order.push((id, prio, payload));
            }
        }

        // Run compaction
        let _ = engine.compact();
        engine.shutdown().unwrap();
    }

    // Reopen from cold reboot
    let recovered_engine = Engine::open(&dir, None).unwrap();

    let mut popped = Vec::new();
    while let Some((rec, _)) = recovered_engine.pop_and_lease(1, 60).unwrap() {
        popped.push((rec.id(), rec.priority(), rec.payload().to_vec()));
    }

    assert_eq!(popped.len(), 30, "All 30 tasks must be recovered");

    // Group popped by priority: 100 first, then 50, then 10
    let prio_100: Vec<_> = popped.iter().filter(|t| t.1 == 100).collect();
    let prio_50: Vec<_> = popped.iter().filter(|t| t.1 == 50).collect();
    let prio_10: Vec<_> = popped.iter().filter(|t| t.1 == 10).collect();

    assert_eq!(prio_100.len(), 10);
    assert_eq!(prio_50.len(), 10);
    assert_eq!(prio_10.len(), 10);

    // Verify FIFO within each priority
    for (idx, item) in prio_100.iter().enumerate() {
        assert_eq!(
            item.2,
            format!("task_prio_100_idx_{}", idx).into_bytes()
        );
    }
    for (idx, item) in prio_50.iter().enumerate() {
        assert_eq!(
            item.2,
            format!("task_prio_50_idx_{}", idx).into_bytes()
        );
    }
    for (idx, item) in prio_10.iter().enumerate() {
        assert_eq!(
            item.2,
            format!("task_prio_10_idx_{}", idx).into_bytes()
        );
    }

    recovered_engine.shutdown().unwrap();
    let _ = fs::remove_dir_all(&dir);
}
