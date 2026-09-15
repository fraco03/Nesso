use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use nesso::storage::engine::{Engine, GroupCommitConfig, SyncMode};
use nesso::storage::record::{OpType, Record, HEADER_SIZE, MAGIC_BYTE};
use nesso::storage::wal::{Wal, WalReader};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_test_dir(test_name: &str) -> PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("nesso_crash_edge_{}_{}_{}", test_name, count, ts));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

// -----------------------------------------------------------------------------
// Test 1: Simulated crashes creating various .compacting files
// -----------------------------------------------------------------------------
#[test]
fn test_crash_phase1_partial_compacting_variants() {
    let dir = temp_test_dir("crash_phase1_variants");

    // Initialize engine, write tasks across segments
    {
        let engine = Engine::open_with_config(
            &dir,
            GroupCommitConfig {
                sync_mode: SyncMode::Standard,
                max_batch_size: 1,
                batch_window: Duration::from_millis(1),
                idle_commit_threshold: Duration::ZERO,
            },
        )
        .unwrap();

        engine.push(b"task-1-data".to_vec(), 10).unwrap();
        engine.force_flush_group_commit().unwrap();

        engine.push(b"task-2-data".to_vec(), 20).unwrap();
        engine.force_flush_group_commit().unwrap();
        engine.shutdown().unwrap();
    }

    // Variant A: 0-byte .compacting file
    let compacting_path = dir.join("nesso.00001.compacting");
    File::create(&compacting_path).unwrap();
    assert!(compacting_path.exists());

    {
        let engine = Engine::open(&dir, None).unwrap();
        assert!(
            !compacting_path.exists(),
            "0-byte .compacting file must be unlinked"
        );

        let t1 = engine
            .pop_and_lease(1, 60)
            .unwrap()
            .expect("Task with priority 20 should pop");
        assert_eq!(t1.0.priority(), 20);
        assert_eq!(t1.0.payload(), b"task-2-data");

        let t2 = engine
            .pop_and_lease(1, 60)
            .unwrap()
            .expect("Task with priority 10 should pop");
        assert_eq!(t2.0.priority(), 10);
        assert_eq!(t2.0.payload(), b"task-1-data");

        engine.shutdown().unwrap();
    }

    // Variant B: Truncated header (10 bytes) in .compacting and an uncleaned .tmp file
    {
        let mut f = File::create(&compacting_path).unwrap();
        f.write_all(&[
            MAGIC_BYTE, 0x01, 0x02, 0x03, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00,
        ])
        .unwrap();
        f.sync_all().unwrap();
    }
    let tmp_path = dir.join("nesso.00001.tmp");
    {
        let mut f = File::create(&tmp_path).unwrap();
        f.write_all(b"temporary unfinished state").unwrap();
        f.sync_all().unwrap();
    }
    assert!(compacting_path.exists());
    assert!(tmp_path.exists());

    {
        let engine = Engine::open(&dir, None).unwrap();
        assert!(
            !compacting_path.exists(),
            "Truncated .compacting must be unlinked"
        );
        assert!(!tmp_path.exists(), "Leftover .tmp file must be unlinked");
        engine.shutdown().unwrap();
    }

    // Variant C: Full valid record in .compacting but before rename, with unsynced data
    {
        let mut f = File::create(&compacting_path).unwrap();
        let rec = Record::new(999, OpType::Created, 100, b"uncommitted-record".to_vec());
        f.write_all(&rec.encode()).unwrap();
    }
    assert!(compacting_path.exists());

    {
        let engine = Engine::open(&dir, None).unwrap();
        assert!(
            !compacting_path.exists(),
            ".compacting before rename must be dropped"
        );

        // Uncommitted record 999 must NOT be in the system
        let all_records: Vec<_> = engine
            .inner
            .lock()
            .unwrap()
            .wal
            .iter_all()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for (_, _, rec) in all_records {
            assert_ne!(
                rec.id(),
                999,
                "Uncommitted compaction record must not be recovered"
            );
        }
        engine.shutdown().unwrap();
    }

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Test 2: Crash mid-Phase 2 between rename and unlink
// -----------------------------------------------------------------------------
#[test]
fn test_crash_phase2_post_rename_pre_unlink() {
    let dir = temp_test_dir("crash_phase2_post_rename");

    // Populate engine with small segments triggering rotations
    {
        let mut wal = Wal::open(&dir, Some(50)).unwrap(); // 50 bytes threshold forces rotation

        // Segment 1: Task 1 (created), Task 2 (created)
        wal.append(&Record::new(1, OpType::Created, 10, vec![1; 20]))
            .unwrap();
        wal.append(&Record::new(2, OpType::Created, 10, vec![2; 20]))
            .unwrap();

        // Segment 2: Task 1 (acked), Task 3 (created)
        wal.append(&Record::new(1, OpType::Acked, 10, vec![]))
            .unwrap();
        wal.append(&Record::new(3, OpType::Created, 10, vec![3; 20]))
            .unwrap();

        // Segment 3: Task 3 (acked), Task 4 (created)
        wal.append(&Record::new(3, OpType::Acked, 10, vec![]))
            .unwrap();
        wal.append(&Record::new(4, OpType::Created, 10, vec![4; 20]))
            .unwrap();

        // Segment 4 (active): Task 5 (created)
        wal.append(&Record::new(5, OpType::Created, 10, vec![5; 20]))
            .unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.active_segment_id(), 4);
    }

    // Execute Phase 1 on closed segments [1, 2, 3]
    let closed_segments = vec![1, 2, 3];
    let (stats, offsets) = Wal::execute_compaction_phase1(&dir, &closed_segments).unwrap();
    assert_eq!(stats.surviving_records, 2); // Task 2 and Task 4 survive
    assert!(offsets.contains_key(&2));
    assert!(offsets.contains_key(&4));
    assert!(!offsets.contains_key(&1));
    assert!(!offsets.contains_key(&3));

    let compacting_path = dir.join("nesso.00001.compacting");
    let dest_path = dir.join("nesso.00001.wal");
    assert!(compacting_path.exists());

    // Simulate crash mid-Phase 2:
    // Atomic rename succeeds: nesso.00001.compacting -> nesso.00001.wal
    // BUT the crash occurs before removing closed segments > 1 (00002.wal and 00003.wal remain)
    fs::rename(&compacting_path, &dest_path).unwrap();
    assert!(dest_path.exists());
    assert!(dir.join("nesso.00002.wal").exists());
    assert!(dir.join("nesso.00003.wal").exists());

    // Reopen engine: must clean up obsolete segments 2 and 3 and avoid resurrecting acked tasks 1 and 3
    let engine = Engine::open(&dir, None).unwrap();

    assert!(
        !dir.join("nesso.00002.wal").exists(),
        "Segment 2 must be unlinked post-crash"
    );
    assert!(
        !dir.join("nesso.00003.wal").exists(),
        "Segment 3 must be unlinked post-crash"
    );

    let mut popped_ids = Vec::new();
    while let Some((rec, _)) = engine.pop_and_lease(1, 60).unwrap() {
        popped_ids.push(rec.id());
    }

    assert_eq!(
        popped_ids,
        vec![2, 4, 5],
        "Only active tasks 2, 4, 5 must be recovered; acked tasks 1 and 3 must not be resurrected"
    );
    engine.shutdown().unwrap();
    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Test 3: Torn write at active segment tail and safe truncation
// -----------------------------------------------------------------------------
#[test]
fn test_torn_write_active_segment_tail_truncation() {
    let dir = temp_test_dir("torn_active_tail_truncation");

    // Push 5 valid tasks to active segment
    {
        let engine = Engine::open(&dir, None).unwrap();
        for i in 1..=5 {
            engine
                .push(format!("payload-{}", i).into_bytes(), 10)
                .unwrap();
        }
        engine.force_flush_group_commit().unwrap();
        engine.shutdown().unwrap();
    }

    let active_path = dir.join("nesso.00001.wal");
    let valid_len = fs::metadata(&active_path).unwrap().len();

    // Incur a torn write at the active segment tail:
    // A valid 19-byte header declaring payload_len = 120, but only 35 bytes of payload written
    let torn_rec = Record::new(999, OpType::Created, 10, vec![0xEE; 120]);
    let encoded = torn_rec.encode(); // 19 + 120 = 139 bytes
    let torn_slice = &encoded[..54]; // 19 header + 35 payload (partial)

    {
        let mut f = OpenOptions::new().append(true).open(&active_path).unwrap();
        f.write_all(torn_slice).unwrap();
        f.sync_all().unwrap();
    }

    assert_eq!(fs::metadata(&active_path).unwrap().len(), valid_len + 54);

    // Reopen engine: Wal::open must detect the torn tail and truncate the file back to valid_len
    {
        let engine = Engine::open(&dir, None).unwrap();
        assert_eq!(
            fs::metadata(&active_path).unwrap().len(),
            valid_len,
            "Active segment must be truncated to last valid frame offset"
        );

        // Push 3 new records: IDs 6, 7, 8
        for i in 6..=8 {
            engine
                .push(format!("payload-{}", i).into_bytes(), 10)
                .unwrap();
        }
        engine.force_flush_group_commit().unwrap();
        engine.shutdown().unwrap();
    }

    // Reopen engine again: all 8 records must be recovered with zero loss
    {
        let engine = Engine::open(&dir, None).unwrap();
        let mut popped = Vec::new();
        while let Some((rec, _)) = engine.pop_and_lease(1, 60).unwrap() {
            popped.push((rec.id(), rec.payload().to_vec()));
        }

        assert_eq!(popped.len(), 8, "All 8 valid records must be recovered");
        for (idx, (id, payload)) in popped.iter().enumerate() {
            let expected_id = (idx + 1) as u64;
            let expected_payload = format!("payload-{}", expected_id).into_bytes();
            assert_eq!(*id, expected_id);
            assert_eq!(*payload, expected_payload);
        }

        engine.shutdown().unwrap();
    }

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Test 4: Truncated record at closed segment boundary
// -----------------------------------------------------------------------------
#[test]
fn test_torn_write_closed_segment_boundary() {
    let dir = temp_test_dir("torn_closed_boundary");

    // Closed segment 1 with 3 valid records, plus 8 bytes of incomplete header at tail
    let seg1_path = dir.join("nesso.00001.wal");
    {
        let mut f = File::create(&seg1_path).unwrap();
        for i in 1..=3 {
            let rec = Record::new(i, OpType::Created, 5, format!("seg1-{}", i).into_bytes());
            f.write_all(&rec.encode()).unwrap();
        }
        f.write_all(&[MAGIC_BYTE, 0x11, 0x22, 0x33, 0x44, 0x00, 0x05, 0x99])
            .unwrap();
        f.sync_all().unwrap();
    }

    // Closed segment 2 with 2 valid records, plus truncated payload at tail
    let seg2_path = dir.join("nesso.00002.wal");
    {
        let mut f = File::create(&seg2_path).unwrap();
        for i in 4..=5 {
            let rec = Record::new(i, OpType::Created, 5, format!("seg2-{}", i).into_bytes());
            f.write_all(&rec.encode()).unwrap();
        }
        let torn = Record::new(888, OpType::Created, 5, vec![0xAA; 60]);
        let enc = torn.encode();
        f.write_all(&enc[..34]).unwrap(); // 19 header + 15 payload
        f.sync_all().unwrap();
    }

    // Active segment 3 with 2 valid records
    let seg3_path = dir.join("nesso.00003.wal");
    {
        let mut f = File::create(&seg3_path).unwrap();
        for i in 6..=7 {
            let rec = Record::new(i, OpType::Created, 5, format!("seg3-{}", i).into_bytes());
            f.write_all(&rec.encode()).unwrap();
        }
        f.sync_all().unwrap();
    }

    // Open WAL and verify iter_all() skips partial bytes at segment boundaries
    let wal = Wal::open(&dir, None).unwrap();
    assert_eq!(wal.active_segment_id(), 3);

    let all_records: Vec<_> = wal
        .iter_all()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        all_records.len(),
        7,
        "Must recover all 7 valid records across the 3 segments"
    );

    let ids: Vec<u64> = all_records.iter().map(|(_, _, r)| r.id()).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6, 7]);

    // Verify engine recovery
    let engine = Engine::open(&dir, None).unwrap();
    let mut popped_ids = Vec::new();
    while let Some((rec, _)) = engine.pop_and_lease(1, 60).unwrap() {
        popped_ids.push(rec.id());
    }
    assert_eq!(popped_ids, vec![1, 2, 3, 4, 5, 6, 7]);
    engine.shutdown().unwrap();

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Test 5: Bad magic byte variations
// -----------------------------------------------------------------------------
#[test]
fn test_corrupted_header_magic_byte_variations() {
    let dir = temp_test_dir("corrupted_magic_byte");

    let wal_path = dir.join("nesso.00001.wal");
    let reader = WalReader::new(dir.clone());

    // Case A: Magic byte corruption at segment start
    {
        let rec = Record::new(1, OpType::Created, 1, b"start-record".to_vec());
        let mut enc = rec.encode();
        enc[0] = 0x00; // Invalid magic byte
        fs::write(&wal_path, &enc).unwrap();

        let res = reader.read_at(1, 0);
        assert!(res.is_err(), "Invalid magic byte must return Err");
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert!(Record::decode(&enc).is_none());
    }

    // Case B: Magic byte corruption in middle of segment
    {
        let rec1 = Record::new(1, OpType::Created, 1, b"rec-1".to_vec());
        let rec2 = Record::new(2, OpType::Created, 1, b"rec-2".to_vec());
        let rec3 = Record::new(3, OpType::Created, 1, b"rec-3".to_vec());

        let enc1 = rec1.encode();
        let mut enc2 = rec2.encode();
        enc2[0] = 0xFF; // Bad magic byte
        let enc3 = rec3.encode();

        let mut buf = Vec::new();
        let off1 = 0u64;
        buf.extend_from_slice(&enc1);
        let off2 = buf.len() as u64;
        buf.extend_from_slice(&enc2);
        let off3 = buf.len() as u64;
        buf.extend_from_slice(&enc3);

        fs::write(&wal_path, &buf).unwrap();

        let r1 = reader
            .read_at(1, off1)
            .unwrap()
            .expect("Record 1 should be readable");
        assert_eq!(r1.id(), 1);

        let r2 = reader.read_at(1, off2);
        assert!(r2.is_err());
        assert_eq!(r2.unwrap_err().kind(), io::ErrorKind::InvalidData);

        let r3 = reader
            .read_at(1, off3)
            .unwrap()
            .expect("Record 3 should be readable");
        assert_eq!(r3.id(), 3);
    }

    // Case C: Magic byte corruption at segment tail
    {
        let rec1 = Record::new(10, OpType::Created, 1, b"rec-10".to_vec());
        let rec2 = Record::new(20, OpType::Created, 1, b"rec-20".to_vec());
        let enc1 = rec1.encode();
        let mut enc2 = rec2.encode();
        enc2[0] = 0x4D; // Corrupt magic byte at tail

        let mut buf = Vec::new();
        buf.extend_from_slice(&enc1);
        buf.extend_from_slice(&enc2);

        fs::write(&wal_path, &buf).unwrap();

        // Active segment open should truncate the tail corrupted record cleanly
        let wal = Wal::open(&dir, None).unwrap();
        assert_eq!(wal.current_size(), enc1.len() as u64);
        assert_eq!(fs::metadata(&wal_path).unwrap().len(), enc1.len() as u64);
    }

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Test 6: Unbounded allocation defense on corrupted payload_len
// -----------------------------------------------------------------------------
#[test]
fn test_corrupted_header_huge_payload_length_alloc_bound() {
    let dir = temp_test_dir("corrupted_huge_payload_len");
    let wal_path = dir.join("nesso.00001.wal");
    let reader = WalReader::new(dir.clone());

    // Frame with payload_len = u32::MAX
    let mut header_u32_max = [0u8; HEADER_SIZE];
    header_u32_max[0] = MAGIC_BYTE;
    header_u32_max[5] = OpType::Created as u8;
    header_u32_max[6] = 1; // priority
    header_u32_max[7..15].copy_from_slice(&100u64.to_be_bytes());
    header_u32_max[15..19].copy_from_slice(&u32::MAX.to_be_bytes());

    // Frame with payload_len = 2 GB (0x80000000)
    let mut header_2gb = [0u8; HEADER_SIZE];
    header_2gb[0] = MAGIC_BYTE;
    header_2gb[5] = OpType::Created as u8;
    header_2gb[6] = 1;
    header_2gb[7..15].copy_from_slice(&200u64.to_be_bytes());
    header_2gb[15..19].copy_from_slice(&(2 * 1024 * 1024 * 1024u32).to_be_bytes());

    let mut buf = Vec::new();
    let off_u32_max = 0u64;
    buf.extend_from_slice(&header_u32_max);
    let off_2gb = buf.len() as u64;
    buf.extend_from_slice(&header_2gb);

    fs::write(&wal_path, &buf).unwrap();

    // 1. WalReader::read_at bounds check
    let res1 = reader.read_at(1, off_u32_max);
    assert!(res1.is_err(), "u32::MAX payload_len must return Err");
    assert_eq!(res1.unwrap_err().kind(), io::ErrorKind::InvalidData);

    let res2 = reader.read_at(1, off_2gb);
    assert!(res2.is_err(), "2 GB payload_len must return Err");
    assert_eq!(res2.unwrap_err().kind(), io::ErrorKind::InvalidData);

    // 2. Record::decode bounds check
    assert!(
        Record::decode(&header_u32_max).is_none(),
        "Record::decode must reject u32::MAX without OOM"
    );
    assert!(
        Record::decode(&header_2gb).is_none(),
        "Record::decode must reject 2 GB without OOM"
    );

    // 3. WalIteratorAll bounds check
    let wal = Wal::open(&dir, None).unwrap();
    let records: Vec<_> = wal
        .iter_all()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        records.is_empty(),
        "Iter should skip corrupted frames without panicking"
    );

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// Test 7: CRC32 single-bit flip matrix across all bytes
// -----------------------------------------------------------------------------
#[test]
fn test_crc32_bit_flip_single_bit_matrix() {
    let rec = Record::new(987654321, OpType::Created, 128, vec![0x37; 50]);
    let encoded = rec.encode();
    assert_eq!(encoded.len(), HEADER_SIZE + 50);

    // Baseline: unmodified buffer decodes cleanly
    let baseline = Record::decode(&encoded).expect("Baseline record must decode");
    assert_eq!(baseline.id(), 987654321);
    assert_eq!(baseline.op_type(), OpType::Created);
    assert_eq!(baseline.priority(), 128);
    assert_eq!(baseline.payload().len(), 50);

    let mut total_flips = 0;
    let mut rejected_flips = 0;

    // Test every bit flip across every byte of the record
    for byte_idx in 0..encoded.len() {
        for bit in 0..8 {
            let mut corrupted = encoded.clone();
            corrupted[byte_idx] ^= 1 << bit;
            total_flips += 1;

            if Record::decode(&corrupted).is_none() {
                rejected_flips += 1;
            }
        }
    }

    assert_eq!(total_flips, encoded.len() * 8);
    assert_eq!(
        rejected_flips, total_flips,
        "100% of single-bit flips across header and payload must be detected and rejected"
    );
}

// -----------------------------------------------------------------------------
// Test 8: Cold reboot exact FIFO replay across interleaved lifecycle states
// -----------------------------------------------------------------------------
#[test]
fn test_cold_reboot_exact_fifo_interleaved_lifecycle() {
    let dir = temp_test_dir("cold_reboot_exact_fifo");

    let priorities = [0u8, 50u8, 128u8, 255u8];

    // Setup: 50 tasks across the 4 priorities with interleaved lifecycle states
    // Group 5: 10 Acked tasks (terminal)
    // Group 4: 10 Nacked tasks (re-enqueued to ready queue)
    // Group 3: 10 Expired leases (expired offline, re-enqueued to ready queue)
    // Group 2: 10 Active leases (long TTL, preserved in leased map)
    // Group 1: 10 Unleased tasks (never leased, ready in queue)
    {
        let engine = Engine::open_with_config(
            &dir,
            GroupCommitConfig {
                sync_mode: SyncMode::Standard,
                max_batch_size: 1,
                batch_window: Duration::from_millis(1),
                idle_commit_threshold: Duration::ZERO,
            },
        )
        .unwrap();

        // Push 50 tasks
        for i in 0..50 {
            let prio = priorities[i % priorities.len()];
            let payload = format!("task-payload-{:03}-prio-{}", i, prio).into_bytes();
            engine.push(payload, prio).unwrap();
        }
        engine.force_flush_group_commit().unwrap();

        // Group 2: Pop 10 tasks with long TTL = 3600s (active lease)
        for _ in 0..10 {
            let _ = engine.pop_and_lease(103, 3600).unwrap().unwrap();
        }

        // Group 3: Pop 10 tasks with short TTL = 1s (will expire offline)
        for _ in 0..10 {
            let _ = engine.pop_and_lease(102, 1).unwrap().unwrap();
        }

        // Group 5: Pop 10 tasks and ACK them (terminal)
        for _ in 0..10 {
            let (rec, _) = engine.pop_and_lease(100, 60).unwrap().unwrap();
            engine.ack(rec.id(), 100).unwrap();
        }

        // Group 4: Pop 10 tasks and NACK them (re-enqueued)
        for _ in 0..10 {
            let (rec, _) = engine.pop_and_lease(101, 60).unwrap().unwrap();
            engine.nack(rec.id(), 101).unwrap();
        }

        // Remaining 10 tasks (Group 1) are untouched in ready queue

        engine.force_flush_group_commit().unwrap();
        engine.shutdown().unwrap();
    }

    // Wait 2 seconds so Group 3 leases expire offline
    thread::sleep(Duration::from_millis(2100));

    // Cold reboot
    let engine = Engine::open(&dir, None).unwrap();

    // Verify Active leases (Group 2): exactly 10 tasks in leased map
    {
        let state = engine.inner.lock().unwrap();
        assert_eq!(
            state.leased.len(),
            10,
            "10 active leases must be preserved across reboot"
        );
        for lease in state.leased.values() {
            assert_eq!(lease.consumer_id, 103);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            assert!(
                lease.expire_timestamp > now,
                "Active lease expire_timestamp must be in the future"
            );
        }
    }

    // Pop all remaining ready tasks:
    // Total ready tasks must be exactly 30 (10 Unleased + 10 Nacked + 10 Expired)
    let mut popped_tasks = Vec::new();
    while let Some((rec, retries)) = engine.pop_and_lease(200, 60).unwrap() {
        popped_tasks.push((rec.id(), rec.priority(), rec.payload().to_vec(), retries));
    }

    assert_eq!(
        popped_tasks.len(),
        30,
        "Exactly 30 ready tasks must be popped (10 unleased, 10 nacked, 10 expired)"
    );

    // Verify priority monotonicity: higher priority tasks must pop first
    for i in 1..popped_tasks.len() {
        let prev_prio = popped_tasks[i - 1].1;
        let curr_prio = popped_tasks[i].1;
        assert!(
            prev_prio >= curr_prio,
            "Priority ordering violated: task {} has prio {} after prio {}",
            popped_tasks[i].0,
            curr_prio,
            prev_prio
        );
    }

    // Verify payload data integrity: each payload must match its format
    for (id, prio, payload, _) in &popped_tasks {
        let payload_str = String::from_utf8(payload.clone()).unwrap();
        assert!(
            payload_str.starts_with("task-payload-"),
            "Payload corrupted for task {}: {}",
            id,
            payload_str
        );
        assert!(
            payload_str.ends_with(&format!("-prio-{}", prio)),
            "Priority mismatch in payload for task {}: {}",
            id,
            payload_str
        );
    }

    // Queue must now be empty
    assert!(engine.pop_and_lease(200, 60).unwrap().is_none());

    engine.shutdown().unwrap();
    let _ = fs::remove_dir_all(&dir);
}
