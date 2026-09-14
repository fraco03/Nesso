use nesso::storage::record::{OpType, Record};
use nesso::storage::wal::{Wal, WalReader};
use nesso::storage::engine::{Engine, EngineState};
use nesso::storage::group_commit::GroupCommit;
use std::env;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io::Write;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_wal_dir(test_name: &str) -> std::path::PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!("nesso_wal_seg_test_{}_{}", test_name, count));
    let _ = fs::remove_dir_all(&path); let _ = fs::remove_file(&path);
    path
}

#[test]
fn test_append_exceeding_threshold() {
    let dir = temp_wal_dir("threshold");
    let mut wal = Wal::open(&dir, Some(100)).unwrap(); // Threshold 100 bytes
    
    let rec1 = Record::new(1, OpType::Created, 1, vec![0; 90]); // 90 bytes payload + 19 header = 99 bytes
    let (seg1, _) = wal.append(&rec1).unwrap();
    assert_eq!(seg1, 1);
    
    // This second write should trigger rotation
    let rec2 = Record::new(2, OpType::Created, 1, vec![0; 50]);
    let (seg2, _) = wal.append(&rec2).unwrap();
    assert_eq!(seg2, 2);
}

#[test]
fn test_read_at_past_segment() {
    let dir = temp_wal_dir("read_past");
    let mut wal = Wal::open(&dir, Some(100)).unwrap();
    
    let rec1 = Record::new(1, OpType::Created, 1, vec![1; 90]);
    let (seg1, off1) = wal.append(&rec1).unwrap();
    
    let rec2 = Record::new(2, OpType::Created, 1, vec![2; 90]);
    let (seg2, _off2) = wal.append(&rec2).unwrap(); // Triggers segment 2
    
    assert_eq!(seg1, 1);
    assert_eq!(seg2, 2);
    
    let reader = WalReader::new(dir.clone()); let read_rec = reader.read_at(seg1, off1).unwrap().unwrap();
    assert_eq!(read_rec.id(), 1);
}

#[test]
fn test_iter_all_multiple_segments() {
    let dir = temp_wal_dir("iter_multiple");
    let mut wal = Wal::open(&dir, Some(100)).unwrap();
    
    wal.append(&Record::new(1, OpType::Created, 1, vec![0; 90])).unwrap(); // seg 1
    wal.append(&Record::new(2, OpType::Created, 1, vec![0; 90])).unwrap(); // seg 2
    wal.append(&Record::new(3, OpType::Created, 1, vec![0; 90])).unwrap(); // seg 3
    
    let records: Vec<_> = wal.iter_all().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].0, 1);
    assert_eq!(records[1].0, 2);
    assert_eq!(records[2].0, 3);
}

#[test]
fn test_reopen_wal_detects_highest_id() {
    let dir = temp_wal_dir("reopen");
    {
        let mut wal = Wal::open(&dir, Some(100)).unwrap();
        wal.append(&Record::new(1, OpType::Created, 1, vec![0; 90])).unwrap();
        wal.append(&Record::new(2, OpType::Created, 1, vec![0; 90])).unwrap(); // creates segment 2
    }
    
    {
        let mut wal = Wal::open(&dir, Some(100)).unwrap();
        let rec3 = Record::new(3, OpType::Created, 1, vec![0; 10]);
        let (seg3, _) = wal.append(&rec3).unwrap();
        assert_eq!(seg3, 3); // Should rotate to segment 3 because segment 2 is already at 109 bytes
    }
}

#[test]
fn test_iter_with_truncated_intermediate_segment() {
    let dir = temp_wal_dir("truncated");
    let mut wal = Wal::open(&dir, Some(100)).unwrap();
    wal.append(&Record::new(1, OpType::Created, 1, vec![1; 20])).unwrap();
    
    // Corrupt segment 1 explicitly
    {
        let path = dir.join("nesso.00001.wal");
        let mut f = fs::OpenOptions::new().append(true).open(path).unwrap();
        f.write_all(&[0x00, 0x01]).unwrap(); // Partial write
    }
    
    // Create segment 2 natively
    wal.append(&Record::new(99, OpType::Created, 1, vec![0; 200])).unwrap();
    wal.append(&Record::new(2, OpType::Created, 1, vec![2; 20])).unwrap();
    
    let records: Vec<_> = wal.iter_all().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].0, 1); // From seg 1
    assert_eq!(records[1].0, 2); // From seg 2
}

// Ensure tests compile

#[test]
fn test_engine_e2e_segment_recovery() {
    let dir = temp_wal_dir("e2e_recovery");
    
    {
        // Open Engine with a custom small threshold WAL to force segmentation quickly
        let wal = Wal::open(&dir, Some(150)).unwrap(); 
        let state = EngineState::new(wal);
        let engine = Engine {
            inner: std::sync::Arc::new(std::sync::Mutex::new(state)),
            reader: std::sync::Arc::new(WalReader::new(dir.clone())),
            group_commit: std::sync::Arc::new(GroupCommit::new(Default::default())),
            expiration_worker: Default::default(),
            compaction_lock: Default::default(),
        };
        
        // Push 3 tasks, spanning multiple segments
        engine.push(vec![0; 80], 1).unwrap(); 
        engine.push(vec![0; 80], 1).unwrap(); 
        engine.push(vec![0; 80], 1).unwrap(); 
        
        // Lease one task to mutate state
        engine.pop_and_lease(100, 10).unwrap(); 
    } 
    
    {
        // Reopen with standard 64MB limit, testing cross-segment recovery
        let engine = Engine::open(&dir, None).unwrap();
        let state = engine.inner.lock().unwrap();
        
        assert_eq!(state.ready_queue.len(), 2);
        assert_eq!(state.leased.len(), 1);
        assert_eq!(state.data_index.len(), 3);
        assert_eq!(state.index.len(), 3); 
    }
}

#[test]
fn test_iter_with_corrupt_magic_byte_full_header() {
    let dir = temp_wal_dir("magic_byte");
    let mut wal = Wal::open(&dir, Some(150)).unwrap();
    wal.append(&Record::new(1, OpType::Created, 1, vec![1; 20])).unwrap(); // seg 1
    
    // Corrupt segment 1 explicitly with a FULL header but bad magic byte
    {
        use std::io::Write;
        let path = dir.join("nesso.00001.wal");
        let mut f = fs::OpenOptions::new().append(true).open(path).unwrap();
        let mut bad_record = vec![0u8; 19 + 10]; // 19 header + 10 payload
        bad_record[0] = 0x99; // BAD MAGIC BYTE!
        bad_record[15..19].copy_from_slice(&(10u32).to_be_bytes()); // valid payload len
        f.write_all(&bad_record).unwrap();
    }
    
    // Force rotation by writing more bytes natively
    wal.append(&Record::new(99, OpType::Created, 1, vec![0; 200])).unwrap(); wal.append(&Record::new(2, OpType::Created, 1, vec![2; 20])).unwrap(); // triggers seg 2
    
    // Read via iter_all. It should skip the corrupted part of seg 1, and read seg 2 perfectly.
    let records: Vec<_> = wal.iter_all().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].0, 1);
    assert_eq!(records[1].0, 2);
}

#[test]
fn test_recovery_expires_stale_lease_spanning_segments() {
    let dir = temp_wal_dir("stale_lease_segmented");
    let id = {
        let wal = Wal::open(&dir, Some(100)).unwrap();
        let state = EngineState::new(wal);
        let engine = Engine {
            inner: std::sync::Arc::new(std::sync::Mutex::new(state)),
            reader: std::sync::Arc::new(WalReader::new(dir.clone())),
            group_commit: std::sync::Arc::new(GroupCommit::new(Default::default())),
            expiration_worker: Default::default(),
            compaction_lock: Default::default(),
        };
        
        let id = engine.push(vec![1, 2, 3], 5).unwrap(); // Seg 1
        engine.push(vec![0; 80], 1).unwrap(); // Forces rotation to Seg 2
        engine.pop_and_lease(42, 1).unwrap(); // Leases `id` from Seg 1, but writes Leased event to Seg 2
        id
    }; // Engine and WAL drop here
    
    std::thread::sleep(std::time::Duration::from_millis(1500)); // Lease expires offline
    
    let engine = Engine::open(&dir, None).unwrap(); // Standard recovery
    let state = engine.inner.lock().unwrap();
    
    assert!(!state.leased.contains_key(&id));
    
    // One from the lease recovering into ready, one from the dummy task pushed to force rotation
    assert_eq!(state.ready_queue.len(), 2); 
}
