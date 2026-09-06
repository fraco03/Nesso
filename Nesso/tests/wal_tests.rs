use Nesso::storage::record::{OpType, Record};
use Nesso::storage::wal::{Wal, WalReader};
use std::env;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io::Write;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_wal_dir(test_name: &str) -> std::path::PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!("nesso_wal_dir_test_{}_{}", test_name, count));
    let _ = fs::remove_dir_all(&path); let _ = fs::remove_file(&path);
    path
}

#[test]
fn test_append_and_read_at() {
    let path = temp_wal_dir("append_read");
    let mut wal = Wal::open(&path, None).unwrap();
    
    let rec = Record::new(1, OpType::Created, 5, b"data".to_vec());
    let (seg, offset) = wal.append(&rec).unwrap();
    
    let reader = WalReader::new(path.clone()); let read_rec = reader.read_at(seg, offset).unwrap().unwrap();
    assert_eq!(read_rec.id(), 1);
    assert_eq!(read_rec.payload(), b"data");
}

#[test]
fn test_multiple_append_and_read() {
    let path = temp_wal_dir("multiple_append");
    let mut wal = Wal::open(&path, None).unwrap();
    
    let mut offsets = Vec::new();
    for i in 0..5 {
        let rec = Record::new(i, OpType::Created, 1, format!("data{}", i).into_bytes());
        offsets.push(wal.append(&rec).unwrap());
    }
    
    for (i, &(seg, offset)) in offsets.iter().enumerate() {
        let reader = WalReader::new(path.clone()); let rec = reader.read_at(seg, offset).unwrap().unwrap();
        assert_eq!(rec.id(), i as u64);
        assert_eq!(rec.payload(), format!("data{}", i).as_bytes());
    }
}

#[test]
fn test_iter_all_empty_dir() {
    let path = temp_wal_dir("iter_empty");
    let wal = Wal::open(&path, None).unwrap();
    
    let mut iter = wal.iter_all().unwrap();
    assert!(iter.next().is_none());
}

#[test]
fn test_iter_all_valid_records() {
    let path = temp_wal_dir("iter_valid");
    let mut wal = Wal::open(&path, None).unwrap();
    
    for i in 0..3 {
        wal.append(&Record::new(i, OpType::Created, 1, b"hi".to_vec())).unwrap();
    }
    
    let iter = wal.iter_all().unwrap();
    let records: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
    
    assert_eq!(records.len(), 3);
    for (i, (_, _, rec)) in records.iter().enumerate() {
        assert_eq!(rec.id(), i as u64);
    }
}
