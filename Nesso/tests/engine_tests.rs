use Nesso::storage::engine::Engine;
use std::env;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::thread;
use std::io;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_wal_path(test_name: &str) -> std::path::PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!("nesso_engine_test_{}_{}.wal", test_name, count));
    let _ = fs::remove_dir_all(&path); let _ = fs::remove_file(&path);
    path
}

#[test]
fn test_happy_path() {
    let path = temp_wal_path("happy");
    let engine = Engine::open(&path).unwrap();
    let id = engine.push(b"test".to_vec(), 1).unwrap();
    let (rec, _) = engine.pop_and_lease(42, 10).unwrap().unwrap();
    engine.ack(rec.id(), 42).unwrap();
    
    let state = engine.inner.lock().unwrap();
    assert!(state.ready_queue.is_empty());
    assert!(state.leased.is_empty());
    assert!(state.data_index.is_empty());
}

#[test]
fn test_priority_ordering() {
    let path = temp_wal_path("priority");
    let engine = Engine::open(&path).unwrap();
    engine.push(b"low".to_vec(), 1).unwrap();
    engine.push(b"high".to_vec(), 10).unwrap();
    engine.push(b"mid".to_vec(), 5).unwrap();
    
    assert_eq!(engine.pop_and_lease(1, 10).unwrap().unwrap().0.payload(), b"high");
    assert_eq!(engine.pop_and_lease(1, 10).unwrap().unwrap().0.payload(), b"mid");
    assert_eq!(engine.pop_and_lease(1, 10).unwrap().unwrap().0.payload(), b"low");
}

#[test]
fn test_fifo_same_priority() {
    let path = temp_wal_path("fifo");
    let engine = Engine::open(&path).unwrap();
    let id1 = engine.push(b"first".to_vec(), 5).unwrap();
    let id2 = engine.push(b"second".to_vec(), 5).unwrap();
    
    assert_eq!(engine.pop_and_lease(1, 10).unwrap().unwrap().0.id(), id1);
    assert_eq!(engine.pop_and_lease(1, 10).unwrap().unwrap().0.id(), id2);
}

#[test]
fn test_nack_retries() {
    let path = temp_wal_path("nack");
    let engine = Engine::open(&path).unwrap();
    let id = engine.push(b"test".to_vec(), 1).unwrap();
    
    let (rec, _) = engine.pop_and_lease(1, 10).unwrap().unwrap();
    engine.nack(rec.id(), 1).unwrap();
    
    let (rec2, _) = engine.pop_and_lease(2, 10).unwrap().unwrap();
    assert_eq!(rec2.id(), id); // Returned to queue
}

#[test]
fn test_dead_letter_max_retries() {
    let path = temp_wal_path("deadletter");
    let engine = Engine::open(&path).unwrap();
    engine.push(b"test".to_vec(), 1).unwrap();
    
    for _ in 0..3 {
        let (rec, _) = engine.pop_and_lease(1, 10).unwrap().unwrap();
        engine.nack(rec.id(), 1).unwrap();
    }
    
    assert!(engine.pop_and_lease(1, 10).unwrap().is_none());
}

#[test]
fn test_wrong_consumer_ack() {
    let path = temp_wal_path("wrong_ack");
    let engine = Engine::open(&path).unwrap();
    let id = engine.push(b"test".to_vec(), 1).unwrap();
    
    engine.pop_and_lease(1, 10).unwrap();
    
    let res = engine.ack(id, 99);
    assert_eq!(res.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
}

#[test]
fn test_ack_unleased_task() {
    let path = temp_wal_path("ack_unleased");
    let engine = Engine::open(&path).unwrap();
    let res = engine.ack(999, 1);
    assert_eq!(res.unwrap_err().kind(), io::ErrorKind::NotFound);
}

#[test]
fn test_pop_empty() {
    let path = temp_wal_path("empty");
    let engine = Engine::open(&path).unwrap();
    assert!(engine.pop_and_lease(1, 10).unwrap().is_none());
}

#[test]
fn test_expiration_background() {
    let path = temp_wal_path("expire_bg");
    let engine = Engine::open(&path).unwrap();
    let id = engine.push(b"test".to_vec(), 1).unwrap();
    
    engine.pop_and_lease(1, 1).unwrap();
    thread::sleep(Duration::from_millis(2500));
    
    let (rec, _) = engine.pop_and_lease(2, 10).unwrap().unwrap();
    assert_eq!(rec.id(), id);
}

#[test]
fn test_recovery_never_leased() {
    let path = temp_wal_path("rec_never");
    {
        let engine = Engine::open(&path).unwrap();
        engine.push(b"test".to_vec(), 1).unwrap();
    }
    {
        let engine = Engine::open(&path).unwrap();
        let state = engine.inner.lock().unwrap();
        assert_eq!(state.ready_queue.len(), 1);
    }
}

#[test]
fn test_recovery_active_lease() {
    let path = temp_wal_path("rec_active");
    {
        let engine = Engine::open(&path).unwrap();
        engine.push(b"test".to_vec(), 1).unwrap();
        engine.pop_and_lease(1, 100).unwrap();
    }
    {
        let engine = Engine::open(&path).unwrap();
        let state = engine.inner.lock().unwrap();
        assert_eq!(state.leased.len(), 1);
        assert!(state.ready_queue.is_empty());
    }
}

#[test]
fn test_recovery_expired_offline() {
    let path = temp_wal_path("rec_exp_off");
    {
        let engine = Engine::open(&path).unwrap();
        engine.push(b"test".to_vec(), 1).unwrap();
        engine.pop_and_lease(1, 1).unwrap();
    }
    
    thread::sleep(Duration::from_millis(1500));
    
    {
        let engine = Engine::open(&path).unwrap();
        let state = engine.inner.lock().unwrap();
        assert_eq!(state.ready_queue.len(), 1);
        assert!(state.leased.is_empty());
    }
}
