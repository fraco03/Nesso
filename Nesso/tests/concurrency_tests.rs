use Nesso::storage::engine::Engine;
use std::env;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_wal_path(test_name: &str) -> std::path::PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!("nesso_conc_test_{}_{}.wal", test_name, count));
    let _ = fs::remove_dir_all(&path); let _ = fs::remove_file(&path);
    path
}

#[test]
fn test_concurrent_push() {
    let path = temp_wal_path("push");
    let engine = Arc::new(Engine::open(&path).unwrap());
    
    let mut handles = vec![];
    let num_threads = 10;
    let items_per_thread = 100;
    
    for _ in 0..num_threads {
        let engine_clone = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for _ in 0..items_per_thread {
                engine_clone.push(b"data".to_vec(), 1).unwrap();
            }
        }));
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let state = engine.inner.lock().unwrap();
    assert_eq!(state.ready_queue.len(), num_threads * items_per_thread);
}

#[test]
fn test_no_double_delivery() {
    let path = temp_wal_path("delivery");
    let engine = Arc::new(Engine::open(&path).unwrap());
    
    let total_items = 1000;
    for _ in 0..total_items {
        engine.push(b"data".to_vec(), 1).unwrap();
    }
    
    let consumed_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    let num_threads = 5;
    
    for i in 0..num_threads {
        let engine_clone = Arc::clone(&engine);
        let count_clone = Arc::clone(&consumed_count);
        handles.push(thread::spawn(move || {
            loop {
                match engine_clone.pop_and_lease(i as u32, 10) {
                    Ok(Some((rec, _))) => {
                        count_clone.fetch_add(1, Ordering::SeqCst);
                        engine_clone.ack(rec.id(), i as u32).unwrap();
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }));
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    assert_eq!(consumed_count.load(Ordering::SeqCst), total_items as usize);
}

#[test]
fn test_mutex_poison_recovery_simulated() {
    let path = temp_wal_path("poison");
    let engine = Engine::open(&path).unwrap();
    engine.push(b"data".to_vec(), 1).unwrap();
    
    // An application error in pop should not poison the lock (we use safe unwrapping now)
    // To explicitly test lock availability after errors:
    assert!(engine.ack(999, 1).is_err()); // Error returned safely
    assert!(engine.pop_and_lease(1, 10).unwrap().is_some()); // Engine still fully functional
}

#[test]
fn test_mpmc_chaos_stress() {
    let path = temp_wal_path("mpmc_chaos");
    let engine = Arc::new(Engine::open(&path).unwrap());

    let num_producers = 4;
    let tasks_per_producer = 250;
    let num_consumers = 4;

    let producers_done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut producer_handles = vec![];
    for _ in 0..num_producers {
        let engine_clone = Arc::clone(&engine);
        producer_handles.push(thread::spawn(move || {
            for i in 0..tasks_per_producer {
                // Mix of priorities
                let priority = (i % 3) as u8;
                engine_clone.push(b"chaos".to_vec(), priority).unwrap();
                
                // Slight delay to heavily interleave pushes and pops
                if i % 10 == 0 {
                    thread::sleep(std::time::Duration::from_micros(10));
                }
            }
        }));
    }

    let mut consumer_handles = vec![];
    for c in 0..num_consumers {
        let engine_clone = Arc::clone(&engine);
        let done_flag = Arc::clone(&producers_done);
        consumer_handles.push(thread::spawn(move || {
            let consumer_id = c + 100;
            let mut empty_streak = 0;
            
            loop {
                match engine_clone.pop_and_lease(consumer_id, 5) {
                    Ok(Some((rec, _))) => {
                        empty_streak = 0;
                        
                        // Deterministically simulate some failures based on ID
                        if rec.id() % 4 == 0 {
                            engine_clone.nack(rec.id(), consumer_id).unwrap();
                        } else {
                            engine_clone.ack(rec.id(), consumer_id).unwrap();
                        }
                    }
                    Ok(None) => {
                        empty_streak += 1;
                        // Stop if producers finished and queue seems consistently empty
                        if done_flag.load(Ordering::SeqCst) && empty_streak > 50 {
                            break;
                        }
                        thread::sleep(std::time::Duration::from_millis(2));
                    }
                    Err(_) => panic!("Engine error during pop"),
                }
            }
        }));
    }

    for h in producer_handles {
        h.join().unwrap();
    }
    
    producers_done.store(true, Ordering::SeqCst);

    for h in consumer_handles {
        h.join().unwrap();
    }

    let state = engine.inner.lock().unwrap();
    assert!(state.ready_queue.is_empty(), "Queue should be empty");
    assert!(state.leased.is_empty(), "No active leases should remain");
    assert!(state.data_index.is_empty(), "All payloads should be cleaned up");
}

#[test]
fn test_concurrent_append_with_rotation() {
    let path = temp_wal_path("concurrent_rotation");
    
    // Open Engine with a custom small threshold WAL (10 KB) to force heavy rotation under concurrency
    let wal = Nesso::storage::wal::Wal::open(&path, Some(10 * 1024)).unwrap();
    let state = Nesso::storage::engine::EngineState {
        wal,
        next_id: 1,
        index: std::collections::HashMap::new(),
        data_index: std::collections::HashMap::new(),
        ready_queue: std::collections::BinaryHeap::new(),
        leased: std::collections::HashMap::new(),
    };
    let engine = std::sync::Arc::new(Nesso::storage::engine::Engine { reader: std::sync::Arc::new(Nesso::storage::wal::WalReader::new(path.clone())), inner: std::sync::Arc::new(std::sync::Mutex::new(state)) });

    let num_threads = 10;
    let pushes_per_thread = 100;
    let mut handles = vec![];

    for t in 0..num_threads {
        let engine_clone = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for i in 0..pushes_per_thread {
                let payload = format!("thread_{}_msg_{}", t, i).into_bytes(); // approx 20 bytes
                // 1000 total pushes * (19 + 20 bytes) = ~39 KB -> Should create ~4 segments
                engine_clone.push(payload, 1).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let state = engine.inner.lock().unwrap();
    assert_eq!(state.ready_queue.len(), num_threads * pushes_per_thread);
    assert_eq!(state.index.len(), num_threads * pushes_per_thread);
}
