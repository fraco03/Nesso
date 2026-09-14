use nesso::storage::engine::Engine;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_test_dir(test_name: &str) -> PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!("nesso_pqueue_test_{}_{}", test_name, count));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn test_strict_priority_dispatch_ordering() {
    let dir = temp_test_dir("strict_priority");
    let engine = Engine::open(&dir, None).unwrap();

    // Push tasks with varying priorities
    let p_list = [10u8, 255u8, 0u8, 128u8, 50u8, 255u8, 0u8, 128u8];
    for (i, &p) in p_list.iter().enumerate() {
        let payload = format!("task_{}_{}", p, i).into_bytes();
        engine.push(payload, p).unwrap();
    }

    let mut popped_priorities = Vec::new();
    while let Ok(Some((rec, _))) = engine.pop_and_lease(1, 60) {
        popped_priorities.push(rec.priority());
        engine.ack(rec.id(), 1).unwrap();
    }

    // Must be strictly monotonically non-increasing (descending priority order)
    assert_eq!(popped_priorities, vec![255, 255, 128, 128, 50, 10, 0, 0]);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_concurrent_priority_producers_and_consumers() {
    let dir = temp_test_dir("concurrent_pq");
    let engine = Arc::new(Engine::open(&dir, None).unwrap());

    let num_producers = 4;
    let num_consumers = 4;
    let items_per_producer = 250;

    let mut handles = Vec::new();

    // Spawn producers
    for p_id in 0..num_producers {
        let eng = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for i in 0..items_per_producer {
                let priority = ((p_id * 50 + i) % 256) as u8;
                let payload = format!("prod_{}_{}", p_id, i).into_bytes();
                eng.push(payload, priority).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Total tasks should be exactly num_producers * items_per_producer
    let (ready, leased) = engine.status();
    assert_eq!(ready, num_producers * items_per_producer);
    assert_eq!(leased, 0);

    // Spawn consumers to pop and ack
    let mut consumer_handles = Vec::new();
    let total_popped = Arc::new(AtomicUsize::new(0));

    for c_id in 0..num_consumers {
        let eng = Arc::clone(&engine);
        let ctr = Arc::clone(&total_popped);
        consumer_handles.push(thread::spawn(move || {
            while let Ok(Some((rec, _))) = eng.pop_and_lease(c_id as u32, 60) {
                eng.ack(rec.id(), c_id as u32).unwrap();
                ctr.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in consumer_handles {
        h.join().unwrap();
    }

    assert_eq!(
        total_popped.load(Ordering::SeqCst),
        num_producers * items_per_producer
    );
    let (ready_end, leased_end) = engine.status();
    assert_eq!(ready_end, 0);
    assert_eq!(leased_end, 0);

    let _ = fs::remove_dir_all(&dir);
}
