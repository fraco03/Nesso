use nesso::storage::engine::Engine;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_test_dir(test_name: &str) -> PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!("nesso_payload_cache_test_{}_{}", test_name, count));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn test_hot_path_payload_cache_hits() {
    let dir = temp_test_dir("hits");
    let engine = Engine::open(&dir, None).unwrap();

    // Push 100 tasks
    for i in 0..100 {
        let payload = format!("task_payload_{}", i).into_bytes();
        engine.push(payload, 5).unwrap();
    }

    // Pop all 100 tasks
    for i in 0..100 {
        let (rec, retries) = engine.pop_and_lease(1, 60).unwrap().expect("expected task");
        assert_eq!(retries, 0);
        let expected_payload = format!("task_payload_{}", i).into_bytes();
        assert_eq!(rec.payload(), expected_payload.as_slice());
    }

    // Check payload cache statistics: all 100 pops must have been direct cache hits (0 misses)
    let (hits, misses, _) = engine.payload_cache_stats();
    assert_eq!(hits, 100, "All 100 tasks should hit the in-memory payload cache");
    assert_eq!(misses, 0, "There should be 0 cache misses for freshly pushed tasks");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cold_read_fallback_after_engine_restart() {
    let dir = temp_test_dir("cold_read");

    // Session 1: Push tasks and drop engine
    {
        let engine = Engine::open(&dir, None).unwrap();
        engine.push(b"cold_payload_alpha".to_vec(), 10).unwrap();
        engine.push(b"cold_payload_beta".to_vec(), 20).unwrap();
        engine.sync().unwrap();
        engine.shutdown().unwrap();
    }

    // Session 2: Reopen engine (payload cache starts empty)
    {
        let engine = Engine::open(&dir, None).unwrap();

        // Check initial cache stats
        let (hits_init, misses_init, _) = engine.payload_cache_stats();
        assert_eq!(hits_init, 0);
        assert_eq!(misses_init, 0);

        // Pop highest priority (beta, priority 20)
        let (rec_beta, _) = engine.pop_and_lease(1, 60).unwrap().expect("expected beta");
        assert_eq!(rec_beta.payload(), b"cold_payload_beta");

        // Pop next priority (alpha, priority 10)
        let (rec_alpha, _) = engine.pop_and_lease(1, 60).unwrap().expect("expected alpha");
        assert_eq!(rec_alpha.payload(), b"cold_payload_alpha");

        // Verify that cold reads triggered fallback to disk (2 misses, 0 hits)
        let (hits, misses, _) = engine.payload_cache_stats();
        assert_eq!(hits, 0, "Cold restart should not have in-memory hits");
        assert_eq!(misses, 2, "Cold restart should fall back to disk read on miss");
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_terminal_eviction_on_ack_and_dead_letter() {
    let dir = temp_test_dir("eviction");
    let engine = Engine::open(&dir, None).unwrap();

    let id1 = engine.push(b"payload_1".to_vec(), 1).unwrap();
    let id2 = engine.push(b"payload_2".to_vec(), 1).unwrap();

    // Verify both are in cache
    {
        let state = engine.inner.lock().unwrap();
        assert_eq!(state.payload_cache.len(), 2);
    }

    // Pop and ack id1
    let (rec1, _) = engine.pop_and_lease(42, 60).unwrap().unwrap();
    assert_eq!(rec1.id(), id1);
    engine.ack(id1, 42).unwrap();

    // id1 should be evicted from payload cache upon ack
    {
        let state = engine.inner.lock().unwrap();
        assert_eq!(state.payload_cache.len(), 1);
    }

    // Pop and nack id2 until dead-lettered (MAX_RETRIES = 3)
    let (rec2, _) = engine.pop_and_lease(42, 60).unwrap().unwrap();
    assert_eq!(rec2.id(), id2);
    engine.nack(id2, 42).unwrap(); // retry 1: re-enqueued, stays in cache
    {
        let state = engine.inner.lock().unwrap();
        assert_eq!(state.payload_cache.len(), 1, "Retried task should remain cached");
    }

    let _ = engine.pop_and_lease(42, 60).unwrap().unwrap();
    engine.nack(id2, 42).unwrap(); // retry 2: re-enqueued, stays in cache

    let _ = engine.pop_and_lease(42, 60).unwrap().unwrap();
    engine.nack(id2, 42).unwrap(); // retry 3: dead-lettered!

    // Upon dead-letter, id2 must be evicted from payload cache
    {
        let state = engine.inner.lock().unwrap();
        assert_eq!(state.payload_cache.len(), 0, "Dead-lettered task must be evicted");
    }

    let _ = fs::remove_dir_all(&dir);
}
