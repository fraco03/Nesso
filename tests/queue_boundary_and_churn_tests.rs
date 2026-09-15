use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use axum::serve;
use base64::prelude::*;
use base64::Engine as _;
use serde_json::json;
use tokio::net::TcpListener;

use nesso::storage::engine::{Engine, EngineState, GroupCommit, TaskRef};
use nesso::storage::priority_queue::PriorityBucketQueue;
use nesso::storage::wal::{Wal, WalReader};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_test_dir(test_name: &str) -> PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!(
        "nesso_q_boundary_{}_{}_{}",
        test_name,
        std::process::id(),
        count
    ));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

fn wait_for_lease_expiration(engine: &Engine, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let (_ready, leased) = engine.status();
        if leased == 0 {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    false
}

async fn spawn_test_server(data_dir: PathBuf) -> String {
    let router = nesso::server::create_router(data_dir);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        serve(listener, router).await.unwrap();
    });
    format!("http://{}", addr)
}

// =========================================================================
// SECTION 1: Discrete Priority Queue & Bitmap Boundaries (Tests 1–5)
// =========================================================================

/// Test 1: Full discrete priority spectrum (0..=255) monotonicity and FIFO order.
/// Pushes all 256 priorities in reverse order, then a second permutation of 256 priorities.
/// Verifies that popped tasks are strictly monotonically non-increasing (255 down to 0)
/// and that tasks sharing identical priorities preserve strict FIFO insertion ordering.
#[test]
fn test_priority_spectrum_full_gamut_monotonicity() {
    let dir = temp_test_dir("spectrum_monotonicity");
    let engine = Engine::open(&dir, None).unwrap();

    // Round 1: Push 0..=255 in ascending order (lowest priority first)
    for p in 0..=255u8 {
        let payload = format!("r1_p_{}", p).into_bytes();
        engine.push(payload, p).unwrap();
    }

    // Round 2: Push 0..=255 in a deterministic permutation: p = (i * 101 + 37) % 256
    // Since gcd(101, 256) == 1, this touches every priority in 0..=255 exactly once.
    for i in 0..256 {
        let p = ((i * 101 + 37) % 256) as u8;
        let payload = format!("r2_p_{}", p).into_bytes();
        engine.push(payload, p).unwrap();
    }

    let (ready, leased) = engine.status();
    assert_eq!(ready, 512);
    assert_eq!(leased, 0);

    let mut popped_priorities = Vec::with_capacity(512);
    let mut round1_popped = [false; 256];

    while let Ok(Some((rec, _))) = engine.pop_and_lease(1, 60) {
        let p = rec.priority();
        popped_priorities.push(p);

        let payload_str = String::from_utf8(rec.payload().to_vec()).unwrap();
        if payload_str.starts_with("r1_") {
            round1_popped[p as usize] = true;
        } else if payload_str.starts_with("r2_") {
            // Strict FIFO guarantee: Round 1 task at priority p MUST pop before Round 2 task at priority p
            assert!(
                round1_popped[p as usize],
                "FIFO violation: Round 2 task popped before Round 1 task at priority {}",
                p
            );
        } else {
            panic!("Unexpected payload prefix: {}", payload_str);
        }

        engine.ack(rec.id(), 1).unwrap();
    }

    assert_eq!(popped_priorities.len(), 512);

    // Verify non-increasing order: each popped priority must be <= previous popped priority
    for i in 1..popped_priorities.len() {
        assert!(
            popped_priorities[i - 1] >= popped_priorities[i],
            "Priority inversion detected: popped[{}] = {} followed by popped[{}] = {}",
            i - 1,
            popped_priorities[i - 1],
            i,
            popped_priorities[i]
        );
    }

    // Verify exactly two tasks per priority, from 255 down to 0
    let mut expected_sequence = Vec::with_capacity(512);
    for p in (0..=255u8).rev() {
        expected_sequence.push(p);
        expected_sequence.push(p);
    }
    assert_eq!(popped_priorities, expected_sequence);

    let (ready_end, leased_end) = engine.status();
    assert_eq!(ready_end, 0);
    assert_eq!(leased_end, 0);

    let _ = fs::remove_dir_all(&dir);
}

/// Test 2: Hardware bitmap word boundaries (63/64, 127/128, 191/192) and bit clearing isolation.
/// Tests that transitions across u64 words:
/// - Word 0 (bits 0..=63) -> Word 1 (bits 64..=127) at boundary 63/64
/// - Word 1 (bits 64..=127) -> Word 2 (bits 128..=191) at boundary 127/128
/// - Word 2 (bits 128..=191) -> Word 3 (bits 192..=255) at boundary 191/192
/// - Extremes at 0 (Word 0 bit 0) and 255 (Word 3 bit 63)
/// function with exact bit manipulation, proper precedence, and zero bit-mask bleed.
#[test]
fn test_hardware_bitmap_word_boundaries_exact_bits() {
    // Part A: Test PriorityBucketQueue data structure directly
    {
        let mut pq = PriorityBucketQueue::new();
        let boundary_pairs = [(63u8, 64u8), (127u8, 128u8), (191u8, 192u8), (0u8, 255u8)];

        for &(low, high) in &boundary_pairs {
            // Push low then high
            pq.push(TaskRef { id: 1, priority: low, retries: 0 });
            pq.push(TaskRef { id: 2, priority: high, retries: 0 });

            // High must pop first
            let t1 = pq.pop().expect("high task must pop first");
            assert_eq!(t1.priority, high);
            assert_eq!(t1.id, 2);

            // Low must pop second
            let t2 = pq.pop().expect("low task must pop second");
            assert_eq!(t2.priority, low);
            assert_eq!(t2.id, 1);

            assert!(pq.pop().is_none());

            // Reverse push order: high then low
            pq.push(TaskRef { id: 10, priority: high, retries: 0 });
            pq.push(TaskRef { id: 20, priority: low, retries: 0 });

            let t3 = pq.pop().expect("high task must pop first");
            assert_eq!(t3.priority, high);
            assert_eq!(t3.id, 10);

            let t4 = pq.pop().expect("low task must pop second");
            assert_eq!(t4.priority, low);
            assert_eq!(t4.id, 20);

            assert!(pq.pop().is_none());
        }

        // Test multi-bit clusters spanning word boundaries:
        // [62, 63, 64, 65], [126, 127, 128, 129], [190, 191, 192, 193]
        let cluster = [
            62u8, 63, 64, 65,
            126, 127, 128, 129,
            190, 191, 192, 193,
        ];

        for &p in &cluster {
            pq.push(TaskRef { id: p as u64, priority: p, retries: 0 });
        }

        let mut expected_descending = cluster.to_vec();
        expected_descending.sort_by(|a, b| b.cmp(a));

        for &expected_p in &expected_descending {
            let task = pq.pop().unwrap();
            assert_eq!(task.priority, expected_p);
            assert_eq!(task.id, expected_p as u64);
        }

        assert!(pq.is_empty());
        assert_eq!(pq.len(), 0);
    }

    // Part B: Test word boundaries end-to-end via Engine with WAL persistence
    {
        let dir = temp_test_dir("bitmap_boundaries_engine");
        let engine = Engine::open(&dir, None).unwrap();

        // Push across all word boundary transitions
        let test_priorities = [0u8, 63, 64, 127, 128, 191, 192, 255];
        for &p in &test_priorities {
            let payload = format!("boundary_{}", p).into_bytes();
            engine.push(payload, p).unwrap();
        }

        let mut popped = Vec::new();
        while let Ok(Some((rec, _))) = engine.pop_and_lease(42, 60) {
            popped.push(rec.priority());
            engine.ack(rec.id(), 42).unwrap();
        }

        assert_eq!(popped, vec![255, 192, 191, 128, 127, 64, 63, 0]);

        let (ready, leased) = engine.status();
        assert_eq!(ready, 0);
        assert_eq!(leased, 0);

        let _ = fs::remove_dir_all(&dir);
    }
}

/// Test 3: Empty queue boundary conditions and drain idempotence.
/// Verifies that:
/// - Calling pop on an empty queue returns None repeatedly without panic or state corruption.
/// - Draining a populated queue to 0 leaves it clean and subsequent pops return None.
/// - Re-pushing tasks after a complete drain restores normal queue operation.
#[test]
fn test_empty_queue_and_drain_boundary_conditions() {
    let dir = temp_test_dir("empty_and_drain");
    let engine = Engine::open(&dir, None).unwrap();

    // 1. Calling pop on freshly initialized empty queue
    for _ in 0..100 {
        let res = engine.pop_and_lease(1, 10).unwrap();
        assert!(res.is_none(), "Expected None on empty queue pop");
    }
    assert_eq!(engine.status(), (0, 0));

    // 2. Push 10 tasks across diverse priorities
    for i in 0..10 {
        let p = (i * 25) as u8;
        engine.push(format!("payload_{}", i).into_bytes(), p).unwrap();
    }
    assert_eq!(engine.status(), (10, 0));

    // 3. Drain all 10 tasks
    let mut drained_count = 0;
    while let Ok(Some((rec, _))) = engine.pop_and_lease(1, 30) {
        engine.ack(rec.id(), 1).unwrap();
        drained_count += 1;
    }
    assert_eq!(drained_count, 10);
    assert_eq!(engine.status(), (0, 0));

    // 4. Calling pop after full drain must return None idempotently
    for _ in 0..100 {
        let res = engine.pop_and_lease(1, 10).unwrap();
        assert!(res.is_none(), "Expected None after full drain");
    }
    assert_eq!(engine.status(), (0, 0));

    // 5. Re-pushing after drain must resume normal queue operations
    let re_id = engine.push(b"re_pushed".to_vec(), 200).unwrap();
    assert_eq!(engine.status(), (1, 0));

    let (rec, _) = engine.pop_and_lease(1, 10).unwrap().expect("re-pushed task must pop");
    assert_eq!(rec.id(), re_id);
    assert_eq!(rec.payload(), b"re_pushed");
    engine.ack(rec.id(), 1).unwrap();

    assert_eq!(engine.status(), (0, 0));
    assert!(engine.pop_and_lease(1, 10).unwrap().is_none());

    let _ = fs::remove_dir_all(&dir);
}

/// Test 4: Alternating priority traffic patterns.
/// Simulates high-frequency interleaving of pushes and pops across wildly different priorities
/// (e.g. 0, 255, 128, 64, 0, 255) to verify bitmap updates and FIFO queues remain coherent.
#[test]
fn test_alternating_priority_traffic_patterns() {
    let dir = temp_test_dir("alternating_priority");
    let engine = Engine::open(&dir, None).unwrap();

    // Specific sequence from specification:
    // push(0), push(255) -> pop gives 255
    let id_0 = engine.push(b"task_0".to_vec(), 0).unwrap();
    let id_255a = engine.push(b"task_255a".to_vec(), 255).unwrap();

    let (rec1, _) = engine.pop_and_lease(1, 60).unwrap().unwrap();
    assert_eq!(rec1.id(), id_255a);
    assert_eq!(rec1.priority(), 255);
    engine.ack(rec1.id(), 1).unwrap();

    // push(128) -> pop gives 128
    let id_128 = engine.push(b"task_128".to_vec(), 128).unwrap();
    let (rec2, _) = engine.pop_and_lease(1, 60).unwrap().unwrap();
    assert_eq!(rec2.id(), id_128);
    assert_eq!(rec2.priority(), 128);
    engine.ack(rec2.id(), 1).unwrap();

    // push(64) -> pop gives 64
    let id_64 = engine.push(b"task_64".to_vec(), 64).unwrap();
    let (rec3, _) = engine.pop_and_lease(1, 60).unwrap().unwrap();
    assert_eq!(rec3.id(), id_64);
    assert_eq!(rec3.priority(), 64);
    engine.ack(rec3.id(), 1).unwrap();

    // push(255) -> pop gives 255
    let id_255b = engine.push(b"task_255b".to_vec(), 255).unwrap();
    let (rec4, _) = engine.pop_and_lease(1, 60).unwrap().unwrap();
    assert_eq!(rec4.id(), id_255b);
    assert_eq!(rec4.priority(), 255);
    engine.ack(rec4.id(), 1).unwrap();

    // Interleave multiple priorities: 192, 127, 191, 63
    let id_192 = engine.push(b"task_192".to_vec(), 192).unwrap();
    let id_127 = engine.push(b"task_127".to_vec(), 127).unwrap();
    let id_191 = engine.push(b"task_191".to_vec(), 191).unwrap();
    let id_63 = engine.push(b"task_63".to_vec(), 63).unwrap();

    // Now pop the remaining 5 tasks in strict priority order:
    // 192, 191, 127, 63, 0 (the very first pushed task)
    let expected_rem = [
        (id_192, 192),
        (id_191, 191),
        (id_127, 127),
        (id_63, 63),
        (id_0, 0),
    ];

    for (exp_id, exp_p) in expected_rem {
        let (rec, _) = engine.pop_and_lease(1, 60).unwrap().expect("task must exist");
        assert_eq!(rec.id(), exp_id);
        assert_eq!(rec.priority(), exp_p);
        engine.ack(rec.id(), 1).unwrap();
    }

    assert_eq!(engine.status(), (0, 0));

    // Dynamic zig-zag traffic pattern:
    // 100 cycles:
    // - push low (priority 15)
    // - push high (priority 225)
    // - pop (must be priority 225)
    // - push mid (priority 115)
    // - pop (must be priority 115)
    for i in 0..100 {
        let p_low_id = engine.push(format!("low_{}", i).into_bytes(), 15).unwrap();
        let p_high_id = engine.push(format!("high_{}", i).into_bytes(), 225).unwrap();

        let (popped_high, _) = engine.pop_and_lease(1, 60).unwrap().unwrap();
        assert_eq!(popped_high.id(), p_high_id);
        assert_eq!(popped_high.priority(), 225);
        engine.ack(popped_high.id(), 1).unwrap();

        let p_mid_id = engine.push(format!("mid_{}", i).into_bytes(), 115).unwrap();

        let (popped_mid, _) = engine.pop_and_lease(1, 60).unwrap().unwrap();
        assert_eq!(popped_mid.id(), p_mid_id);
        assert_eq!(popped_mid.priority(), 115);
        engine.ack(popped_mid.id(), 1).unwrap();

        let _ = p_low_id;
    }

    // After 100 cycles, exactly 100 low tasks (priority 15) must remain in strict FIFO order
    let (ready, leased) = engine.status();
    assert_eq!(ready, 100);
    assert_eq!(leased, 0);

    for i in 0..100 {
        let (rec, _) = engine.pop_and_lease(1, 60).unwrap().unwrap();
        assert_eq!(rec.priority(), 15);
        let payload = String::from_utf8(rec.payload().to_vec()).unwrap();
        assert_eq!(payload, format!("low_{}", i));
        engine.ack(rec.id(), 1).unwrap();
    }

    assert_eq!(engine.status(), (0, 0));
    let _ = fs::remove_dir_all(&dir);
}

/// Test 5: Dense FIFO integrity within identical priorities across gamut.
/// Tests that across all 8 boundary priorities [0, 63, 64, 127, 128, 191, 192, 255],
/// 50 sequential tasks per priority preserve 100% strict FIFO ordering upon extraction.
#[test]
fn test_fifo_integrity_within_identical_priorities() {
    let dir = temp_test_dir("fifo_within_priorities");
    let engine = Engine::open(&dir, None).unwrap();

    let priorities = [0u8, 63, 64, 127, 128, 191, 192, 255];
    let tasks_per_priority = 50;

    // Push 50 tasks for each priority
    for &p in &priorities {
        for seq in 0..tasks_per_priority {
            let payload = format!("p_{}_seq_{}", p, seq).into_bytes();
            engine.push(payload, p).unwrap();
        }
    }

    assert_eq!(engine.status(), (priorities.len() * tasks_per_priority, 0));

    // Extraction: Must pop highest priorities first (255 down to 0).
    // Within each priority, sequences must be strictly 0..50.
    for &p in priorities.iter().rev() {
        for expected_seq in 0..tasks_per_priority {
            let (rec, _) = engine.pop_and_lease(1, 60).unwrap().expect("task must exist");
            assert_eq!(rec.priority(), p, "Unexpected priority popped");

            let expected_payload = format!("p_{}_seq_{}", p, expected_seq);
            let actual_payload = String::from_utf8(rec.payload().to_vec()).unwrap();
            assert_eq!(
                actual_payload, expected_payload,
                "FIFO violation at priority {}: expected seq {} but got {}",
                p, expected_seq, actual_payload
            );

            engine.ack(rec.id(), 1).unwrap();
        }
    }

    assert_eq!(engine.status(), (0, 0));
    let _ = fs::remove_dir_all(&dir);
}

// =========================================================================
// SECTION 2: Lease Lifecycle, Expiration & Concurrency (Tests 6–9)
// =========================================================================

/// Test 6: Late ACK and Late NACK rejection after lease expiration and reassignment.
/// Verifies that:
/// - Consumer 1 leases Task 1 with TTL = 1s.
/// - Task 1 expires and is returned to ready queue.
/// - Consumer 2 leases Task 1.
/// - Consumer 1's late ACK and late NACK are strictly rejected with PermissionDenied.
/// - Consumer 2 can successfully ACK Task 1.
/// - Duplicate ACKs, duplicate NACKs, and ACKs on non-existent tasks return NotFound.
#[test]
fn test_lease_late_ack_and_nack_rejection() {
    let dir = temp_test_dir("late_ack_rejection");
    let engine = Engine::open(&dir, None).unwrap();

    let task_id = engine.push(b"important_job".to_vec(), 10).unwrap();

    // 1. Consumer 1 leases Task 1 with TTL = 1s
    let (rec, retries) = engine.pop_and_lease(101, 1).unwrap().unwrap();
    assert_eq!(rec.id(), task_id);
    assert_eq!(retries, 0);

    assert_eq!(engine.status(), (0, 1));

    // 2. Wait for lease to expire via background expiration worker
    let expired = wait_for_lease_expiration(&engine, Duration::from_secs(5));
    assert!(expired, "Lease did not expire within timeout");

    // Once expired, task is back in ready queue with retries = 1
    assert_eq!(engine.status(), (1, 0));

    // 3. Consumer 2 leases Task 1
    let (rec2, retries2) = engine.pop_and_lease(102, 60).unwrap().unwrap();
    assert_eq!(rec2.id(), task_id);
    assert_eq!(retries2, 1);
    assert_eq!(engine.status(), (0, 1));

    // 4. Consumer 1 attempts late ACK -> MUST return PermissionDenied
    let ack_res = engine.ack(task_id, 101);
    assert!(ack_res.is_err());
    assert_eq!(
        ack_res.unwrap_err().kind(),
        io::ErrorKind::PermissionDenied,
        "Expected PermissionDenied on late ACK from expired consumer"
    );

    // 5. Consumer 1 attempts late NACK -> MUST return PermissionDenied
    let nack_res = engine.nack(task_id, 101);
    assert!(nack_res.is_err());
    assert_eq!(
        nack_res.unwrap_err().kind(),
        io::ErrorKind::PermissionDenied,
        "Expected PermissionDenied on late NACK from expired consumer"
    );

    // 6. Consumer 2 (current valid lease holder) calls ACK -> MUST succeed
    let valid_ack = engine.ack(task_id, 102);
    assert!(valid_ack.is_ok(), "Valid lease holder ACK should succeed");

    assert_eq!(engine.status(), (0, 0));

    // 7. Subsequent ACK or NACK on completed task -> MUST return NotFound
    let dup_ack = engine.ack(task_id, 102);
    assert_eq!(dup_ack.unwrap_err().kind(), io::ErrorKind::NotFound);

    let dup_nack = engine.nack(task_id, 102);
    assert_eq!(dup_nack.unwrap_err().kind(), io::ErrorKind::NotFound);

    // 8. ACK or NACK on non-existent task ID -> MUST return NotFound
    let non_exist_ack = engine.ack(99999, 102);
    assert_eq!(non_exist_ack.unwrap_err().kind(), io::ErrorKind::NotFound);

    let non_exist_nack = engine.nack(99999, 102);
    assert_eq!(non_exist_nack.unwrap_err().kind(), io::ErrorKind::NotFound);

    let _ = fs::remove_dir_all(&dir);
}

/// Test 7: Lease DeadLettering after MAX_RETRIES (3 retries).
/// Tests that:
/// - A task expiring 3 consecutive times transitions to DeadLettered and is dropped from active state.
/// - A task nack'd 3 consecutive times transitions to DeadLettered.
/// - DeadLettered tasks are never resurrected after engine restart (cold reboot).
#[test]
fn test_lease_dead_letter_after_max_retries() {
    let dir = temp_test_dir("dead_letter_max_retries");

    // Part A: DeadLetter via 3 consecutive background expirations
    {
        let engine = Engine::open(&dir, None).unwrap();
        let id_exp = engine.push(b"expire_me".to_vec(), 10).unwrap();

        // Round 1: Lease (retry 0) -> expire -> retry 1
        let (r1, retries1) = engine.pop_and_lease(1, 1).unwrap().unwrap();
        assert_eq!(r1.id(), id_exp);
        assert_eq!(retries1, 0);
        assert!(wait_for_lease_expiration(&engine, Duration::from_secs(5)));
        assert_eq!(engine.status(), (1, 0));

        // Round 2: Lease (retry 1) -> expire -> retry 2
        let (r2, retries2) = engine.pop_and_lease(2, 1).unwrap().unwrap();
        assert_eq!(r2.id(), id_exp);
        assert_eq!(retries2, 1);
        assert!(wait_for_lease_expiration(&engine, Duration::from_secs(5)));
        assert_eq!(engine.status(), (1, 0));

        // Round 3: Lease (retry 2) -> expire -> retry 3 (>= MAX_RETRIES) -> DeadLettered!
        let (r3, retries3) = engine.pop_and_lease(3, 1).unwrap().unwrap();
        assert_eq!(r3.id(), id_exp);
        assert_eq!(retries3, 2);
        assert!(wait_for_lease_expiration(&engine, Duration::from_secs(5)));

        // Queue and active leases must now be completely empty!
        assert_eq!(engine.status(), (0, 0));
        assert!(engine.pop_and_lease(4, 10).unwrap().is_none());

        // Part B: DeadLetter via 3 consecutive NACKs
        let id_nack = engine.push(b"nack_me".to_vec(), 10).unwrap();

        for expected_retry in 0..3u8 {
            let (rn, retries) = engine.pop_and_lease(10, 10).unwrap().unwrap();
            assert_eq!(rn.id(), id_nack);
            assert_eq!(retries, expected_retry);
            engine.nack(rn.id(), 10).unwrap();
        }

        // After 3rd NACK, task is DeadLettered
        assert_eq!(engine.status(), (0, 0));
        assert!(engine.pop_and_lease(10, 10).unwrap().is_none());

        engine.sync().unwrap();
    }

    // Part C: Cold reboot - verify dead lettered tasks are never resurrected
    {
        let engine = Engine::open(&dir, None).unwrap();
        assert_eq!(
            engine.status(),
            (0, 0),
            "DeadLettered tasks must not be resurrected on recovery"
        );
        assert!(engine.pop_and_lease(1, 10).unwrap().is_none());

        // New task push and pop works cleanly
        let new_id = engine.push(b"fresh_task".to_vec(), 5).unwrap();
        assert_eq!(engine.status(), (1, 0));
        let (rec, _) = engine.pop_and_lease(1, 10).unwrap().unwrap();
        assert_eq!(rec.id(), new_id);
        engine.ack(rec.id(), 1).unwrap();
        assert_eq!(engine.status(), (0, 0));
    }

    let _ = fs::remove_dir_all(&dir);
}

/// Test 8: Concurrent lease expiration race.
/// Induces races between worker threads acknowledging tasks and the background expiration thread.
/// Asserts that no task is double-acknowledged and the engine state remains consistent.
#[test]
fn test_concurrent_lease_expiration_race() {
    let dir = temp_test_dir("expiration_race");
    let engine = Arc::new(Engine::open(&dir, None).unwrap());

    let num_tasks = 40;
    for i in 0..num_tasks {
        engine.push(format!("race_task_{}", i).into_bytes(), 1).unwrap();
    }

    let acked_tasks = Arc::new(Mutex::new(HashSet::new()));
    let num_workers = 4;
    let mut handles = Vec::new();

    for worker_id in 0..num_workers {
        let eng = Arc::clone(&engine);
        let acked = Arc::clone(&acked_tasks);

        handles.push(thread::spawn(move || {
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(8) {
                // Lease task with short TTL (1s)
                match eng.pop_and_lease(worker_id, 1) {
                    Ok(Some((rec, _))) => {
                        let id = rec.id();
                        // Variable delay around 1s boundary to maximize race with expiration thread
                        let delay_ms = (id * 37) % 1500;
                        thread::sleep(Duration::from_millis(delay_ms));

                        match eng.ack(id, worker_id) {
                            Ok(()) => {
                                let mut guard = acked.lock().unwrap();
                                let inserted = guard.insert(id);
                                assert!(
                                    inserted,
                                    "Double ACK detected for task ID {}: already acknowledged!",
                                    id
                                );
                            }
                            Err(e) => {
                                // Must be PermissionDenied (re-leased) or NotFound (expired/dead-lettered)
                                assert!(
                                    e.kind() == io::ErrorKind::PermissionDenied
                                        || e.kind() == io::ErrorKind::NotFound,
                                    "Unexpected error on ACK: {:?}",
                                    e
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        thread::sleep(Duration::from_millis(50));
                        let (ready, leased) = eng.status();
                        if ready == 0 && leased == 0 {
                            break;
                        }
                    }
                    Err(e) => panic!("pop_and_lease error: {:?}", e),
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Wait until background worker drains any remaining expired tasks
    let drained = wait_for_lease_expiration(&engine, Duration::from_secs(6));
    assert!(drained);

    // Drain any remaining tasks that expired and are back in ready queue
    while let Ok(Some((rec, _))) = engine.pop_and_lease(999, 10) {
        let id = rec.id();
        engine.ack(id, 999).unwrap();
        let mut guard = acked_tasks.lock().unwrap();
        guard.insert(id);
    }

    assert_eq!(engine.status(), (0, 0));
    let _ = fs::remove_dir_all(&dir);
}

/// Test 9: Zero double delivery under multi-consumer lease churn.
/// 10 concurrent consumers compete for 500 tasks.
/// An active lease registry actively validates that no two consumers hold valid leases
/// for the same task at the same instant.
#[test]
fn test_lease_concurrency_zero_double_delivery() {
    let dir = temp_test_dir("zero_double_delivery");
    let engine = Arc::new(Engine::open(&dir, None).unwrap());

    let total_tasks = 500;
    for i in 0..total_tasks {
        let priority = (i % 256) as u8;
        engine.push(format!("task_{}", i).into_bytes(), priority).unwrap();
    }

    assert_eq!(engine.status(), (total_tasks, 0));

    // Active lease registry: tracks which consumer currently holds which task
    let active_leases = Arc::new(Mutex::new(HashMap::<u64, u32>::new()));
    let completed_tasks = Arc::new(Mutex::new(HashSet::<u64>::new()));

    let num_consumers = 10;
    let mut handles = Vec::new();

    for c_id in 0..num_consumers {
        let eng = Arc::clone(&engine);
        let active = Arc::clone(&active_leases);
        let completed = Arc::clone(&completed_tasks);

        handles.push(thread::spawn(move || {
            loop {
                // Lease with 15s TTL so they don't expire prematurely during active processing
                match eng.pop_and_lease(c_id, 15) {
                    Ok(Some((rec, _))) => {
                        let id = rec.id();

                        // 1. Double delivery check
                        {
                            let mut act = active.lock().unwrap();
                            if let Some(other_c) = act.get(&id) {
                                panic!(
                                    "DOUBLE DELIVERY DETECTED! Task {} leased by consumer {} while already held by consumer {}",
                                    id, c_id, other_c
                                );
                            }
                            act.insert(id, c_id);
                        }

                        // Simulate simulated work
                        thread::sleep(Duration::from_micros(25));

                        // 2. Acknowledge task
                        eng.ack(id, c_id).unwrap();

                        // 3. Remove from active leases and add to completed
                        {
                            let mut act = active.lock().unwrap();
                            let removed = act.remove(&id);
                            assert_eq!(removed, Some(c_id));
                        }

                        {
                            let mut comp = completed.lock().unwrap();
                            let inserted = comp.insert(id);
                            assert!(inserted, "Duplicate completion for task {}", id);
                        }
                    }
                    Ok(None) => {
                        let comp = completed.lock().unwrap();
                        if comp.len() == total_tasks {
                            break;
                        }
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(e) => panic!("Consumer error: {:?}", e),
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let final_completed = completed_tasks.lock().unwrap().len();
    assert_eq!(
        final_completed, total_tasks,
        "All {} tasks must be completed",
        total_tasks
    );

    let active_left = active_leases.lock().unwrap().len();
    assert_eq!(active_left, 0, "No active leases should remain");

    let (ready_end, leased_end) = engine.status();
    assert_eq!(ready_end, 0);
    assert_eq!(leased_end, 0);

    let _ = fs::remove_dir_all(&dir);
}

// =========================================================================
// SECTION 3: Rapid Client Churn (Tests 10–13)
// =========================================================================

/// Test 10: Burst pushes and immediate pops without sync, followed by immediate sync.
/// Pushes 1,000 tasks and pops/acks all 1,000 tasks purely in memory (OS page cache),
/// then executes an explicit group commit sync() barrier.
/// Verifies cold reboot recovery leaves the engine clean with 0 lingering tasks.
#[test]
fn test_rapid_churn_burst_push_pop_without_sync() {
    let dir = temp_test_dir("churn_burst_no_sync");

    let total_tasks = 1000;

    // Phase 1: High-speed burst push and pop without sync
    {
        let engine = Engine::open(&dir, None).unwrap();

        // Burst pushes without sync
        for i in 0..total_tasks {
            let payload = format!("burst_data_{}", i).into_bytes();
            engine.push(payload, (i % 256) as u8).unwrap();
        }

        let (ready, leased) = engine.status();
        assert_eq!(ready, total_tasks);
        assert_eq!(leased, 0);

        // Immediate pop and ack without sync
        let mut popped_count = 0;
        while let Ok(Some((rec, _))) = engine.pop_and_lease(1, 60) {
            engine.ack(rec.id(), 1).unwrap();
            popped_count += 1;
        }

        assert_eq!(popped_count, total_tasks);
        assert_eq!(engine.status(), (0, 0));

        // Group commit sync flushes the entire burst to persistent disk
        engine.sync().unwrap();
    }

    // Phase 2: Cold reboot verification
    {
        let engine = Engine::open(&dir, None).unwrap();
        assert_eq!(
            engine.status(),
            (0, 0),
            "Cold reboot must reflect complete drain of all burst tasks"
        );
        assert!(engine.pop_and_lease(1, 10).unwrap().is_none());

        // Subsequent pushes and pops operate normally
        let id = engine.push(b"post_reboot".to_vec(), 10).unwrap();
        assert_eq!(id, (total_tasks + 1) as u64);

        let (rec, _) = engine.pop_and_lease(1, 10).unwrap().unwrap();
        assert_eq!(rec.id(), id);
        assert_eq!(rec.payload(), b"post_reboot");
        engine.ack(rec.id(), 1).unwrap();

        assert_eq!(engine.status(), (0, 0));
    }

    let _ = fs::remove_dir_all(&dir);
}

/// Test 11: Rapid client churn with partial drain and sync durability.
/// Pushes 2,000 tasks across multiple priority tiers without sync,
/// pops and acks the top 1,000 tasks without sync, then calls sync().
/// Verifies cold reboot recovers exactly the remaining 1,000 unleased tasks with bit-exact FIFO order.
#[test]
fn test_rapid_churn_partial_drain_and_durability() {
    let dir = temp_test_dir("churn_partial_drain");

    // Phase 1: Push 2,000 tasks:
    // 500 at priority 200, 500 at priority 150, 500 at priority 100, 500 at priority 50
    {
        let engine = Engine::open(&dir, None).unwrap();

        for &p in &[200u8, 150, 100, 50] {
            for seq in 0..500 {
                let payload = format!("p_{}_seq_{}", p, seq).into_bytes();
                engine.push(payload, p).unwrap();
            }
        }

        assert_eq!(engine.status(), (2000, 0));

        // Pop and ack 1,000 tasks without sync:
        // Must pop all 500 of priority 200, then all 500 of priority 150!
        for expected_p in [200u8, 150] {
            for expected_seq in 0..500 {
                let (rec, _) = engine.pop_and_lease(1, 60).unwrap().expect("task must exist");
                assert_eq!(rec.priority(), expected_p);
                let expected_payload = format!("p_{}_seq_{}", expected_p, expected_seq);
                assert_eq!(rec.payload(), expected_payload.as_bytes());
                engine.ack(rec.id(), 1).unwrap();
            }
        }

        assert_eq!(engine.status(), (1000, 0));

        // Sync remaining state to disk
        engine.sync().unwrap();
    }

    // Phase 2: Cold reboot recovery
    {
        let engine = Engine::open(&dir, None).unwrap();
        assert_eq!(
            engine.status(),
            (1000, 0),
            "Exactly 1,000 tasks must be recovered post-restart"
        );

        // Drain remaining 1,000 tasks: all 500 of priority 100, then all 500 of priority 50
        for expected_p in [100u8, 50] {
            for expected_seq in 0..500 {
                let (rec, _) = engine.pop_and_lease(1, 60).unwrap().expect("task must exist");
                assert_eq!(rec.priority(), expected_p);
                let expected_payload = format!("p_{}_seq_{}", expected_p, expected_seq);
                assert_eq!(rec.payload(), expected_payload.as_bytes());
                engine.ack(rec.id(), 1).unwrap();
            }
        }

        assert_eq!(engine.status(), (0, 0));
        assert!(engine.pop_and_lease(1, 10).unwrap().is_none());
    }

    let _ = fs::remove_dir_all(&dir);
}

/// Test 12: Rapid churn with WAL segment rollover.
/// Uses a constrained segment threshold (15 KB) to trigger multiple WAL rotations
/// during rapid un-synced push and pop cycles.
/// Verifies that segments are safely rotated, flushed, and replayed on cold reboot.
#[test]
fn test_rapid_churn_with_segment_rollover() {
    let dir = temp_test_dir("churn_segment_rollover");
    let segment_threshold = 15 * 1024; // 15 KB threshold

    // Phase 1: Heavy churn triggering multi-segment rotation
    {
        let wal = Wal::open(&dir, Some(segment_threshold)).unwrap();
        let state = EngineState::new(wal);
        let reader = Arc::new(WalReader::new(dir.clone()));
        let group_commit = Arc::new(GroupCommit::new(Default::default()));
        let engine = Engine::new(state, reader, group_commit);

        // 1. Push 500 tasks with ~120 byte payloads (~70 KB total -> ~5 segments)
        for i in 0..500 {
            let payload = format!("rollover_burst_msg_{:04}_{:080}", i, i).into_bytes();
            engine.push(payload, (i % 64) as u8).unwrap();
        }

        // 2. Pop and ack 250 tasks without sync
        for _ in 0..250 {
            let (rec, _) = engine.pop_and_lease(1, 60).unwrap().unwrap();
            engine.ack(rec.id(), 1).unwrap();
        }

        // 3. Push another 250 tasks (~35 KB -> ~2 more segments)
        for i in 500..750 {
            let payload = format!("rollover_burst_msg_{:04}_{:080}", i, i).into_bytes();
            engine.push(payload, (i % 64) as u8).unwrap();
        }

        // 4. Pop and ack 250 tasks without sync
        for _ in 0..250 {
            let (rec, _) = engine.pop_and_lease(1, 60).unwrap().unwrap();
            engine.ack(rec.id(), 1).unwrap();
        }

        // 5. Final sync
        engine.sync().unwrap();

        // 6. Verify multiple segment files exist in directory
        let wal_count = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
            .count();

        assert!(
            wal_count >= 3,
            "Expected >= 3 WAL segments due to small threshold, found {}",
            wal_count
        );
    }

    // Phase 2: Cold reboot recovery across all rotated segments
    {
        let wal = Wal::open(&dir, Some(segment_threshold)).unwrap();
        let reader = Arc::new(WalReader::new(dir.clone()));
        let mut state = EngineState::new(wal);
        state.recover(&reader).unwrap();
        let group_commit = Arc::new(GroupCommit::new(Default::default()));
        let engine = Engine::new(state, reader, group_commit);

        // Remaining tasks: 750 pushed - 500 acked = 250 tasks
        assert_eq!(
            engine.status(),
            (250, 0),
            "Expected 250 unleased tasks to be recovered across segments"
        );

        let mut drained = 0;
        while let Ok(Some((rec, _))) = engine.pop_and_lease(1, 60) {
            assert!(!rec.payload().is_empty());
            engine.ack(rec.id(), 1).unwrap();
            drained += 1;
        }

        assert_eq!(drained, 250);
        assert_eq!(engine.status(), (0, 0));
    }

    let _ = fs::remove_dir_all(&dir);
}

/// Test 13: Rapid client churn through the HTTP REST API.
/// Pushes 100 tasks via HTTP POST with `?sync=false`, and concurrently pops and acks
/// them via 4 HTTP client workers.
/// Verifies REST API handles rapid churn without connection hangs, deadlocks, or task loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rapid_client_churn_http_api() {
    let dir = temp_test_dir("http_client_churn");
    let base_url = spawn_test_server(dir.clone()).await;
    let client = reqwest::Client::new();

    let total_tasks = 100;

    // 1. Burst push 100 tasks with ?sync=false
    for i in 0..total_tasks {
        let payload = BASE64_STANDARD.encode(format!("http_churn_{}", i).as_bytes());
        let res = client
            .post(format!("{}/v1/queues/churn_q/push?sync=false", base_url))
            .json(&json!({
                "payload": payload,
                "priority": (i % 50) as u8,
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(
            res.status(),
            reqwest::StatusCode::CREATED,
            "Push should return 201 CREATED"
        );
    }

    // Check status post-push
    let status_res = client
        .get(format!("{}/v1/queues/churn_q/status", base_url))
        .send()
        .await
        .unwrap();
    let status_json: serde_json::Value = status_res.json().await.unwrap();
    assert_eq!(status_json["ready_tasks"], total_tasks);
    assert_eq!(status_json["active_leases"], 0);

    // 2. Spawn 4 concurrent HTTP consumers popping and acking
    let total_acked = Arc::new(AtomicUsize::new(0));
    let mut consumer_tasks = Vec::new();

    for c_id in 0..4u32 {
        let url = base_url.clone();
        let http_client = client.clone();
        let counter = Arc::clone(&total_acked);

        consumer_tasks.push(tokio::spawn(async move {
            let mut empty_retries = 0;
            while counter.load(Ordering::SeqCst) < total_tasks && empty_retries < 20 {
                let pop_res = http_client
                    .post(format!("{}/v1/queues/churn_q/pop?sync=false", url))
                    .json(&json!({
                        "consumer_id": c_id + 1,
                        "lease_secs": 10,
                        "wait_secs": 0,
                    }))
                    .send()
                    .await
                    .unwrap();

                if pop_res.status() == reqwest::StatusCode::OK {
                    empty_retries = 0;
                    let pop_body: serde_json::Value = pop_res.json().await.unwrap();
                    let task_id = pop_body["id"].as_u64().unwrap();

                    // Ack the task immediately
                    let ack_res = http_client
                        .post(format!(
                            "{}/v1/queues/churn_q/tasks/{}/ack?sync=false",
                            url, task_id
                        ))
                        .json(&json!({
                            "consumer_id": c_id + 1,
                        }))
                        .send()
                        .await
                        .unwrap();

                    assert_eq!(ack_res.status(), reqwest::StatusCode::OK);
                    counter.fetch_add(1, Ordering::SeqCst);
                } else if pop_res.status() == reqwest::StatusCode::NO_CONTENT {
                    empty_retries += 1;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                } else {
                    panic!("Unexpected pop response status: {}", pop_res.status());
                }
            }
        }));
    }

    for task in consumer_tasks {
        task.await.unwrap();
    }

    assert_eq!(
        total_acked.load(Ordering::SeqCst),
        total_tasks,
        "All 100 tasks must be acknowledged via HTTP"
    );

    // 3. Final status check: empty queue and zero active leases
    let final_status_res = client
        .get(format!("{}/v1/queues/churn_q/status", base_url))
        .send()
        .await
        .unwrap();
    let final_status_json: serde_json::Value = final_status_res.json().await.unwrap();
    assert_eq!(final_status_json["ready_tasks"], 0);
    assert_eq!(final_status_json["active_leases"], 0);

    let _ = fs::remove_dir_all(&dir);
}
