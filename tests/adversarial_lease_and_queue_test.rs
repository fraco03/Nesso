use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use nesso::storage::engine::{Engine, TaskRef};
use nesso::storage::priority_queue::PriorityBucketQueue;

static ADVERSARIAL_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_adv_dir(name: &str) -> PathBuf {
    let count = ADVERSARIAL_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!(
        "nesso_adv_{}_{}_{}",
        name,
        std::process::id(),
        count
    ));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

// =========================================================================
// TEST 1: Stress test hardware bitmap word boundaries under multi-threaded contention
// Word boundaries: (63, 64), (127, 128), (191, 192), and extremes (0, 255)
// 8 producer threads push boundary priorities concurrently.
// 8 consumer threads pop concurrently, recording each popped task in real-time.
// Asserts:
// - Exact total items delivered
// - Strict FIFO preservation within each (producer, priority) stream
// - Strict priority monotonicity across popped items
// =========================================================================
#[test]
fn test_stress_word_boundaries_multi_threaded_contention() {
    let dir = temp_adv_dir("boundary_contention");
    let engine = Arc::new(Engine::open(&dir, None).unwrap());

    let boundary_priorities = [0u8, 63, 64, 127, 128, 191, 192, 255];
    let num_producers = 8;
    let items_per_producer_per_p = 50; // 8 * 8 * 50 = 3,200 items

    let mut producer_handles = Vec::new();
    let start_barrier = Arc::new(std::sync::Barrier::new(num_producers + 1));

    for prod_id in 0..num_producers {
        let eng = Arc::clone(&engine);
        let barrier = Arc::clone(&start_barrier);
        let priorities = boundary_priorities;

        producer_handles.push(thread::spawn(move || {
            barrier.wait();
            for &p in &priorities {
                for seq in 0..items_per_producer_per_p {
                    let payload = format!("prod_{}_p_{}_seq_{:04}", prod_id, p, seq).into_bytes();
                    eng.push(payload, p).unwrap();
                }
            }
        }));
    }

    start_barrier.wait();

    for h in producer_handles {
        h.join().unwrap();
    }

    let total_expected = num_producers * boundary_priorities.len() * items_per_producer_per_p;
    let (ready, leased) = engine.status();
    assert_eq!(ready, total_expected);
    assert_eq!(leased, 0);

    // Multi-consumer extraction to stress concurrent pop_and_lease
    // Record into a mutex-protected global log IN REAL-TIME at pop instant
    let num_consumers = 8;
    let popped_log = Arc::new(Mutex::new(Vec::with_capacity(total_expected)));
    let mut consumer_handles = Vec::new();

    for c_id in 0..num_consumers {
        let eng = Arc::clone(&engine);
        let log = Arc::clone(&popped_log);

        consumer_handles.push(thread::spawn(move || {
            while let Ok(Some((rec, _))) = eng.pop_and_lease(c_id + 100, 60) {
                let p = rec.priority();
                let payload_str = String::from_utf8(rec.payload().to_vec()).unwrap();
                {
                    let mut g = log.lock().unwrap();
                    g.push((rec.id(), p, payload_str));
                }
                eng.ack(rec.id(), c_id + 100).unwrap();
            }
        }));
    }

    for h in consumer_handles {
        h.join().unwrap();
    }

    let all_popped = popped_log.lock().unwrap().clone();
    assert_eq!(all_popped.len(), total_expected);

    // 1. Verify monotonic non-increasing priority order
    for w in all_popped.windows(2) {
        assert!(
            w[0].1 >= w[1].1,
            "Priority inversion detected: prio {} popped before prio {}",
            w[0].1,
            w[1].1
        );
    }

    // 2. Verify strict FIFO within each (producer, priority) stream
    let mut producer_seqs: HashMap<(usize, u8), Vec<usize>> = HashMap::new();
    for (_id, p, payload_str) in &all_popped {
        let parts: Vec<&str> = payload_str.split('_').collect();
        let prod_id: usize = parts[1].parse().unwrap();
        let prio: u8 = parts[3].parse().unwrap();
        let seq: usize = parts[5].parse().unwrap();
        assert_eq!(*p, prio);

        producer_seqs.entry((prod_id, prio)).or_default().push(seq);
    }

    for ((prod_id, prio), seqs) in producer_seqs {
        assert_eq!(
            seqs.len(),
            items_per_producer_per_p,
            "Missing items for prod {} prio {}",
            prod_id,
            prio
        );
        for w in seqs.windows(2) {
            assert!(
                w[0] < w[1],
                "FIFO violation in prod {} prio {}: seq {} popped before seq {}",
                prod_id,
                prio,
                w[0],
                w[1]
            );
        }
    }

    let (final_ready, final_leased) = engine.status();
    assert_eq!(final_ready, 0);
    assert_eq!(final_leased, 0);

    let _ = fs::remove_dir_all(&dir);
}

// =========================================================================
// TEST 2: Fuzz test PriorityBucketQueue oracle comparison across 100,000 ops
// Compares PriorityBucketQueue against a naive sorted Vec oracle.
// Random push, pop, clear across boundary values and arbitrary priorities.
// =========================================================================
#[test]
fn test_priority_bucket_queue_fuzz_oracle() {
    let mut pq = PriorityBucketQueue::new();
    let mut oracle: Vec<TaskRef> = Vec::new();

    let mut seed = 0x123456789abcdef0u64;
    let mut next_rand = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        seed
    };

    let total_ops = 100_000;
    let mut next_id = 1u64;

    for _ in 0..total_ops {
        let op = next_rand() % 10;
        if op < 6 || oracle.is_empty() {
            // Push
            let p = if next_rand() % 2 == 0 {
                let boundaries = [0u8, 63, 64, 127, 128, 191, 192, 255];
                boundaries[(next_rand() as usize) % boundaries.len()]
            } else {
                (next_rand() % 256) as u8
            };

            let task = TaskRef {
                id: next_id,
                priority: p,
                retries: 0,
            };
            next_id += 1;

            pq.push(task.clone());
            oracle.push(task);
        } else {
            // Pop
            let pq_task = pq.pop();

            assert!(!oracle.is_empty());
            let mut max_idx = 0;
            let mut max_p = oracle[0].priority;

            for i in 1..oracle.len() {
                if oracle[i].priority > max_p {
                    max_p = oracle[i].priority;
                    max_idx = i;
                }
            }

            let oracle_task = oracle.remove(max_idx);

            let pq_val = pq_task.expect("PQ must not be empty when oracle is not");
            assert_eq!(
                pq_val.priority, oracle_task.priority,
                "Priority mismatch between PQ and oracle"
            );
            assert_eq!(
                pq_val.id, oracle_task.id,
                "FIFO ID mismatch at priority {}: PQ returned {}, oracle returned {}",
                pq_val.priority, pq_val.id, oracle_task.id
            );
        }

        assert_eq!(pq.len(), oracle.len());
        assert_eq!(pq.is_empty(), oracle.is_empty());
    }

    // Drain remaining
    while !oracle.is_empty() {
        let pq_val = pq.pop().unwrap();
        let mut max_idx = 0;
        let mut max_p = oracle[0].priority;
        for i in 1..oracle.len() {
            if oracle[i].priority > max_p {
                max_p = oracle[i].priority;
                max_idx = i;
            }
        }
        let oracle_val = oracle.remove(max_idx);
        assert_eq!(pq_val.id, oracle_val.id);
        assert_eq!(pq_val.priority, oracle_val.priority);
    }
    assert!(pq.pop().is_none());
}

// =========================================================================
// TEST 3: Extreme alternating priority bursts and rapid churn
// 10 cycles of 1,000 tasks burst at extreme priorities:
// 255 -> 0 -> 191 -> 64 -> 128 -> 127 -> 192 -> 63
// Interleaved with rapid un-synced drains and partial pushes.
// Asserts bit manipulation doesn't drop bits or cross-corrupt bitmap words.
// =========================================================================
#[test]
fn test_extreme_alternating_priority_bursts_and_churn() {
    let dir = temp_adv_dir("extreme_alternating_churn");
    let engine = Engine::open(&dir, None).unwrap();

    let burst_priorities = [255u8, 0, 191, 64, 128, 127, 192, 63];
    let tasks_per_burst = 200;

    for cycle in 0..5 {
        // Burst push across alternating extremes
        for &p in &burst_priorities {
            for seq in 0..tasks_per_burst {
                let payload = format!("c_{}_p_{}_s_{}", cycle, p, seq).into_bytes();
                engine.push(payload, p).unwrap();
            }
        }

        let total_in_burst = burst_priorities.len() * tasks_per_burst;
        let (ready, leased) = engine.status();
        assert_eq!(ready, total_in_burst);
        assert_eq!(leased, 0);

        // Pop in strict priority order (descending):
        // 255, 192, 191, 128, 127, 64, 63, 0
        let mut sorted_priorities = burst_priorities;
        sorted_priorities.sort_by(|a, b| b.cmp(a));

        for &expected_p in &sorted_priorities {
            for expected_seq in 0..tasks_per_burst {
                let (rec, _) = engine.pop_and_lease(1, 60).unwrap().expect("task must exist");
                assert_eq!(
                    rec.priority(),
                    expected_p,
                    "Cycle {}: expected priority {} but got {}",
                    cycle,
                    expected_p,
                    rec.priority()
                );
                let payload = String::from_utf8(rec.payload().to_vec()).unwrap();
                assert_eq!(
                    payload,
                    format!("c_{}_p_{}_s_{}", cycle, expected_p, expected_seq),
                    "Cycle {}: FIFO violation at priority {}",
                    cycle,
                    expected_p
                );
                engine.ack(rec.id(), 1).unwrap();
            }
        }

        assert_eq!(engine.status(), (0, 0));
    }

    let _ = fs::remove_dir_all(&dir);
}

// =========================================================================
// TEST 4: True double-delivery prevention under concurrent workers
// Spawns 16 concurrent workers with unexpired leases.
// Asserts that no two workers ever hold an active UNEXPIRED lease for the same task.
// Also asserts that no task is ever successfully ACKed more than once.
// =========================================================================
#[test]
fn test_true_zero_double_delivery_and_exclusive_ack() {
    let dir = temp_adv_dir("exclusive_lease_stress");
    let engine = Arc::new(Engine::open(&dir, None).unwrap());

    let num_tasks = 400;
    for i in 0..num_tasks {
        engine.push(format!("task_{}", i).into_bytes(), (i % 256) as u8).unwrap();
    }

    // Active unexpired lease tracker: task_id -> (worker_id, expire_instant)
    let active_unexpired = Arc::new(Mutex::new(HashMap::<u64, (u32, Instant)>::new()));
    let acked_tasks = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let double_delivery = Arc::new(AtomicBool::new(false));
    let double_ack = Arc::new(AtomicBool::new(false));

    let num_workers = 16;
    let mut handles = Vec::new();

    for worker_id in 0..num_workers {
        let eng = Arc::clone(&engine);
        let active = Arc::clone(&active_unexpired);
        let acked = Arc::clone(&acked_tasks);
        let dd = Arc::clone(&double_delivery);
        let da = Arc::clone(&double_ack);

        handles.push(thread::spawn(move || {
            loop {
                // Lease with 10s TTL - plenty of time before expiration
                match eng.pop_and_lease(worker_id, 10) {
                    Ok(Some((rec, _))) => {
                        let id = rec.id();
                        let now = Instant::now();
                        let expire_instant = now + Duration::from_secs(10);

                        // 1. Verify mutual exclusivity while unexpired
                        {
                            let mut act = active.lock().unwrap();
                            if let Some(&(other_w, other_exp)) = act.get(&id) {
                                if now < other_exp {
                                    eprintln!(
                                        "FATAL DOUBLE DELIVERY: Task {} leased by worker {} while still validly held by worker {}",
                                        id, worker_id, other_w
                                    );
                                    dd.store(true, Ordering::SeqCst);
                                }
                            }
                            act.insert(id, (worker_id, expire_instant));
                        }

                        // Simulate non-trivial work
                        thread::sleep(Duration::from_micros(100));

                        // 2. ACK the task
                        match eng.ack(id, worker_id) {
                            Ok(()) => {
                                let mut a = acked.lock().unwrap();
                                if !a.insert(id) {
                                    eprintln!("FATAL DOUBLE ACK: Task {} acked twice!", id);
                                    da.store(true, Ordering::SeqCst);
                                }
                            }
                            Err(e) => {
                                eprintln!("Unexpected ACK error for task {}: {:?}", id, e);
                            }
                        }

                        // Remove from active
                        {
                            let mut act = active.lock().unwrap();
                            if let Some(&(w, _)) = act.get(&id) {
                                if w == worker_id {
                                    act.remove(&id);
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        let a = acked.lock().unwrap();
                        if a.len() == num_tasks {
                            break;
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(e) => panic!("pop_and_lease error: {:?}", e),
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(
        !double_delivery.load(Ordering::SeqCst),
        "Double delivery violation detected!"
    );
    assert!(
        !double_ack.load(Ordering::SeqCst),
        "Double ACK violation detected!"
    );

    let total_acked = acked_tasks.lock().unwrap().len();
    assert_eq!(total_acked, num_tasks);
    assert_eq!(engine.status(), (0, 0));

    let _ = fs::remove_dir_all(&dir);
}

// =========================================================================
// TEST 5: Targeted TOCTOU race between background expiration thread and ACK
// Tests the gap where start_expiration_thread drops the engine lock
// between finding expired tasks and processing task failures.
// Asserts:
// - No ACKed task is resurrected into ready_queue
// - No pop_and_lease returns Err(NotFound: "Missing data offset")
// =========================================================================
#[test]
fn test_toctou_expiration_vs_ack_race() {
    let dir = temp_adv_dir("toctou_race");
    let engine = Arc::new(Engine::open(&dir, None).unwrap());

    let mut missing_data_offset_count = 0;
    let mut resurrected_tasks = Vec::new();

    // Run 10 iterations with higher concurrency and tight timing
    for iter in 0..10 {
        let num_tasks = 400;
        let mut task_ids = Vec::new();

        for i in 0..num_tasks {
            let id = engine.push(format!("toctou_iter_{}_task_{}", iter, i).into_bytes(), 50).unwrap();
            task_ids.push(id);
        }

        // Lease all tasks with TTL = 1s
        for &id in &task_ids {
            let res = engine.pop_and_lease(id as u32, 1).unwrap();
            assert!(res.is_some());
        }

        // Wait until right at the 1.0s expiration boundary
        // Stagger worker threads between 950ms and 1050ms
        let ack_threads = 16;
        let mut handles = Vec::new();
        let ids_arc = Arc::new(task_ids.clone());
        let successfully_acked = Arc::new(Mutex::new(HashSet::new()));

        for t_idx in 0..ack_threads {
            let eng = Arc::clone(&engine);
            let ids = Arc::clone(&ids_arc);
            let acked = Arc::clone(&successfully_acked);

            handles.push(thread::spawn(move || {
                let stagger = 950 + (t_idx * 10);
                thread::sleep(Duration::from_millis(stagger as u64));

                for &id in ids.iter() {
                    if eng.ack(id, id as u32).is_ok() {
                        acked.lock().unwrap().insert(id);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Wait 1.5 seconds for background expiration thread to settle
        thread::sleep(Duration::from_millis(1500));

        let acked_set = successfully_acked.lock().unwrap().clone();

        // Drain the ready queue and check for "Missing data offset" or resurrected tasks
        loop {
            match engine.pop_and_lease(9999, 10) {
                Ok(Some((rec, _))) => {
                    let id = rec.id();
                    if acked_set.contains(&id) {
                        eprintln!("VULNERABILITY DETECTED: Task {} was ACKed, but popped again from ready_queue!", id);
                        resurrected_tasks.push(id);
                    }
                    let _ = engine.ack(id, 9999);
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("VULNERABILITY DETECTED: pop_and_lease returned error: {:?}", e);
                    if e.to_string().contains("Missing data offset") {
                        missing_data_offset_count += 1;
                    }
                    break;
                }
            }
        }
    }

    let _ = fs::remove_dir_all(&dir);

    assert_eq!(
        missing_data_offset_count, 0,
        "Engine failed with 'Missing data offset' due to TOCTOU expiration race!"
    );
    assert!(
        resurrected_tasks.is_empty(),
        "ACKed tasks were resurrected back into ready_queue: {:?}",
        resurrected_tasks
    );
}
