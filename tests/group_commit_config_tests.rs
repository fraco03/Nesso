use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use nesso::server::AppState;
use nesso::storage::engine::{Engine, GroupCommitConfig, SyncMode};

fn temp_data_dir(test_name: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let unique_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.push(format!("nesso_group_commit_cfg_{}_{}", test_name, unique_id));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_default_group_commit_config() {
    let dir = temp_data_dir("default_cfg");

    // When passing None, Engine must use the default configuration:
    // batch_window: 2ms, max_batch_size: 64, idle_commit_threshold: 0, sync_mode: Standard
    let engine = Engine::open(&dir, None).unwrap();
    assert_eq!(engine.config(), GroupCommitConfig::default());
    assert_eq!(engine.config().batch_window, Duration::from_millis(2));
    assert_eq!(engine.config().max_batch_size, 64);
    assert_eq!(engine.config().idle_commit_threshold, Duration::ZERO);
    assert_eq!(engine.config().sync_mode, SyncMode::Standard);

    // AppState::new should also initialize with default config
    let state = AppState::new(dir.clone());
    assert_eq!(state.group_commit_config, GroupCommitConfig::default());

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_explicit_group_commit_config() {
    let dir = temp_data_dir("explicit_cfg");
    let custom_cfg = GroupCommitConfig {
        batch_window: Duration::from_millis(15),
        max_batch_size: 32,
        idle_commit_threshold: Duration::from_micros(500),
        sync_mode: SyncMode::FullHardware,
    };

    let engine = Engine::open_with_config(&dir, custom_cfg).unwrap();
    assert_eq!(engine.config(), custom_cfg);
    assert_eq!(engine.config().batch_window, Duration::from_millis(15));
    assert_eq!(engine.config().max_batch_size, 32);
    assert_eq!(engine.config().idle_commit_threshold, Duration::from_micros(500));
    assert_eq!(engine.config().sync_mode, SyncMode::FullHardware);

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// REQUIRED TEST 1: Single isolated thread, zero concurrency.
// Measures push+sync latency under adaptive commit vs fixed window.
// With adaptive commit (idle_commit_threshold = 250us), the leader doesn't wait
// the entire batch_window (e.g. 10ms); latency is ~idle_threshold + fsync_latency.
// -----------------------------------------------------------------------------
#[test]
fn test_single_thread_isolated_adaptive_latency() {
    let dir_fixed = temp_data_dir("single_fixed");
    let dir_adaptive = temp_data_dir("single_adaptive");

    // Fixed Engine: batch_window 15ms, idle_threshold 15ms (forces full 15ms wait)
    let cfg_fixed = GroupCommitConfig {
        batch_window: Duration::from_millis(15),
        max_batch_size: 64,
        idle_commit_threshold: Duration::from_millis(15),
        sync_mode: SyncMode::Standard,
    };
    let engine_fixed = Engine::open_with_config(&dir_fixed, cfg_fixed).unwrap();

    // Adaptive Engine: batch_window 15ms, idle_threshold 250 microseconds
    let cfg_adaptive = GroupCommitConfig {
        batch_window: Duration::from_millis(15),
        max_batch_size: 64,
        idle_commit_threshold: Duration::from_micros(250),
        sync_mode: SyncMode::Standard,
    };
    let engine_adaptive = Engine::open_with_config(&dir_adaptive, cfg_adaptive).unwrap();

    let iterations = 5;

    // Measure Fixed Engine
    let start_fixed = Instant::now();
    for i in 0..iterations {
        engine_fixed.push(format!("fixed_msg_{}", i).into_bytes(), 1).unwrap();
        engine_fixed.sync().unwrap();
    }
    let elapsed_fixed = start_fixed.elapsed();
    let avg_fixed_ms = elapsed_fixed.as_secs_f64() * 1000.0 / iterations as f64;

    // Measure Adaptive Engine
    let start_adaptive = Instant::now();
    for i in 0..iterations {
        engine_adaptive.push(format!("adaptive_msg_{}", i).into_bytes(), 1).unwrap();
        engine_adaptive.sync().unwrap();
    }
    let elapsed_adaptive = start_adaptive.elapsed();
    let avg_adaptive_ms = elapsed_adaptive.as_secs_f64() * 1000.0 / iterations as f64;

    println!(
        "TEST 1 -> Avg latency: Fixed (15ms window) = {:.2}ms, Adaptive (250us threshold) = {:.2}ms",
        avg_fixed_ms, avg_adaptive_ms
    );

    // Adaptive commit must be substantially faster, saving > 10ms per iteration
    assert!(
        avg_adaptive_ms < avg_fixed_ms,
        "Adaptive latency ({:.2}ms) must be significantly lower than fixed ({:.2}ms)",
        avg_adaptive_ms,
        avg_fixed_ms
    );
    assert!(
        avg_fixed_ms - avg_adaptive_ms >= 8.0,
        "Adaptive commit should save roughly the difference between 15ms and 250us, saved: {:.2}ms",
        avg_fixed_ms - avg_adaptive_ms
    );

    let _ = fs::remove_dir_all(&dir_fixed);
    let _ = fs::remove_dir_all(&dir_adaptive);
}

// -----------------------------------------------------------------------------
// REQUIRED TEST 2: High concurrency (16 threads starting via Barrier).
// Verifies that batching is NOT prematurely aborted by adaptive commit:
// Because threads arrive in rapid succession (< idle_commit_threshold),
// the idle timer is continuously reset and requests coalesce into 1 batch.
// -----------------------------------------------------------------------------
#[test]
fn test_high_concurrency_batching_preservation() {
    let dir = temp_data_dir("high_concurrency");
    let config = GroupCommitConfig {
        batch_window: Duration::from_millis(20),
        max_batch_size: 64,
        idle_commit_threshold: Duration::from_micros(500),
        sync_mode: SyncMode::Standard,
    };
    let engine = Arc::new(Engine::open_with_config(&dir, config).unwrap());

    let num_threads = 16;
    let ops_per_thread = 5; // 80 total operations under concurrent load
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let eng = Arc::clone(&engine);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            for i in 0..ops_per_thread {
                eng.push(format!("concurrent_msg_{}_{}", t, i).into_bytes(), 1).unwrap();
                eng.sync().unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let total_batches = engine.synced_batches();
    let total_ops = num_threads * ops_per_thread;
    let avg_batch_size = total_ops as f64 / total_batches as f64;
    println!(
        "TEST 2 -> {} concurrent ops across 16 threads produced {} batches (avg {:.1} msgs/batch)",
        total_ops, total_batches, avg_batch_size
    );

    // Group commit must batch aggressively: average batch size must be >= 4 msgs/batch
    // (Without batching, it would produce 80 physical sync batches).
    assert!(
        avg_batch_size >= 4.0,
        "High concurrency batching failed: average batch size was only {:.1} msgs/batch (total batches: {})",
        avg_batch_size,
        total_batches
    );

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// REQUIRED TEST 3: Intermediate scenario (staggered arrivals).
// 3 threads arrive every idle_commit_threshold / 2 (e.g. 5ms apart with 10ms threshold).
// Because each arrival occurs before the idle timeout expires, the idle timer is reset,
// coalescing all 3 operations into a SINGLE physical fsync batch.
// -----------------------------------------------------------------------------
#[test]
fn test_intermediate_staggered_arrivals_coalesced() {
    let dir = temp_data_dir("staggered_coalesced");
    let threshold = Duration::from_millis(15);
    let config = GroupCommitConfig {
        batch_window: Duration::from_millis(100),
        max_batch_size: 64,
        idle_commit_threshold: threshold,
        sync_mode: SyncMode::Standard,
    };
    let engine = Arc::new(Engine::open_with_config(&dir, config).unwrap());

    let num_threads = 3;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = Vec::new();

    for i in 0..num_threads {
        let eng = Arc::clone(&engine);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            // Stagger arrival by threshold / 2 (e.g. 0ms, 6ms, 12ms)
            if i > 0 {
                thread::sleep(Duration::from_millis(6 * i as u64));
            }
            eng.push(format!("staggered_msg_{}", i).into_bytes(), 1).unwrap();
            eng.sync().unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let batches = engine.synced_batches();
    println!("TEST 3 -> 3 staggered threads (interval < threshold) produced {} batch(es)", batches);

    // Must be grouped into exactly 1 batch because arrivals were sufficiently close
    assert_eq!(
        batches, 1,
        "Staggered threads within idle threshold should have coalesced into 1 batch, got {}",
        batches
    );

    let _ = fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------
// REQUIRED TEST 4: Explicit verification of "write_all completed BEFORE join-batch".
// Verifies that at the exact moment sync() is entered, the record data has ALREADY
// been physically written to the WAL segment file and is immediately readable.
// -----------------------------------------------------------------------------
#[test]
fn test_write_all_completed_before_group_commit_join() {
    let dir = temp_data_dir("write_all_ordering");
    let config = GroupCommitConfig {
        batch_window: Duration::from_millis(10),
        max_batch_size: 64,
        idle_commit_threshold: Duration::from_micros(250),
        sync_mode: SyncMode::Standard,
    };
    let engine = Arc::new(Engine::open_with_config(&dir, config).unwrap());

    let num_threads = 8;
    let mut handles = Vec::new();

    for i in 0..num_threads {
        let eng = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            let payload = format!("ordered_write_payload_{}", i).into_bytes();
            
            // 1. push() completes write_all to WAL file descriptor and updates memory index
            let task_id = eng.push(payload.clone(), 5).unwrap();

            // 2. VERIFY: Before sync() is even called, the record MUST already exist on disk
            //    in the WAL segment and be bit-exact readable via reader
            let (seg_id, offset) = {
                let state = eng.inner.lock().unwrap();
                *state.data_index.get(&task_id).expect("Record must be in data_index")
            };

            let disk_record = eng.reader.read_at(seg_id, offset)
                .expect("I/O error reading WAL")
                .expect("Record must exist at offset");

            assert_eq!(disk_record.id(), task_id);
            assert_eq!(disk_record.payload(), &payload[..]);

            // 3. Only now join the group commit sync
            eng.sync().unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let (ready, leased) = engine.status();
    assert_eq!(ready, num_threads);
    assert_eq!(leased, 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_batching_behavior_comparison_by_window_and_batch_size() {
    let dir_a = temp_data_dir("batch_long");
    let config_long = GroupCommitConfig {
        batch_window: Duration::from_millis(50),
        max_batch_size: 64,
        idle_commit_threshold: Duration::from_millis(50),
        sync_mode: SyncMode::Standard,
    };
    let engine_long = Arc::new(Engine::open_with_config(&dir_a, config_long).unwrap());

    let dir_b = temp_data_dir("batch_short");
    let config_short = GroupCommitConfig {
        batch_window: Duration::from_millis(1),
        max_batch_size: 64,
        idle_commit_threshold: Duration::from_micros(100),
        sync_mode: SyncMode::Standard,
    };
    let engine_short = Arc::new(Engine::open_with_config(&dir_b, config_short).unwrap());

    let num_threads = 8;

    // Run on Engine Long (50ms window)
    {
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = Vec::new();
        for i in 0..num_threads {
            let engine = Arc::clone(&engine_long);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                if i > 0 {
                    thread::sleep(Duration::from_millis(3 * i as u64));
                }
                engine.push(format!("payload_long_{}", i).into_bytes(), 1).unwrap();
                engine.sync().unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    // Run on Engine Short (1ms window)
    {
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = Vec::new();
        for i in 0..num_threads {
            let engine = Arc::clone(&engine_short);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                if i > 0 {
                    thread::sleep(Duration::from_millis(3 * i as u64));
                }
                engine.push(format!("payload_short_{}", i).into_bytes(), 1).unwrap();
                engine.sync().unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    let batches_long = engine_long.synced_batches();
    let batches_short = engine_short.synced_batches();
    println!("TEST RESULTS -> batches_long (50ms): {}, batches_short (1ms): {}", batches_long, batches_short);

    assert!(
        batches_long < batches_short,
        "Engine long (window 50ms) should have produced strictly fewer batches than engine short (1ms): long={}, short={}",
        batches_long,
        batches_short
    );

    let _ = fs::remove_dir_all(&dir_a);
    let _ = fs::remove_dir_all(&dir_b);
}

#[test]
fn test_crash_recovery_with_custom_config() {
    let dir = temp_data_dir("crash_recovery_custom_cfg");
    let custom_cfg = GroupCommitConfig {
        batch_window: Duration::from_millis(5),
        max_batch_size: 16,
        idle_commit_threshold: Duration::from_micros(300),
        sync_mode: SyncMode::Standard,
    };

    // Phase 1: Push tasks and acknowledge one with custom config
    let mut pushed_ids = Vec::new();
    {
        let engine = Engine::open_with_config(&dir, custom_cfg).unwrap();
        for i in 0..10 {
            let id = engine.push(format!("custom_task_{}", i).into_bytes(), (i % 3) as u8).unwrap();
            pushed_ids.push(id);
        }
        engine.sync().unwrap();

        // Lease and ack the first task
        let (popped, _) = engine.pop_and_lease(1, 30).unwrap().unwrap();
        engine.ack(popped.id(), 1).unwrap();
        engine.sync().unwrap();
    }

    // Phase 2: Recover with explicit custom config
    {
        let recovered_engine = Engine::open(&dir, Some(custom_cfg)).unwrap();
        assert_eq!(recovered_engine.config(), custom_cfg);
        let (ready, leased) = recovered_engine.status();
        assert_eq!(ready, 9);
        assert_eq!(leased, 0);

        // Verify remaining 9 tasks can be leased
        for _ in 0..9 {
            let res = recovered_engine.pop_and_lease(2, 30).unwrap();
            assert!(res.is_some());
        }
    }

    // Phase 3: Recover with default config (passing None)
    {
        let default_engine = Engine::open(&dir, None).unwrap();
        assert_eq!(default_engine.config(), GroupCommitConfig::default());
        let (ready, leased) = default_engine.status();
        assert_eq!(ready, 0);
        assert_eq!(leased, 9);
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_sync_mode_execution() {
    // 1. Standard POSIX fsync mode
    let dir_std = temp_data_dir("sync_mode_std");
    let cfg_std = GroupCommitConfig {
        batch_window: Duration::from_millis(5),
        max_batch_size: 16,
        idle_commit_threshold: Duration::ZERO,
        sync_mode: SyncMode::Standard,
    };
    {
        let engine = Engine::open_with_config(&dir_std, cfg_std).unwrap();
        assert_eq!(engine.config().sync_mode, SyncMode::Standard);
        for i in 0..5 {
            engine.push(format!("std_task_{}", i).into_bytes(), 1).unwrap();
        }
        engine.sync().unwrap();
    }
    {
        let recovered = Engine::open_with_config(&dir_std, cfg_std).unwrap();
        let (ready, _) = recovered.status();
        assert_eq!(ready, 5);
        for i in 0..5 {
            let (record, _) = recovered.pop_and_lease(1, 30).unwrap().unwrap();
            assert_eq!(record.payload(), format!("std_task_{}", i).as_bytes());
        }
    }
    let _ = fs::remove_dir_all(&dir_std);

    // 2. Full hardware flush mode
    let dir_full = temp_data_dir("sync_mode_full");
    let cfg_full = GroupCommitConfig {
        batch_window: Duration::from_millis(5),
        max_batch_size: 16,
        idle_commit_threshold: Duration::ZERO,
        sync_mode: SyncMode::FullHardware,
    };
    {
        let engine = Engine::open_with_config(&dir_full, cfg_full).unwrap();
        assert_eq!(engine.config().sync_mode, SyncMode::FullHardware);
        for i in 0..5 {
            engine.push(format!("full_task_{}", i).into_bytes(), 1).unwrap();
        }
        engine.sync().unwrap();
    }
    {
        let recovered = Engine::open_with_config(&dir_full, cfg_full).unwrap();
        let (ready, _) = recovered.status();
        assert_eq!(ready, 5);
        for i in 0..5 {
            let (record, _) = recovered.pop_and_lease(1, 30).unwrap().unwrap();
            assert_eq!(record.payload(), format!("full_task_{}", i).as_bytes());
        }
    }
    let _ = fs::remove_dir_all(&dir_full);
}

