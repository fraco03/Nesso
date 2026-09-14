use nesso::storage::engine::Engine;
use std::env;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("==================================================");
    println!("   Nesso — Embedded Persistent Queue Demo         ");
    println!("==================================================");

    let mut data_dir = env::temp_dir();
    data_dir.push("nesso_demo_worker_pool");
    let _ = fs::remove_dir_all(&data_dir);
    fs::create_dir_all(&data_dir)?;

    println!("1. Initializing Engine at {:?}", data_dir);
    let engine = Arc::new(Engine::open(&data_dir, None)?);

    // ----------------------------------------------------
    // Producer: Enqueue 12 tasks with mixed priorities (1..=3)
    // ----------------------------------------------------
    println!("\n2. Producer: enqueuing 12 tasks with varying priorities...");
    for i in 1..=12 {
        let priority = match i % 3 {
            0 => 3, // High priority
            1 => 1, // Low priority
            _ => 2, // Medium priority
        };
        let payload = format!(r#"{{"task_id": {}, "action": "process_image", "item": "img_{}.png"}}"#, i, i);
        let id = engine.push(payload.into_bytes(), priority)?;
        println!("   -> Enqueued Task #{:02} (priority: {})", id, priority);
    }

    let (ready, leased) = engine.status();
    println!("   Initial queue status: {} ready tasks, {} in lease", ready, leased);

    // ----------------------------------------------------
    // Worker Pool: 3 concurrent workers with lease and retry
    // ----------------------------------------------------
    println!("\n3. Spawning 3 concurrent workers...");
    let running = Arc::new(AtomicBool::new(true));
    let mut workers = Vec::new();

    for worker_id in 1..=3 {
        let engine_clone = Arc::clone(&engine);
        let running_clone = Arc::clone(&running);

        let handle = thread::spawn(move || {
            while running_clone.load(Ordering::Relaxed) {
                // Try acquiring a task with 5-second lease
                match engine_clone.pop_and_lease(worker_id, 5) {
                    Ok(Some((record, retries))) => {
                        let payload_str = String::from_utf8_lossy(record.payload());
                        println!(
                            "   [Worker {}] Leased Task #{} (priority: {}, retry: {}) -> {}",
                            worker_id,
                            record.id(),
                            record.priority(),
                            retries,
                            payload_str
                        );

                        // Simulate work
                        thread::sleep(Duration::from_millis(60));

                        // Simulate transient failure for task #4 on first attempt
                        if record.id() == 4 && retries == 0 {
                            println!("   [Worker {}] Task #4 failed transiently! Sending NACK...", worker_id);
                            let _ = engine_clone.nack(record.id(), worker_id);
                        } else {
                            println!("   [Worker {}] Task #{} completed successfully -> ACK", worker_id, record.id());
                            let _ = engine_clone.ack(record.id(), worker_id);
                        }
                    }
                    Ok(None) => {
                        // Queue temporarily empty
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(e) => {
                        eprintln!("   [Worker {}] Error: {:?}", worker_id, e);
                        break;
                    }
                }
            }
        });
        workers.push(handle);
    }

    // Wait until all tasks are consumed
    loop {
        let (ready, leased) = engine.status();
        if ready == 0 && leased == 0 {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    // Stop workers
    running.store(false, Ordering::Relaxed);
    for w in workers {
        let _ = w.join();
    }

    println!("\n4. All tasks have been processed.");
    let (ready, leased) = engine.status();
    println!("   Queue status: {} ready, {} in lease", ready, leased);

    // ----------------------------------------------------
    // Execute Compaction to clean up the log
    // ----------------------------------------------------
    println!("\n5. Executing Compaction on closed segments...");
    let compacted = engine.compact()?;
    println!("   Compaction result: {}", if compacted { "Segments consolidated successfully" } else { "No old segments to compact" });

    println!("\n6. Demo completed successfully!");
    let _ = fs::remove_dir_all(&data_dir);
    Ok(())
}
