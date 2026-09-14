use nesso::storage::engine::Engine;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn child_worker_entry() {
    if std::env::var("NESSO_CRASH_TEST_CHILD").is_err() {
        // Parent test runner: returns immediately with success
        return;
    }

    let queue_dir = std::env::var("NESSO_QUEUE_DIR").expect("missing NESSO_QUEUE_DIR");
    let control_dir = std::env::var("NESSO_CONTROL_DIR").expect("missing NESSO_CONTROL_DIR");
    let num_threads: usize = std::env::var("NESSO_NUM_THREADS")
        .unwrap_or_else(|_| "4".to_string())
        .parse()
        .unwrap();

    let engine = Arc::new(Engine::open(&queue_dir, None).unwrap());
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let engine_clone = Arc::clone(&engine);
        let ctrl_path = PathBuf::from(&control_dir).join(format!("confirmed_t{}.txt", t));

        handles.push(thread::spawn(move || {
            let mut ctrl_file = OpenOptions::new()
                .create(true)
                .append(true)
                .write(true)
                .open(&ctrl_path)
                .unwrap();

            let mut i = 0u64;
            loop {
                let payload = format!("child_data_t{}_{}", t, i).into_bytes();
                let id = engine_clone.push(payload, 1).unwrap();
                engine_clone.sync().unwrap();

                // ONLY AFTER sync() returns Ok(()), we record the confirmed ID
                // and force its persistence to the control file.
                writeln!(ctrl_file, "{}", id).unwrap();
                ctrl_file.flush().unwrap();
                let _ = ctrl_file.sync_data();

                i += 1;
            }
        }));
    }

    for h in handles {
        let _ = h.join();
    }
}

#[test]
fn test_kill_9_crash_recovery() {
    if std::env::var("NESSO_CRASH_TEST_CHILD").is_ok() {
        return;
    }

    let test_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_base = std::env::temp_dir().join(format!("nesso_crash_test_{}", test_id));
    let queue_dir = temp_base.join("queue");
    let control_dir = temp_base.join("control");
    fs::create_dir_all(&queue_dir).unwrap();
    fs::create_dir_all(&control_dir).unwrap();

    let exe = std::env::current_exe().unwrap();
    let num_threads = 4;

    // Spawn child process running the workload
    let mut child = Command::new(exe)
        .arg("child_worker_entry")
        .arg("--nocapture")
        .arg("--exact")
        .env("NESSO_CRASH_TEST_CHILD", "1")
        .env("NESSO_QUEUE_DIR", queue_dir.to_str().unwrap())
        .env("NESSO_CONTROL_DIR", control_dir.to_str().unwrap())
        .env("NESSO_NUM_THREADS", num_threads.to_string())
        .spawn()
        .unwrap();

    // Allow workers to push & sync concurrently for 250ms
    thread::sleep(Duration::from_millis(250));

    // Abruptly terminate the process with SIGKILL (kill -9)
    child.kill().expect("Failed to send SIGKILL to child");
    let _ = child.wait();

    // Read all confirmed IDs from the control files
    let mut confirmed_ids = Vec::new();
    for entry in fs::read_dir(&control_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().and_then(|s| s.to_str()) == Some("txt") {
            let content = fs::read_to_string(entry.path()).unwrap();
            for line in content.lines() {
                if let Ok(id) = line.trim().parse::<u64>() {
                    confirmed_ids.push(id);
                }
            }
        }
    }

    // Verify operations were actually dispatched and confirmed before kill -9
    assert!(
        !confirmed_ids.is_empty(),
        "Expected at least some operations to be confirmed before SIGKILL"
    );

    // Verify recovery: open the queue directory
    let engine = Engine::open(&queue_dir, None).expect("Engine recovery failed after SIGKILL");

    // Verify EVERY confirmed ID is present in the recovered engine's index
    let state = engine.inner.lock().unwrap();
    for id in &confirmed_ids {
        assert!(
            state.index.contains_key(id),
            "Confirmed ID {} was not found in recovered engine index!",
            id
        );
        assert!(
            state.data_index.contains_key(id),
            "Confirmed ID {} was not found in recovered engine data_index!",
            id
        );
    }
    drop(state);

    // Verify engine remains fully operational post-crash
    let new_id = engine
        .push(b"post_crash_test".to_vec(), 2)
        .expect("Failed to push after recovery");
    engine.sync().expect("Failed to sync after recovery");
    assert!(new_id > *confirmed_ids.iter().max().unwrap_or(&0));

    // Clean up temporary test directories
    let _ = fs::remove_dir_all(&temp_base);
}
