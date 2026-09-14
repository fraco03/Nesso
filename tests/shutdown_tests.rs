use axum::serve;
use base64::prelude::*;
use base64::Engine as _;
use serde_json::json;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

use nesso::server::{self, AppState};
use nesso::storage::engine::Engine;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_data_dir(test_name: &str) -> PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!("nesso_shutdown_test_{}_{}", test_name, count));
    let _ = fs::remove_dir_all(&path);
    let _ = fs::create_dir_all(&path);
    path
}

// -----------------------------------------------------------------------------
// Test 1: Push with sync=true during shutdown
// A client sends a push request with sync=true while a shutdown is triggered
// concurrently. The forced group commit flush ensures the batch is durably
// fsync-ed before termination, and the client receives a successful 201 response.
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_shutdown_push_sync_completes() {
    let dir = temp_data_dir("push_sync_completes");
    let state = AppState::new(dir.clone());
    let shutdown_token = state.shutdown_token.clone();
    let app = server::create_router_with_state(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn({
        let shutdown_token = shutdown_token.clone();
        async move {
            serve(listener, app)
                .with_graceful_shutdown(server::shutdown_signal(shutdown_token))
                .await
                .unwrap();
        }
    });

    let base_url = format!("http://{}", addr);
    let client = reqwest::Client::new();

    // Ensure server is ready to accept requests
    for _ in 0..50 {
        if let Ok(res) = client.get(format!("{}/health", base_url)).send().await {
            if res.status() == reqwest::StatusCode::OK {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Spawn the push request in a concurrent task so it connects and sends in flight
    let payload = BASE64_STANDARD.encode(b"persistent_task_during_shutdown");
    let push_url = format!("{}/v1/queues/shutdown_q/push?sync=true", base_url);
    let push_handle = tokio::spawn(async move {
        client
            .post(push_url)
            .json(&json!({
                "payload": payload,
                "priority": 10
            }))
            .send()
            .await
    });

    // Small delay to ensure the request is in-flight on the server, then trigger shutdown
    tokio::time::sleep(Duration::from_millis(10)).await;
    shutdown_token.cancel();

    let push_res = push_handle.await.unwrap().unwrap();
    assert_eq!(push_res.status(), reqwest::StatusCode::CREATED);
    let push_json: serde_json::Value = push_res.json().await.unwrap();
    let task_id = push_json["id"].as_u64().unwrap();
    assert!(task_id > 0);

    // Wait for server to finish graceful drain
    server_handle.await.unwrap();

    // Flush remaining queues and stop workers
    state.shutdown_all_queues().await.unwrap();

    // Verify task is durably persisted on disk
    let queue_dir = dir.join("shutdown_q");
    let recovered_engine = Engine::open(&queue_dir, None).unwrap();
    let (ready, leased) = recovered_engine.status();
    assert_eq!(ready, 1);
    assert_eq!(leased, 0);

    let (popped_rec, _) = recovered_engine.pop_and_lease(99, 10).unwrap().unwrap();
    assert_eq!(popped_rec.id(), task_id);
    assert_eq!(popped_rec.payload(), b"persistent_task_during_shutdown");
}

// -----------------------------------------------------------------------------
// Test 2: Long-polling prompt wakeup with HTTP 503
// A consumer enters long-polling with wait_secs: 30 on an empty queue.
// When shutdown is triggered after 500ms, the consumer must be woken up
// immediately and receive an explicit 503 Service Unavailable response with
// {"error": "server_shutting_down"}, instead of waiting 30 seconds or getting
// an unhandled connection reset.
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_shutdown_long_polling_wakes_with_503() {
    let dir = temp_data_dir("long_polling_503");
    let state = AppState::new(dir.clone());
    let shutdown_token = state.shutdown_token.clone();
    let app = server::create_router_with_state(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn({
        let shutdown_token = shutdown_token.clone();
        async move {
            serve(listener, app)
                .with_graceful_shutdown(server::shutdown_signal(shutdown_token))
                .await
                .unwrap();
        }
    });

    let base_url = format!("http://{}", addr);
    let client = reqwest::Client::new();

    // Ensure server is ready to accept requests
    for _ in 0..50 {
        if let Ok(res) = client.get(format!("{}/health", base_url)).send().await {
            if res.status() == reqwest::StatusCode::OK {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let start = Instant::now();

    // Consumer enters 30-second long-polling
    let pop_handle = tokio::spawn(async move {
        client
            .post(format!("{}/v1/queues/empty_lp_q/pop", base_url))
            .json(&json!({
                "consumer_id": 42,
                "lease_secs": 10,
                "wait_secs": 30
            }))
            .send()
            .await
            .unwrap()
    });

    // Sleep 500ms to ensure consumer is parked in tokio::select!
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Trigger shutdown
    shutdown_token.cancel();

    let pop_res = pop_handle.await.unwrap();
    let elapsed = start.elapsed();

    // Must return 503 SERVICE_UNAVAILABLE
    assert_eq!(pop_res.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = pop_res.json().await.unwrap();
    assert_eq!(body["error"], "server_shutting_down");

    // Must complete well within the 30-second window (e.g. < 2s)
    assert!(
        elapsed < Duration::from_secs(2),
        "Long-polling pop should wake up immediately on shutdown, took {:?}",
        elapsed
    );

    server_handle.await.unwrap();
    state.shutdown_all_queues().await.unwrap();
}

// -----------------------------------------------------------------------------
// Test 3: Expiration threads cleanly terminate and join
// Verifies that each Engine's background expiration thread exits promptly upon
// shutdown (sub-millisecond wakeup via Condvar) and joins without hanging.
// -----------------------------------------------------------------------------
#[test]
fn test_shutdown_expiration_threads_terminate() {
    let dir = temp_data_dir("expiration_thread_term");
    let engine = Engine::open(&dir, None).unwrap();

    // Verify expiration worker has a running thread handle
    assert!(engine.expiration_worker.handle.lock().unwrap().is_some());

    let start = Instant::now();
    engine.shutdown().unwrap();
    let elapsed = start.elapsed();

    // Expiration thread must join promptly
    assert!(
        elapsed < Duration::from_millis(500),
        "Expiration thread should join promptly, took {:?}",
        elapsed
    );

    // Handle is taken and joined
    assert!(engine.expiration_worker.handle.lock().unwrap().is_none());

    // Calling shutdown again is idempotent
    assert!(engine.shutdown().is_ok());
}

// -----------------------------------------------------------------------------
// Test 4: Data integrity across restart after clean shutdown
// Pushes multiple tasks across multiple queues with mixed sync flags.
// Shuts down cleanly, then reopens from cold storage, verifying all confirmed
// tasks are present and uncorrupted.
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_shutdown_data_integrity_post_restart() {
    let dir = temp_data_dir("data_integrity_restart");
    let state = AppState::new(dir.clone());
    let shutdown_token = state.shutdown_token.clone();
    let app = server::create_router_with_state(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn({
        let shutdown_token = shutdown_token.clone();
        async move {
            serve(listener, app)
                .with_graceful_shutdown(server::shutdown_signal(shutdown_token))
                .await
                .unwrap();
        }
    });

    let base_url = format!("http://{}", addr);
    let client = reqwest::Client::new();

    // Ensure server is ready to accept requests
    for _ in 0..50 {
        if let Ok(res) = client.get(format!("{}/health", base_url)).send().await {
            if res.status() == reqwest::StatusCode::OK {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut task_ids = Vec::new();

    // Push 10 tasks to queue A and 10 to queue B
    for i in 0..10 {
        let payload = BASE64_STANDARD.encode(format!("payload_A_{}", i).as_bytes());
        let res = client
            .post(format!("{}/v1/queues/queue_A/push?sync={}", base_url, i % 2 == 0))
            .json(&json!({ "payload": payload, "priority": (i % 5) as u8 }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), reqwest::StatusCode::CREATED);
        let resp: serde_json::Value = res.json().await.unwrap();
        task_ids.push(("queue_A", resp["id"].as_u64().unwrap(), format!("payload_A_{}", i)));
    }

    for i in 0..10 {
        let payload = BASE64_STANDARD.encode(format!("payload_B_{}", i).as_bytes());
        let res = client
            .post(format!("{}/v1/queues/queue_B/push?sync=true", base_url))
            .json(&json!({ "payload": payload, "priority": 1 }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), reqwest::StatusCode::CREATED);
        let resp: serde_json::Value = res.json().await.unwrap();
        task_ids.push(("queue_B", resp["id"].as_u64().unwrap(), format!("payload_B_{}", i)));
    }

    // Trigger graceful shutdown
    shutdown_token.cancel();
    server_handle.await.unwrap();
    state.shutdown_all_queues().await.unwrap();

    // Reopen engines from cold disk
    let engine_a = Engine::open(dir.join("queue_A"), None).unwrap();
    let engine_b = Engine::open(dir.join("queue_B"), None).unwrap();

    let (ready_a, leased_a) = engine_a.status();
    let (ready_b, leased_b) = engine_b.status();

    assert_eq!(ready_a, 10);
    assert_eq!(leased_a, 0);
    assert_eq!(ready_b, 10);
    assert_eq!(leased_b, 0);

    // Verify all 10 tasks in queue_B can be popped and payloads match
    for _ in 0..10 {
        let (rec, _) = engine_b.pop_and_lease(1, 10).unwrap().unwrap();
        let expected = format!("payload_B_{}", rec.id() - 1);
        assert_eq!(rec.payload(), expected.as_bytes());
    }

    // Clean shutdown of test engines
    engine_a.shutdown().unwrap();
    engine_b.shutdown().unwrap();
}
