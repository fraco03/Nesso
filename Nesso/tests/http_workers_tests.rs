use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use axum::serve;
use base64::prelude::*;
use reqwest::StatusCode;
use serde_json::json;
use tokio::net::TcpListener;

use nesso::server::{self, AppState};

fn temp_data_dir(test_name: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let unique_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.push(format!("nesso_http_workers_{}_{}", test_name, unique_id));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_single_http_worker_concurrency_no_deadlock() {
    // Construct a multi-thread Tokio runtime explicitly restricted to exactly 1 worker thread.
    // This strictly verifies that asynchronous event loops, background expiration threads,
    // and group commit disk synchronization do not deadlock the single worker thread.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("Failed to build single-worker Tokio runtime");

    runtime.block_on(async {
        let dir = temp_data_dir("single_worker");
        let state = AppState::new(dir.clone());
        let shutdown_token = state.shutdown_token.clone();
        let app = server::create_router_with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{}", addr);

        let server_token = shutdown_token.clone();
        let server_handle = tokio::spawn(async move {
            serve(listener, app)
                .with_graceful_shutdown(server::shutdown_signal(server_token))
                .await
                .unwrap();
        });

        let client = reqwest::Client::new();

        // 1. Verify health check on single worker
        let health_res = client
            .get(format!("{}/health", base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(health_res.status(), StatusCode::OK);

        // 2. Spawn concurrent long-poller waiting for a task (wait_secs = 5)
        let client_lp = client.clone();
        let lp_url = format!("{}/v1/queues/single_worker_q/pop", base_url);
        let lp_handle = tokio::spawn(async move {
            let res = client_lp
                .post(&lp_url)
                .json(&json!({
                    "consumer_id": 42,
                    "lease_secs": 10,
                    "wait_secs": 5
                }))
                .send()
                .await
                .unwrap();
            res
        });

        // Small yield to let long-polling request enter waiting state
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 3. Concurrently push a task with ?sync=true
        let raw_payload = b"single_worker_task_payload";
        let encoded_payload = BASE64_STANDARD.encode(raw_payload);
        let push_res = client
            .post(format!("{}/v1/queues/single_worker_q/push?sync=true", base_url))
            .json(&json!({
                "payload": encoded_payload,
                "priority": 10
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(push_res.status(), StatusCode::CREATED);
        let push_body: serde_json::Value = push_res.json().await.unwrap();
        let pushed_id = push_body["id"].as_u64().unwrap();

        // 4. Long-poller must have awakened and popped the task
        let pop_res = lp_handle.await.unwrap();
        assert_eq!(pop_res.status(), StatusCode::OK);
        let pop_body: serde_json::Value = pop_res.json().await.unwrap();
        assert_eq!(pop_body["id"].as_u64().unwrap(), pushed_id);
        assert_eq!(pop_body["payload"].as_str().unwrap(), encoded_payload);

        // 5. Acknowledge task with ?sync=true
        let ack_res = client
            .post(format!(
                "{}/v1/queues/single_worker_q/tasks/{}/ack?sync=true",
                base_url, pushed_id
            ))
            .json(&json!({ "consumer_id": 42 }))
            .send()
            .await
            .unwrap();
        assert_eq!(ack_res.status(), StatusCode::OK);

        // 6. Verify status shows 0 ready and 0 leased
        let status_res = client
            .get(format!("{}/v1/queues/single_worker_q/status", base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(status_res.status(), StatusCode::OK);
        let status_body: serde_json::Value = status_res.json().await.unwrap();
        assert_eq!(status_body["ready_tasks"].as_u64().unwrap(), 0);
        assert_eq!(status_body["active_leases"].as_u64().unwrap(), 0);

        // 7. Clean shutdown
        shutdown_token.cancel();
        server_handle.await.unwrap();
        state.shutdown_all_queues().await.unwrap();

        let _ = fs::remove_dir_all(&dir);
    });
}

#[test]
fn test_single_http_worker_multiple_queues_concurrency() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("Failed to build single-worker Tokio runtime");

    runtime.block_on(async {
        let dir = temp_data_dir("single_worker_multi_queue");
        let state = AppState::new(dir.clone());
        let shutdown_token = state.shutdown_token.clone();
        let app = server::create_router_with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{}", addr);

        let server_token = shutdown_token.clone();
        let server_handle = tokio::spawn(async move {
            serve(listener, app)
                .with_graceful_shutdown(server::shutdown_signal(server_token))
                .await
                .unwrap();
        });

        let client = reqwest::Client::new();
        let num_queues = 4;
        let mut join_handles = Vec::new();

        for q in 0..num_queues {
            let client = client.clone();
            let base_url = base_url.clone();
            join_handles.push(tokio::spawn(async move {
                let queue_name = format!("queue_{}", q);
                for i in 0..5 {
                    let payload = BASE64_STANDARD.encode(format!("payload_{}_{}", q, i).as_bytes());
                    let res = client
                        .post(format!("{}/v1/queues/{}/push?sync=true", base_url, queue_name))
                        .json(&json!({ "payload": payload, "priority": 1 }))
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(res.status(), StatusCode::CREATED);
                }

                // Pop and ack all 5 items
                for _ in 0..5 {
                    let pop_res = client
                        .post(format!("{}/v1/queues/{}/pop", base_url, queue_name))
                        .json(&json!({ "consumer_id": 99, "lease_secs": 10 }))
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(pop_res.status(), StatusCode::OK);
                    let body: serde_json::Value = pop_res.json().await.unwrap();
                    let id = body["id"].as_u64().unwrap();

                    let ack_res = client
                        .post(format!("{}/v1/queues/{}/tasks/{}/ack?sync=true", base_url, queue_name, id))
                        .json(&json!({ "consumer_id": 99 }))
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(ack_res.status(), StatusCode::OK);
                }
            }));
        }

        for h in join_handles {
            h.await.unwrap();
        }

        // Clean shutdown
        shutdown_token.cancel();
        server_handle.await.unwrap();
        state.shutdown_all_queues().await.unwrap();

        let _ = fs::remove_dir_all(&dir);
    });
}
