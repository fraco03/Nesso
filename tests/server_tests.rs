use axum::serve;
use base64::prelude::*;
use serde_json::json;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_data_dir(test_name: &str) -> PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut path = env::temp_dir();
    path.push(format!("nesso_server_test_{}_{}", test_name, count));
    let _ = fs::remove_dir_all(&path);
    let _ = fs::create_dir_all(&path);
    path
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

#[tokio::test]
async fn test_http_crud_flow() {
    let dir = temp_data_dir("crud_flow");
    let base_url = spawn_test_server(dir).await;
    let client = reqwest::Client::new();

    // 1. Health check
    let res = client.get(format!("{}/health", base_url)).send().await.unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.text().await.unwrap(), "OK");

    // 2. Status on non-existent queue -> 404 NOT_FOUND
    let res = client.get(format!("{}/v1/queues/test_q/status", base_url)).send().await.unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::NOT_FOUND);

    // 3. Push a task (implicitly creates the queue)
    let payload = BASE64_STANDARD.encode(b"hello_nesso_http");
    let res = client
        .post(format!("{}/v1/queues/test_q/push", base_url))
        .json(&json!({
            "payload": payload,
            "priority": 5
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::CREATED);
    let push_resp: serde_json::Value = res.json().await.unwrap();
    let task_id = push_resp["id"].as_u64().unwrap();
    assert_eq!(task_id, 1);

    // 4. Status post-push
    let res = client.get(format!("{}/v1/queues/test_q/status", base_url)).send().await.unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let status_resp: serde_json::Value = res.json().await.unwrap();
    assert_eq!(status_resp["ready_tasks"], 1);
    assert_eq!(status_resp["active_leases"], 0);

    // 5. Pop the task
    let res = client
        .post(format!("{}/v1/queues/test_q/pop", base_url))
        .json(&json!({
            "consumer_id": 100,
            "lease_secs": 10,
            "wait_secs": 0
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let pop_resp: serde_json::Value = res.json().await.unwrap();
    assert_eq!(pop_resp["id"], 1);
    assert_eq!(pop_resp["priority"], 5);
    let rec_payload = BASE64_STANDARD.decode(pop_resp["payload"].as_str().unwrap()).unwrap();
    assert_eq!(rec_payload, b"hello_nesso_http");

    // 6. Pop on empty queue -> 204 No Content
    let res = client
        .post(format!("{}/v1/queues/test_q/pop", base_url))
        .json(&json!({
            "consumer_id": 100,
            "lease_secs": 10,
            "wait_secs": 0
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::NO_CONTENT);

    // 7. Ack the task
    let res = client
        .post(format!("{}/v1/queues/test_q/tasks/{}/ack", base_url, task_id))
        .json(&json!({
            "consumer_id": 100
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);

    // 8. Final status: empty queue, zero active leases
    let res = client.get(format!("{}/v1/queues/test_q/status", base_url)).send().await.unwrap();
    let status_resp: serde_json::Value = res.json().await.unwrap();
    assert_eq!(status_resp["ready_tasks"], 0);
    assert_eq!(status_resp["active_leases"], 0);
}

#[tokio::test]
async fn test_http_compact_endpoint() {
    let dir = temp_data_dir("compact_endpoint");
    let base_url = spawn_test_server(dir).await;
    let client = reqwest::Client::new();

    // Push task 1
    let p = BASE64_STANDARD.encode(b"task1");
    let _ = client
        .post(format!("{}/v1/queues/compact_q/push", base_url))
        .json(&json!({ "payload": p, "priority": 1 }))
        .send()
        .await
        .unwrap();

    // Call compact endpoint: with only a single active segment this is a no-op -> false
    let res = client
        .post(format!("{}/v1/queues/compact_q/compact", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let resp: serde_json::Value = res.json().await.unwrap();
    assert_eq!(resp["compacted"], false);

    // Compact on non-existent queue -> 404 Not Found
    let res = client
        .post(format!("{}/v1/queues/non_existent_q/compact", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_http_long_polling_wakeup() {
    let dir = temp_data_dir("long_polling_wakeup");
    let base_url = spawn_test_server(dir).await;
    let client = reqwest::Client::new();

    let client_clone = client.clone();
    let url_clone = base_url.clone();

    let start = Instant::now();

    // Consumer begins long-polling with wait_secs: 3 on empty queue
    let pop_handle = tokio::spawn(async move {
        client_clone
            .post(format!("{}/v1/queues/lp_q/pop", url_clone))
            .json(&json!({
                "consumer_id": 42,
                "lease_secs": 10,
                "wait_secs": 3
            }))
            .send()
            .await
            .unwrap()
    });

    // After 100ms, producer pushes a task
    tokio::time::sleep(Duration::from_millis(100)).await;
    let payload = BASE64_STANDARD.encode(b"woken_up_task");
    let push_res = client
        .post(format!("{}/v1/queues/lp_q/push", base_url))
        .json(&json!({
            "payload": payload,
            "priority": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(push_res.status(), reqwest::StatusCode::CREATED);

    // Consumer must wake up well before the 3-second timeout!
    let pop_res = pop_handle.await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(pop_res.status(), reqwest::StatusCode::OK);
    let pop_json: serde_json::Value = pop_res.json().await.unwrap();
    let payload_decoded = BASE64_STANDARD.decode(pop_json["payload"].as_str().unwrap()).unwrap();
    assert_eq!(payload_decoded, b"woken_up_task");

    // Verify wakeup was prompt (within 1000ms, not 3000ms)
    assert!(
        elapsed < Duration::from_millis(1000),
        "Consumer should wake up promptly upon push, took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_http_long_polling_timeout() {
    let dir = temp_data_dir("long_polling_timeout");
    let base_url = spawn_test_server(dir).await;
    let client = reqwest::Client::new();

    let start = Instant::now();
    let res = client
        .post(format!("{}/v1/queues/timeout_q/pop", base_url))
        .json(&json!({
            "consumer_id": 42,
            "lease_secs": 10,
            "wait_secs": 1
        }))
        .send()
        .await
        .unwrap();

    let elapsed = start.elapsed();
    // Must return 204 NO_CONTENT after approximately 1 second
    assert_eq!(res.status(), reqwest::StatusCode::NO_CONTENT);
    assert!(
        elapsed >= Duration::from_millis(900),
        "Must wait at least ~1s before timing out, took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_http_long_polling_wakeup_on_nack() {
    let dir = temp_data_dir("long_polling_nack");
    let base_url = spawn_test_server(dir).await;
    let client = reqwest::Client::new();

    // 1. Push a task
    let p = BASE64_STANDARD.encode(b"task_to_nack");
    let push_res = client
        .post(format!("{}/v1/queues/nack_q/push", base_url))
        .json(&json!({ "payload": p, "priority": 1 }))
        .send()
        .await
        .unwrap();
    let push_json: serde_json::Value = push_res.json().await.unwrap();
    let task_id = push_json["id"].as_u64().unwrap();

    // 2. Consumer 1 acquires lease
    let pop_res = client
        .post(format!("{}/v1/queues/nack_q/pop", base_url))
        .json(&json!({ "consumer_id": 1, "lease_secs": 10, "wait_secs": 0 }))
        .send()
        .await
        .unwrap();
    assert_eq!(pop_res.status(), reqwest::StatusCode::OK);

    // 3. Consumer 2 enters long-polling waiting for a task (wait_secs: 3)
    let client_clone = client.clone();
    let url_clone = base_url.clone();
    let start = Instant::now();
    let pop_handle = tokio::spawn(async move {
        client_clone
            .post(format!("{}/v1/queues/nack_q/pop", url_clone))
            .json(&json!({ "consumer_id": 2, "lease_secs": 10, "wait_secs": 3 }))
            .send()
            .await
            .unwrap()
    });

    // 4. Consumer 1 nacks the task after 100ms
    tokio::time::sleep(Duration::from_millis(100)).await;
    let nack_res = client
        .post(format!("{}/v1/queues/nack_q/tasks/{}/nack", base_url, task_id))
        .json(&json!({ "consumer_id": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(nack_res.status(), reqwest::StatusCode::OK);

    // 5. Consumer 2 must wake up immediately receiving the nacked task!
    let pop2_res = pop_handle.await.unwrap();
    let elapsed = start.elapsed();
    assert_eq!(pop2_res.status(), reqwest::StatusCode::OK);
    let pop2_json: serde_json::Value = pop2_res.json().await.unwrap();
    assert_eq!(pop2_json["id"], task_id);
    assert_eq!(pop2_json["retries"], 1);
    assert!(
        elapsed < Duration::from_millis(1000),
        "Consumer 2 should wake up promptly on nack, took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_http_long_polling_wakeup_on_expiration() {
    let dir = temp_data_dir("long_polling_expiration");
    let base_url = spawn_test_server(dir).await;
    let client = reqwest::Client::new();

    // 1. Push a task
    let p = BASE64_STANDARD.encode(b"task_to_expire");
    let push_res = client
        .post(format!("{}/v1/queues/expire_q/push", base_url))
        .json(&json!({ "payload": p, "priority": 1 }))
        .send()
        .await
        .unwrap();
    let push_json: serde_json::Value = push_res.json().await.unwrap();
    let task_id = push_json["id"].as_u64().unwrap();

    // 2. Consumer 1 acquires lease for only 1 second and does NOT ack/nack it
    let pop_res = client
        .post(format!("{}/v1/queues/expire_q/pop", base_url))
        .json(&json!({ "consumer_id": 1, "lease_secs": 1, "wait_secs": 0 }))
        .send()
        .await
        .unwrap();
    assert_eq!(pop_res.status(), reqwest::StatusCode::OK);

    // 3. Consumer 2 enters long-polling with wait_secs: 5
    let client_clone = client.clone();
    let url_clone = base_url.clone();
    let start = Instant::now();
    let pop_handle = tokio::spawn(async move {
        client_clone
            .post(format!("{}/v1/queues/expire_q/pop", url_clone))
            .json(&json!({ "consumer_id": 2, "lease_secs": 10, "wait_secs": 5 }))
            .send()
            .await
            .unwrap()
    });

    // 4. Background engine expiration thread expires the lease (after ~1s)
    // and notifies consumer 2 in long-polling!
    let pop2_res = pop_handle.await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(pop2_res.status(), reqwest::StatusCode::OK);
    let pop2_json: serde_json::Value = pop2_res.json().await.unwrap();
    assert_eq!(pop2_json["id"], task_id);
    assert_eq!(pop2_json["retries"], 1);

    // Verify consumer woke up promptly upon expiration (around 1-2.5s, well before 5s)
    assert!(
        elapsed < Duration::from_millis(3000),
        "Consumer 2 should wake up promptly when lease expires in background, took {:?}",
        elapsed
    );
}
