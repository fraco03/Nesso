use axum::{
    extract::{Path, State, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::Engine as B64Engine;
use base64::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use crate::storage::engine::{Engine, GroupCommitConfig};

#[derive(Clone)]
pub struct QueueContext {
    pub engine: Engine,
    pub notify: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
pub struct AppState {
    pub data_dir: PathBuf,
    pub queues: Arc<Mutex<HashMap<String, QueueContext>>>,
    pub shutdown_token: CancellationToken,
    pub group_commit_config: GroupCommitConfig,
}

impl AppState {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            queues: Arc::new(Mutex::new(HashMap::new())),
            shutdown_token: CancellationToken::new(),
            group_commit_config: GroupCommitConfig::default(),
        }
    }

    pub fn new_with_config(data_dir: PathBuf, group_commit_config: GroupCommitConfig) -> Self {
        Self {
            data_dir,
            queues: Arc::new(Mutex::new(HashMap::new())),
            shutdown_token: CancellationToken::new(),
            group_commit_config,
        }
    }

    pub fn with_shutdown_token(data_dir: PathBuf, shutdown_token: CancellationToken) -> Self {
        Self {
            data_dir,
            queues: Arc::new(Mutex::new(HashMap::new())),
            shutdown_token,
            group_commit_config: GroupCommitConfig::default(),
        }
    }

    pub fn with_shutdown_token_and_config(
        data_dir: PathBuf,
        shutdown_token: CancellationToken,
        group_commit_config: GroupCommitConfig,
    ) -> Self {
        Self {
            data_dir,
            queues: Arc::new(Mutex::new(HashMap::new())),
            shutdown_token,
            group_commit_config,
        }
    }

    /// Shutdown all active queues: forces group commit flush to disk,
    /// then stops and joins background expiration threads.
    pub async fn shutdown_all_queues(&self) -> std::io::Result<()> {
        let queues: Vec<(String, QueueContext)> = {
            let q = self.queues.lock().unwrap();
            q.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        for (_name, ctx) in queues {
            let engine = ctx.engine.clone();
            tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                engine.force_flush_group_commit()?;
                engine.stop_expiration_thread();
                Ok(())
            })
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))??;
        }

        Ok(())
    }

    pub fn get_or_create_queue(&self, name: &str) -> std::io::Result<QueueContext> {
        let mut queues = self.queues.lock().unwrap();
        if let Some(ctx) = queues.get(name) {
            return Ok(ctx.clone());
        }

        let queue_dir = self.data_dir.join(name);
        if !queue_dir.exists() {
            std::fs::create_dir_all(&queue_dir)?;
        }

        let engine = Engine::open(&queue_dir, Some(self.group_commit_config))?;
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_clone = Arc::clone(&notify);
        engine.set_on_task_ready(move || {
            notify_clone.notify_one();
        });
        let ctx = QueueContext {
            engine,
            notify,
        };
        queues.insert(name.to_string(), ctx.clone());
        Ok(ctx)
    }

    pub fn get_existing_queue(&self, name: &str) -> Result<QueueContext, ApiError> {
        {
            let queues = self.queues.lock().unwrap();
            if let Some(ctx) = queues.get(name) {
                return Ok(ctx.clone());
            }
        }
        let queue_dir = self.data_dir.join(name);
        if !queue_dir.exists() {
            return Err(ApiError::NotFound(format!("Queue '{}' not found", name)));
        }
        self.get_or_create_queue(name).map_err(Into::into)
    }
}

// -----------------------------------------
// Data Transfer Objects (DTOs)
// -----------------------------------------

#[derive(Deserialize, Default)]
pub struct SyncQuery {
    #[serde(default)]
    pub sync: bool,
}

#[derive(Deserialize)]
pub struct PushRequest {
    pub payload: String, // base64
    pub priority: u8,
}

#[derive(Serialize)]
pub struct PushResponse {
    pub id: u64,
}

#[derive(Deserialize)]
pub struct PopRequest {
    pub consumer_id: u32,
    pub lease_secs: u64,
    #[serde(default)]
    pub wait_secs: u64,
}

#[derive(Serialize)]
pub struct CompactResponse {
    pub compacted: bool,
}

#[derive(Serialize)]
pub struct PopResponse {
    pub id: u64,
    pub payload: String, // base64
    pub priority: u8,
    pub retries: u8,
}

#[derive(Deserialize)]
pub struct AckNackRequest {
    pub consumer_id: u32,
}

#[derive(Serialize)]
pub struct QueueStatusResponse {
    pub ready_tasks: usize,
    pub active_leases: usize,
}

#[derive(Serialize)]
pub struct QueueListResponse {
    pub queues: Vec<String>,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

// -----------------------------------------
// Error Handling
// -----------------------------------------

pub enum ApiError {
    NotFound(String),
    Conflict(String),
    BadRequest(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error, message) = match self {
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, "not_found", msg),
            ApiError::Conflict(msg) => (StatusCode::CONFLICT, "conflict", msg),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "bad_request", msg),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", msg),
        };

        let body = Json(ErrorResponse {
            error: error.to_string(),
            message,
        });

        (status, body).into_response()
    }
}

impl From<std::io::Error> for ApiError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => ApiError::NotFound(err.to_string()),
            std::io::ErrorKind::PermissionDenied => ApiError::Conflict(err.to_string()),
            _ => ApiError::Internal(err.to_string()),
        }
    }
}

// -----------------------------------------
// Request Handlers
// -----------------------------------------

async fn health_handler() -> &'static str {
    "OK"
}

async fn list_queues_handler(
    State(state): State<AppState>,
) -> Result<Json<QueueListResponse>, ApiError> {
    let dir = state.data_dir.clone();
    let mut queues = tokio::task::spawn_blocking(move || {
        let mut found = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.filter_map(Result::ok) {
                if let Ok(file_type) = entry.file_type() {
                    if file_type.is_dir() {
                        if let Ok(name) = entry.file_name().into_string() {
                            found.push(name);
                        }
                    }
                }
            }
        }
        found
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))?;
    
    queues.sort();
    Ok(Json(QueueListResponse { queues }))
}

async fn status_handler(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
) -> Result<Json<QueueStatusResponse>, ApiError> {
    let queue_ctx = state.get_existing_queue(&queue_name)?;
    let engine = queue_ctx.engine;
    let (ready, active) = tokio::task::spawn_blocking(move || engine.status())
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
        
    Ok(Json(QueueStatusResponse {
        ready_tasks: ready,
        active_leases: active,
    }))
}

async fn compact_handler(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
) -> Result<Json<CompactResponse>, ApiError> {
    let queue_ctx = state.get_existing_queue(&queue_name)?;
    let engine = queue_ctx.engine;

    // =========================================================================
    // ASYNC THREAD ISOLATION:
    // compact() is a heavy synchronous I/O operation (scanning closed segments,
    // consolidating, sync_data, rename, and unlink).
    // It is strictly executed inside spawn_blocking to avoid blocking
    // Tokio's event loop or impacting concurrent requests on other queues.
    // =========================================================================
    let compacted = tokio::task::spawn_blocking(move || -> std::io::Result<bool> {
        engine.compact()
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;

    Ok(Json(CompactResponse { compacted }))
}

async fn push_handler(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Query(sync_query): Query<SyncQuery>,
    Json(req): Json<PushRequest>,
) -> Result<(StatusCode, Json<PushResponse>), ApiError> {
    let queue_ctx = state.get_or_create_queue(&queue_name)?;

    let payload_bytes = BASE64_STANDARD
        .decode(&req.payload)
        .map_err(|e| ApiError::BadRequest(format!("Invalid base64 payload: {}", e)))?;

    let priority = req.priority;
    let engine = queue_ctx.engine.clone();
    let id = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
        let id = engine.push(payload_bytes, priority)?;
        if sync_query.sync { engine.sync()?; }
        Ok(id)
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;

    // Notify any consumer waiting in long-polling
    queue_ctx.notify.notify_one();

    Ok((StatusCode::CREATED, Json(PushResponse { id })))
}

async fn pop_handler(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Query(sync_query): Query<SyncQuery>,
    Json(req): Json<PopRequest>,
) -> Result<Response, ApiError> {
    if state.shutdown_token.is_cancelled() {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "server_shutting_down" })),
        ).into_response());
    }

    let queue_ctx = match state.get_existing_queue(&queue_name) {
        Ok(ctx) => ctx,
        Err(ApiError::NotFound(_)) => {
            if req.wait_secs == 0 {
                return Ok(StatusCode::NO_CONTENT.into_response());
            }
            state.get_or_create_queue(&queue_name)?
        }
        Err(e) => return Err(e),
    };

    let deadline = if req.wait_secs > 0 {
        Some(tokio::time::Instant::now() + std::time::Duration::from_secs(req.wait_secs))
    } else {
        None
    };

    loop {
        if state.shutdown_token.is_cancelled() {
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "server_shutting_down" })),
            ).into_response());
        }

        // =========================================================================
        // NOTIFY / MISSED-WAKEUP PREVENTION:
        // Register the `notified()` future BEFORE checking queue status.
        // If a producer runs push() + notify_one() while we are in spawn_blocking
        // or between check and select!, the wakeup is captured without any loss.
        // =========================================================================
        let notified = queue_ctx.notify.notified();

        let engine = queue_ctx.engine.clone();
        let consumer_id = req.consumer_id;
        let lease_secs = req.lease_secs;
        let sync = sync_query.sync;

        let pop_result = tokio::task::spawn_blocking(move || {
            let res = engine.pop_and_lease(consumer_id, lease_secs)?;
            if sync && res.is_some() { engine.sync()?; }
            Ok::<_, std::io::Error>(res)
        })
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))??;

        if let Some((record, retries)) = pop_result {
            // Task available: `notified` is dropped without waiting
            let payload_b64 = BASE64_STANDARD.encode(record.payload());

            return Ok(Json(PopResponse {
                id: record.id(),
                payload: payload_b64,
                priority: record.priority(),
                retries,
            }).into_response());
        }

        match deadline {
            None => return Ok(StatusCode::NO_CONTENT.into_response()),
            Some(dl) => {
                let now = tokio::time::Instant::now();
                if now >= dl {
                    return Ok(StatusCode::NO_CONTENT.into_response());
                }

                tokio::select! {
                    _ = state.shutdown_token.cancelled() => {
                        return Ok((
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(serde_json::json!({ "error": "server_shutting_down" })),
                        ).into_response());
                    }
                    _ = notified => {
                        // Notification received: retry pop immediately
                    }
                    _ = tokio::time::sleep_until(dl) => {
                        return Ok(StatusCode::NO_CONTENT.into_response());
                    }
                }
            }
        }
    }
}

async fn ack_handler(
    State(state): State<AppState>,
    Path((queue_name, id)): Path<(String, u64)>,
    Query(sync_query): Query<SyncQuery>,
    Json(req): Json<AckNackRequest>,
) -> Result<Response, ApiError> {
    let queue_ctx = state.get_existing_queue(&queue_name)?;
    let engine = queue_ctx.engine;
    let consumer_id = req.consumer_id;
    
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        engine.ack(id, consumer_id)?;
        if sync_query.sync { engine.sync()?; }
        Ok(())
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;
        
    Ok(StatusCode::OK.into_response())
}

async fn nack_handler(
    State(state): State<AppState>,
    Path((queue_name, id)): Path<(String, u64)>,
    Query(sync_query): Query<SyncQuery>,
    Json(req): Json<AckNackRequest>,
) -> Result<Response, ApiError> {
    let queue_ctx = state.get_existing_queue(&queue_name)?;
    let engine = queue_ctx.engine.clone();
    let consumer_id = req.consumer_id;
    
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        engine.nack(id, consumer_id)?;
        if sync_query.sync { engine.sync()?; }
        Ok(())
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;
    
    // Task is back in the ready queue: notify waiting consumers
    queue_ctx.notify.notify_one();
        
    Ok(StatusCode::OK.into_response())
}

pub fn create_router_with_state(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/v1/queues", get(list_queues_handler))
        .route("/v1/queues/{queue_name}/push", post(push_handler))
        .route("/v1/queues/{queue_name}/pop", post(pop_handler))
        .route("/v1/queues/{queue_name}/status", get(status_handler))
        .route("/v1/queues/{queue_name}/compact", post(compact_handler))
        .route("/v1/queues/{queue_name}/tasks/{id}/ack", post(ack_handler))
        .route("/v1/queues/{queue_name}/tasks/{id}/nack", post(nack_handler))
        .with_state(state)
}

pub fn create_router(data_dir: PathBuf) -> Router {
    let state = AppState::new(data_dir);
    create_router_with_state(state)
}

pub fn create_router_with_config(data_dir: PathBuf, group_commit_config: GroupCommitConfig) -> Router {
    let state = AppState::new_with_config(data_dir, group_commit_config);
    create_router_with_state(state)
}

/// Listens for process termination signals (SIGINT / Ctrl+C and SIGTERM)
/// as well as programmatic cancellation of the provided CancellationToken.
/// When triggered, invokes token.cancel() to wake up all waiting long-pollers.
pub async fn shutdown_signal(token: CancellationToken) {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
        _ = token.cancelled() => {},
    }

    token.cancel();
}
