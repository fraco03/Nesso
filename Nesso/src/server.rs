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

use crate::storage::engine::Engine;

#[derive(Clone)]
pub struct AppState {
    pub data_dir: PathBuf,
    pub queues: Arc<Mutex<HashMap<String, Engine>>>,
}

impl AppState {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            queues: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get_or_create_queue(&self, name: &str) -> std::io::Result<Engine> {
        let mut queues = self.queues.lock().unwrap();
        if let Some(engine) = queues.get(name) {
            return Ok(engine.clone());
        }

        let queue_dir = self.data_dir.join(name);
        if !queue_dir.exists() {
            std::fs::create_dir_all(&queue_dir)?;
        }

        let engine = Engine::open(&queue_dir)?;
        queues.insert(name.to_string(), engine.clone());
        Ok(engine)
    }

    pub fn get_existing_queue(&self, name: &str) -> Result<Engine, ApiError> {
        {
            let queues = self.queues.lock().unwrap();
            if let Some(engine) = queues.get(name) {
                return Ok(engine.clone());
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
// DTOs
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
// Handlers
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
    let engine = state.get_existing_queue(&queue_name)?;
    let (ready, active) = tokio::task::spawn_blocking(move || engine.status())
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
        
    Ok(Json(QueueStatusResponse {
        ready_tasks: ready,
        active_leases: active,
    }))
}

async fn push_handler(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Query(sync_query): Query<SyncQuery>,
    Json(req): Json<PushRequest>,
) -> Result<(StatusCode, Json<PushResponse>), ApiError> {
    let engine = state.get_or_create_queue(&queue_name)?;

    let payload_bytes = BASE64_STANDARD
        .decode(&req.payload)
        .map_err(|e| ApiError::BadRequest(format!("Invalid base64 payload: {}", e)))?;

    let priority = req.priority;
    let id = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
        let id = engine.push(payload_bytes, priority)?;
        if sync_query.sync { engine.sync()?; }
        Ok(id)
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;

    Ok((StatusCode::CREATED, Json(PushResponse { id })))
}

async fn pop_handler(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Query(sync_query): Query<SyncQuery>,
    Json(req): Json<PopRequest>,
) -> Result<Response, ApiError> {
    let engine = match state.get_existing_queue(&queue_name) {
        Ok(eng) => eng,
        Err(ApiError::NotFound(_)) => return Ok(StatusCode::NO_CONTENT.into_response()),
        Err(e) => return Err(e),
    };

    let consumer_id = req.consumer_id;
    let lease_secs = req.lease_secs;
    
    // Explicit type mapping to handle the tuple return correctly
    let pop_result = tokio::task::spawn_blocking(move || {
        let res = engine.pop_and_lease(consumer_id, lease_secs)?;
        if sync_query.sync && res.is_some() { engine.sync()?; }
        Ok::<_, std::io::Error>(res)
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;

    if let Some((record, retries)) = pop_result {
        let payload_b64 = BASE64_STANDARD.encode(record.payload());

        Ok(Json(PopResponse {
            id: record.id(),
            payload: payload_b64,
            priority: record.priority(),
            retries,
        }).into_response())
    } else {
        Ok(StatusCode::NO_CONTENT.into_response())
    }
}

async fn ack_handler(
    State(state): State<AppState>,
    Path((queue_name, id)): Path<(String, u64)>,
    Query(sync_query): Query<SyncQuery>,
    Json(req): Json<AckNackRequest>,
) -> Result<Response, ApiError> {
    let engine = state.get_existing_queue(&queue_name)?;
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
    let engine = state.get_existing_queue(&queue_name)?;
    let consumer_id = req.consumer_id;
    
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        engine.nack(id, consumer_id)?;
        if sync_query.sync { engine.sync()?; }
        Ok(())
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;
        
    Ok(StatusCode::OK.into_response())
}

pub fn create_router(data_dir: PathBuf) -> Router {
    let state = AppState::new(data_dir);

    Router::new()
        .route("/health", get(health_handler))
        .route("/v1/queues", get(list_queues_handler))
        .route("/v1/queues/{queue_name}/push", post(push_handler))
        .route("/v1/queues/{queue_name}/pop", post(pop_handler))
        .route("/v1/queues/{queue_name}/status", get(status_handler))
        .route("/v1/queues/{queue_name}/tasks/{id}/ack", post(ack_handler))
        .route("/v1/queues/{queue_name}/tasks/{id}/nack", post(nack_handler))
        .with_state(state)
}
