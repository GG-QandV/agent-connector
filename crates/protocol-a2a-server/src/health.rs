//! Health/readiness router — собственный, мержится с `a2a-server` router-ами.

use adapter_core::AgentRegistry;
use adapter_store_contract::TaskStore;
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use std::sync::Arc;

#[derive(Clone)]
pub struct HealthState {
    task_store: Arc<dyn TaskStore>,
    registry: Arc<AgentRegistry>,
    draining: Arc<std::sync::atomic::AtomicBool>,
}

impl HealthState {
    pub fn new(
        task_store: Arc<dyn TaskStore>,
        registry: Arc<AgentRegistry>,
        draining: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            task_store,
            registry,
            draining,
        }
    }
}

/// Router с `/healthz` (процесс жив, без I/O) и `/readyz` (storage, registry,
/// не draining). 503 без утечки DSN/token/path в теле.
pub fn health_router(state: HealthState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"ok": true})))
}

async fn readyz(State(state): State<HealthState>) -> impl IntoResponse {
    if state.draining.load(std::sync::atomic::Ordering::SeqCst) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"ready": false, "reason": "draining"})),
        );
    }
    if state.registry.agents().is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"ready": false, "reason": "no agents registered"})),
        );
    }
    // Readiness требует доступный storage; /healthz его не трогает.
    match state.task_store.ping().await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"ready": true}))),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"ready": false, "reason": "storage unavailable"})),
        ),
    }
}
