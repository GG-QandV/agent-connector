//! Тесты: agent card, e2e JSON-RPC send, unsupported method, healthz/readyz.

use adapter_core::{
    AdapterCore, AgentDriver, AgentLimits, AgentRegistry, AllowAllPolicy, CoreError,
    DriverCapabilities, DriverEvent, InvokeRequest, Part, RegisteredAgent, TaskId,
};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use memory_task_store::MemoryTaskStore;
use protocol_a2a_server::*;
use std::sync::Arc;
use tokio::sync::mpsc;
use tower::ServiceExt;

/// Тестовый driver: мгновенно завершает задачу.
struct EchoDriver;

#[async_trait]
impl AgentDriver for EchoDriver {
    fn id(&self) -> &str {
        "echo"
    }
    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            cancellation: true,
            provide_input: true,
        }
    }
    async fn health(&self) -> Result<(), CoreError> {
        Ok(())
    }
    async fn invoke(
        &self,
        _task_id: TaskId,
        _req: InvokeRequest,
    ) -> Result<mpsc::Receiver<DriverEvent>, CoreError> {
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let _ = tx
                .send(DriverEvent::Progress {
                    message: "working".into(),
                    percent: Some(50),
                })
                .await;
            let _ = tx
                .send(DriverEvent::Completed(vec![Part::Text {
                    text: "done".into(),
                }]))
                .await;
        });
        Ok(rx)
    }
    async fn cancel(&self, _task_id: TaskId) -> Result<(), CoreError> {
        Ok(())
    }
    async fn provide_input(&self, _task_id: TaskId, _input: Vec<Part>) -> Result<(), CoreError> {
        Ok(())
    }
}

fn make_router() -> axum::Router {
    let store: Arc<dyn adapter_store_contract::TaskStore> = Arc::new(MemoryTaskStore::new());
    let registry = Arc::new(AgentRegistry::new());
    registry.register(RegisteredAgent::new(
        adapter_core::AgentId("echo".into()),
        vec!["echo-skill".into()],
        Arc::new(EchoDriver),
        AgentLimits {
            max_concurrent_tasks: 4,
            max_queued_tasks: 16,
            max_input_bytes: 1024 * 1024,
            max_event_bytes: 256 * 1024,
            default_timeout: std::time::Duration::from_secs(30),
        },
    ));
    let core = Arc::new(AdapterCore::new(
        store.clone(),
        registry.clone(),
        Arc::new(AllowAllPolicy),
        8,
    ));

    let executor = Arc::new(AdapterAgentExecutor::new(core.clone(), "test-client"));
    let task_store = Arc::new(AdapterTaskStore::new(store.clone()));
    let card = Arc::new(AdapterCardProducer::new(
        registry.clone(),
        AdapterCardConfig {
            name: "agent-connector".into(),
            description: "test".into(),
            version: "0.1.0".into(),
            endpoint_url: "https://example.com/".into(),
        },
    ));
    let health = HealthState::new(
        store,
        registry,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    );
    build_router(executor, task_store, card, health, None)
}

#[tokio::test]
async fn test_agent_card_well_known_path() {
    let app = make_router();
    let req = Request::builder()
        .uri("/.well-known/agent-card.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let card: a2a::AgentCard = serde_json::from_slice(&body).unwrap();
    assert_eq!(card.name, "agent-connector");
    assert_eq!(card.capabilities.streaming, Some(true));
    assert_eq!(card.skills.len(), 1);
}

#[tokio::test]
async fn test_healthz_and_readyz() {
    let app = make_router();
    let req = Request::builder()
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let req = Request::builder()
        .uri("/readyz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_e2e_send_message() {
    let app = make_router();
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "SendMessage",
        "params": {
            "message": {
                "messageId": "m1",
                "role": "ROLE_USER",
                "parts": [{"text": "hello"}]
            }
        }
    });
    let req = Request::builder()
        .uri("/")
        .method("POST")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let rpc: a2a::JsonRpcResponse = serde_json::from_slice(&body).unwrap();
    assert!(rpc.error.is_none(), "unexpected error: {:?}", rpc.error);
    assert!(rpc.result.is_some());
}

#[tokio::test]
async fn test_unsupported_method() {
    let app = make_router();
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "unknown.method",
        "params": {}
    });
    let req = Request::builder()
        .uri("/")
        .method("POST")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let rpc: a2a::JsonRpcResponse = serde_json::from_slice(&body).unwrap();
    let error = rpc.error.expect("expected error");
    assert_eq!(error.code, a2a::error_code::METHOD_NOT_FOUND);
}

fn make_auth_router() -> axum::Router {
    // Токен читается из env-переменной — ставим её прямо в тесте.
    std::env::set_var("ADAPTER_TEST_TOKEN", "secret-token");
    let store: Arc<dyn adapter_store_contract::TaskStore> = Arc::new(MemoryTaskStore::new());
    let registry = Arc::new(AgentRegistry::new());
    registry.register(RegisteredAgent::new(
        adapter_core::AgentId("echo".into()),
        vec!["echo-skill".into()],
        Arc::new(EchoDriver),
        AgentLimits {
            max_concurrent_tasks: 4,
            max_queued_tasks: 16,
            max_input_bytes: 1024 * 1024,
            max_event_bytes: 256 * 1024,
            default_timeout: std::time::Duration::from_secs(30),
        },
    ));
    let policy = Arc::new(
        adapter_core::BearerTokenPolicy::from_env(vec![(
            "ADAPTER_TEST_TOKEN".into(),
            adapter_core::TokenGrant {
                caller_id: adapter_core::CallerId("primary-client".into()),
                allowed_scopes: Vec::new(),
            },
        )])
        .unwrap(),
    );
    let core = Arc::new(AdapterCore::new(
        store.clone(),
        registry.clone(),
        policy.clone() as Arc<dyn adapter_core::PolicyEngine>,
        8,
    ));
    let executor = Arc::new(AdapterAgentExecutor::with_auth(
        core.clone(),
        "test-client",
        policy.clone(),
    ));
    let task_store = Arc::new(AdapterTaskStore::new(store.clone()));
    let card = Arc::new(AdapterCardProducer::new(
        registry.clone(),
        AdapterCardConfig {
            name: "agent-connector".into(),
            description: "test".into(),
            version: "0.1.0".into(),
            endpoint_url: "https://example.com/".into(),
        },
    ));
    let health = HealthState::new(
        store,
        registry,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    );
    build_router(
        executor,
        task_store,
        card,
        health,
        Some(AuthState { policy }),
    )
}

#[tokio::test]
async fn test_bearer_auth_protects_jsonrpc_but_not_card() {
    let app = make_auth_router();

    // JSON-RPC без токена — 401.
    let req = Request::builder()
        .uri("/")
        .method("POST")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "GetTask",
                "params": {"id": "00000000-0000-0000-0000-000000000000"}
            })
            .to_string(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // JSON-RPC с валидным токеном — не 401 (задача не найдена, но запрос допущен).
    let req = Request::builder()
        .uri("/")
        .method("POST")
        .header("content-type", "application/json")
        .header("authorization", "Bearer secret-token")
        .body(Body::from(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "GetTask",
                "params": {"id": "00000000-0000-0000-0000-000000000000"}
            })
            .to_string(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);

    // Agent card остаётся публичной.
    let req = Request::builder()
        .uri("/.well-known/agent-card.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
