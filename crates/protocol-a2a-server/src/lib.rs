//! `protocol-a2a-server` — A2A wire server on top of AdapterCore.
//!
//! Integration architecture:
//!
//! ```text
//! axum::Router (a2a-server: agent_card_router + jsonrpc_router)
//!         ↓ implements
//! DefaultRequestHandler  (a2a-server: SSE/resume/cancel/push, bounded broadcast)
//!         ↓ uses
//! AgentExecutor (наш: AdapterAgentExecutor)  → AdapterCore
//! TaskStore     (наш: AdapterTaskStore)      → adapter-store-contract::TaskStore
//! AgentCardProducer (наш: AdapterCardProducer) → registry/config
//! ```
//!
//! Мы НЕ переизобретаем JSON-RPC dispatch, SSE framing, resume или Agent Card
//! serialization — это уже реализовано в `a2a-server` (pinned commit
//! 02ee56024a485a5f184cbc55d1706918ee1ff809, crate `a2a-server-lf`).
//! Наша задача — тонкие адаптеры к AdapterCore.
//!
//! ## Backpressure strategy (SSE)
//!
//! `a2a-server` использует bounded `tokio::sync::broadcast` (capacity 32,
//! см. `handler.rs::EXECUTION_BUFFER_CAPACITY`). При lagging consumer
//! `subscription_stream` возвращает explicit `A2AError::internal(...)` gap/error
//! и закрывает stream; клиент делает resume с курсо-а из store.
//! Выбрана стратегия drop-with-gap-event, а не backpressure-with-timeout:
//! writer (task execution) никогда не блокируется на медленном SSE-клиенте,
//! и другие tasks не страдают. Unbounded канал не используется.

pub mod card;
pub mod executor;
pub mod health;
pub mod task_store;

pub use card::{AdapterCardConfig, AdapterCardProducer};
pub use executor::AdapterAgentExecutor;
pub use health::{health_router, HealthState};
pub use task_store::AdapterTaskStore;

use a2a_server::{agent_card::agent_card_router, jsonrpc::jsonrpc_router};
use std::sync::Arc;

/// Собрать единый axum::Router: agent card + JSON-RPC + health/readiness.
pub fn build_router(
    executor: Arc<AdapterAgentExecutor>,
    task_store: Arc<AdapterTaskStore>,
    card_producer: Arc<AdapterCardProducer>,
    health: HealthState,
) -> axum::Router {
    let handler = Arc::new(a2a_server::DefaultRequestHandler::new(
        (*executor).clone(),
        (*task_store).clone(),
    ));
    agent_card_router(card_producer)
        .merge(jsonrpc_router(handler))
        .merge(health_router(health))
}
