//! Axum middleware: извлекает caller identity из `Authorization: Bearer`
//! header ДО вызова executor и кладёт `Caller` в request extensions.
//!
//! agent_card_router и health_router по design публичны (discovery/health),
//! защищается только JSON-RPC inbound.

use adapter_core::{BearerTokenPolicy, Caller};
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct AuthState {
    pub policy: Arc<BearerTokenPolicy>,
}

/// Middleware: извлекает `Authorization: Bearer <token>`, резолвит его в
/// Caller через BearerTokenPolicy, кладёт Caller в request extensions.
/// Возвращает 401 без раскрытия причины (не "token expired" vs "token
/// invalid" — единообразный ответ, чтобы не давать оракул атакующему).
pub async fn require_bearer_auth(
    State(auth): State<AuthState>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let token = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    let Some(token) = token else {
        return (StatusCode::UNAUTHORIZED, "missing bearer token").into_response();
    };

    let Some(caller) = auth.policy.resolve(token) else {
        return (StatusCode::UNAUTHORIZED, "invalid bearer token").into_response();
    };

    req.extensions_mut().insert(caller);
    next.run(req).await
}

/// Извлекает caller из request extensions (положен middleware'ом). None для
/// неаутентифицированных/анонимных запросов — вызывающий код решает, как
/// трактовать (fallback на дефолтного caller'а или 401).
pub fn extract_caller(req: &Request<Body>) -> Option<Caller> {
    req.extensions().get::<Caller>().cloned()
}
