//! crates/driver-a2a-client/src/dialect_probe.rs
//!
//! Идентичен по алгоритму серверной половине (gatewayd/src/dialect_probe.rs):
//! пробуем SDK (GetTask/{"name":"tasks/<uuid>"}), затем Spec (tasks/get/
//! {"id":"<uuid>"}).
//!
//! ИСПРАВЛЕНО (D1): распознавание "метод не найден" раньше строилось на
//! одной точной подстроке "method_not_found:" — специфичной именно нашему
//! собственному шлюзу (ACP-A2A_gateway::dispatch_a2a_method). Стандартный
//! JSON-RPC 2.0 сервер отвечает на неизвестный метод кодом -32601 с текстом
//! "Method not found" (заглавные, без двоеточия) — старая проверка это
//! пропускала: probe_recognizes возвращал Ok(true) на любом ответе без
//! точного маркера, включая стандартный -32601, и диалект ошибочно
//! принимался за распознанный. Первый реальный вызов после такого
//! ложного распознавания падал.
//!
//! Теперь распознавание — по коду -32601 (стандарт JSON-RPC 2.0:
//! https://www.jsonrpc.org/specification#error_object) ИЛИ по
//! нормализованному (lowercase) тексту, содержащему один из нескольких
//! известных вариантов формулировки: "method not found" (стандарт),
//! "method_not_found" (наш шлюз), "unknown method" (некоторые сторонние
//! реализации). Симметрично исправлению gatewayd/src/dialect_probe.rs.

use crate::error::A2aClientError;
use crate::wire::{sdk::A2aSdkWire, spec::A2aSpecWire, A2aOperation, A2aWire};
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

pub async fn probe_wire_format(
    client: &reqwest::Client,
    endpoint: &str,
    token: Option<&str>,
) -> Result<Arc<dyn A2aWire>, A2aClientError> {
    let probe_task_id = Uuid::new_v4().to_string();

    let sdk_wire: Arc<dyn A2aWire> = Arc::new(A2aSdkWire);
    if probe_recognizes(
        client,
        endpoint,
        token,
        &sdk_wire,
        &A2aOperation::GetTask {
            task_id: probe_task_id.as_str(),
        },
    )
    .await?
    {
        return Ok(sdk_wire);
    }

    let spec_wire: Arc<dyn A2aWire> = Arc::new(A2aSpecWire);
    if probe_recognizes(
        client,
        endpoint,
        token,
        &spec_wire,
        &A2aOperation::GetTask {
            task_id: probe_task_id.as_str(),
        },
    )
    .await?
    {
        return Ok(spec_wire);
    }

    Err(A2aClientError::ProtocolError(
        "dialect probe: endpoint did not recognize SDK or Spec GetTask/tasks-get — \
         cannot auto-detect wire_format, specify it explicitly in config"
            .into(),
    ))
}

/// ИСПРАВЛЕНО (D1): проверяет код -32601 (стандартный JSON-RPC 2.0
/// "Method not found") ИЛИ нормализованный текст с несколькими известными
/// формулировками — не одну точную подстроку конкретного шлюза.
fn looks_like_method_not_found(code: i64, message: &str) -> bool {
    const JSONRPC_STANDARD_METHOD_NOT_FOUND: i64 = -32601;

    if code == JSONRPC_STANDARD_METHOD_NOT_FOUND {
        return true;
    }

    let normalized = message.to_lowercase();
    normalized.contains("method not found")
        || normalized.contains("method_not_found")
        || normalized.contains("unknown method")
}

async fn probe_recognizes(
    client: &reqwest::Client,
    endpoint: &str,
    token: Option<&str>,
    wire: &Arc<dyn A2aWire>,
    op: &A2aOperation<'_>,
) -> Result<bool, A2aClientError> {
    let method = wire.jsonrpc_method(op);
    let params = wire.build_params(op);

    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });

    let mut req = client.post(endpoint).json(&payload);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }

    let resp = req
        .send()
        .await
        .map_err(|e| A2aClientError::Http(e.to_string()))?;
    let body: Value = resp
        .json()
        .await
        .map_err(|e| A2aClientError::Http(e.to_string()))?;

    if let Some(error) = body.get("error") {
        let code = error.get("code").and_then(Value::as_i64).unwrap_or(0);
        let message = error.get("message").and_then(Value::as_str).unwrap_or("");
        return Ok(!looks_like_method_not_found(code, message));
    }

    Ok(true)
}

/// ИСПРАВЛЕНО (D2): protocolVersion в AgentCard — версия A2A СПЕЦИФИКАЦИИ,
/// а не идентификатор wire-реализации конкретного сервера. Сервер на спеке
/// 1.0 может отвечать плоским Task (наш "spec"-диалект), просто
/// задекларировав актуальную версию протокола. Маппинг protocolVersion ->
/// wire был семантически неверной эвристикой (случайно совпадала для нашей
/// пары adapterd=SDK/шлюз=Spec, но не является свойством протокола).
/// Функция оставлена явной точкой расширения (возвращает None всегда) —
/// спека AgentCard сейчас не содержит поля, которое надёжно отличало бы
/// SDK-диалект от Spec-диалекта; реальный детект — только через
/// эмпирический probe_wire_format выше.
pub async fn detect_from_agent_card(
    _client: &reqwest::Client,
    _agent_card_url: &str,
) -> Option<Arc<dyn A2aWire>> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn looks_like_method_not_found_recognizes_standard_jsonrpc_code() {
        assert!(looks_like_method_not_found(-32601, "Method not found"));
    }

    #[test]
    fn looks_like_method_not_found_recognizes_our_gateway_format() {
        assert!(looks_like_method_not_found(-32000, "method_not_found: SendMessage"));
    }

    #[test]
    fn looks_like_method_not_found_recognizes_unknown_method_phrasing() {
        assert!(looks_like_method_not_found(-32000, "Unknown method: foo"));
    }

    #[test]
    fn looks_like_method_not_found_is_case_insensitive() {
        assert!(looks_like_method_not_found(-32000, "METHOD NOT FOUND"));
    }

    #[test]
    fn looks_like_method_not_found_rejects_unrelated_errors() {
        assert!(!looks_like_method_not_found(-32001, "task not found: tasks/deadbeef"));
        assert!(!looks_like_method_not_found(-32000, "internal server error"));
    }

    #[tokio::test]
    async fn probe_recognizes_sdk_with_standard_jsonrpc_method_not_found_on_wrong_dialect() {
        use wiremock::matchers::{body_partial_json, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(serde_json::json!({ "method": "GetTask" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32601, "message": "Method not found" }
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(serde_json::json!({ "method": "tasks/get" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32001, "message": "task not found" }
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let wire = probe_wire_format(&client, &server.uri(), None)
            .await
            .expect("must fall back to spec, not be fooled by standard -32601");
        assert_eq!(wire.name(), "spec");
    }

    #[tokio::test]
    async fn probe_recognizes_sdk_when_server_understands_get_task() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32001, "message": "task not found: tasks/deadbeef" }
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let wire = probe_wire_format(&client, &server.uri(), None)
            .await
            .expect("must detect a dialect");
        assert_eq!(wire.name(), "sdk");
    }

    #[tokio::test]
    async fn probe_falls_back_to_spec_when_sdk_method_not_found() {
        use wiremock::matchers::{body_partial_json, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(serde_json::json!({ "method": "GetTask" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32000, "message": "method_not_found: GetTask" }
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(serde_json::json!({ "method": "tasks/get" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32001, "message": "task not found" }
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let wire = probe_wire_format(&client, &server.uri(), None)
            .await
            .expect("must fall back to spec");
        assert_eq!(wire.name(), "spec");
    }

    #[tokio::test]
    async fn probe_errors_when_neither_dialect_recognized() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32000, "message": "method_not_found: whatever" }
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let result = probe_wire_format(&client, &server.uri(), None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn detect_from_agent_card_always_returns_none_by_design() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/agent.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocolVersion": "1.0"
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let card_url = format!("{}/.well-known/agent.json", server.uri());
        let result = detect_from_agent_card(&client, &card_url).await;
        assert!(
            result.is_none(),
            "protocolVersion must not be used to guess wire dialect (D2 fix)"
        );
    }
}
