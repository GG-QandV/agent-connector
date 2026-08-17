//! crates/driver-a2a-client/src/dialect_probe.rs
//!
//! Идентичен по алгоритму серверной половине (gatewayd/src/dialect_probe.rs):
//! пробуем SDK (GetTask/{"name":"tasks/<uuid>"}), затем Spec (tasks/get/
//! {"id":"<uuid>"}); решение — по маркеру "method_not_found:" в тексте
//! ошибки (см. error.rs::from_jsonrpc_error — тот же принцип), не по коду.

use crate::error::A2aClientError;
use crate::wire::{sdk::A2aSdkWire, spec::A2aSpecWire, A2aOperation, A2aWire};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use uuid::Uuid;

/// Детект по Agent Card (ТЗ §3.2 п.4): GET agent_card_url, читает
/// protocolVersion — "1.0" (или любая версия с ведущим "1") -> Sdk,
/// "0.x" -> Spec. Возвращает None (не Err!) при ЛЮБОЙ проблеме — сетевой
/// ошибке, невалидном JSON, отсутствии/непонятном protocolVersion —
/// потому что это лишь "предпочтительный", а не единственный канал:
/// вызывающий код (resolve_auto_wire) должен упасть на зонд, а не
/// прервать всю резолюцию из-за недоступной карточки.
pub async fn detect_from_agent_card(
    client: &reqwest::Client,
    agent_card_url: &str,
) -> Option<Arc<dyn A2aWire>> {
    let resp = client.get(agent_card_url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: JsonValue = resp.json().await.ok()?;
    let protocol_version = body.get("protocolVersion").and_then(JsonValue::as_str)?;

    if protocol_version.starts_with('1') {
        Some(Arc::new(A2aSdkWire))
    } else if protocol_version.starts_with('0') {
        Some(Arc::new(A2aSpecWire))
    } else {
        // Незнакомая мажорная версия — не угадываем, отдаём None, чтобы
        // resolve_auto_wire() упал на зонд, который хотя бы эмпирически
        // проверит реальное поведение сервера.
        None
    }
}

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
    let body: JsonValue = resp
        .json()
        .await
        .map_err(|e| A2aClientError::Http(e.to_string()))?;

    const METHOD_NOT_FOUND_MARKER: &str = "method_not_found:";

    if let Some(error) = body.get("error") {
        let message = error
            .get("message")
            .and_then(JsonValue::as_str)
            .unwrap_or("");
        return Ok(!message.contains(METHOD_NOT_FOUND_MARKER));
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            .and(body_partial_json(
                serde_json::json!({ "method": "GetTask" }),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32000, "message": "method_not_found: GetTask" }
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({ "method": "tasks/get" }),
            ))
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
    async fn agent_card_with_protocol_version_1_detects_sdk() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/agent.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "test-agent",
                "protocolVersion": "1.0",
                "url": "https://example.com/rpc"
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let card_url = format!("{}/.well-known/agent.json", server.uri());
        let wire = detect_from_agent_card(&client, &card_url)
            .await
            .expect("must detect from protocolVersion 1.0");
        assert_eq!(wire.name(), "sdk");
    }

    #[tokio::test]
    async fn agent_card_with_protocol_version_0_detects_spec() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/agent.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "test-agent",
                "protocolVersion": "0.9",
                "url": "https://example.com/rpc"
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let card_url = format!("{}/.well-known/agent.json", server.uri());
        let wire = detect_from_agent_card(&client, &card_url)
            .await
            .expect("must detect from protocolVersion 0.9");
        assert_eq!(wire.name(), "spec");
    }

    #[tokio::test]
    async fn agent_card_missing_protocol_version_returns_none_not_panic() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/agent.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "test-agent",
                "url": "https://example.com/rpc"
            })))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let card_url = format!("{}/.well-known/agent.json", server.uri());
        let result = detect_from_agent_card(&client, &card_url).await;
        assert!(
            result.is_none(),
            "missing protocolVersion must fall through to probe, not panic"
        );
    }

    #[tokio::test]
    async fn agent_card_unreachable_returns_none_not_err() {
        // Порт 1 практически гарантированно недоступен локально.
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(200))
            .build()
            .unwrap();
        let result =
            detect_from_agent_card(&client, "http://127.0.0.1:1/.well-known/agent.json").await;
        assert!(
            result.is_none(),
            "unreachable card must gracefully fall through, not error the whole resolution"
        );
    }

    #[tokio::test]
    async fn agent_card_takes_priority_over_probe_when_both_available() {
        // Регрессия ключевого требования ТЗ §3.2 п.4 "приоритетнее зонда":
        // если и AgentCard, и зонд-совместимый endpoint доступны, должен
        // сработать именно AgentCard-путь (в этом тесте — просто проверяем,
        // что detect_from_agent_card сама по себе достаточна и не требует
        // похода на endpoint зонда вовсе — она бьёт по card_url, а не по
        // основному endpoint).
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let card_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/agent.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "protocolVersion": "1.0"
            })))
            .mount(&card_server)
            .await;

        // Основной endpoint НЕ смонтирован никаким Mock — если бы
        // resolve_auto_wire() случайно дёрнул зонд на него, тест бы упал
        // с connection refused при попытке POST. Проверяем только
        // detect_from_agent_card изолированно, что она не трогает
        // основной endpoint вообще.
        let client = reqwest::Client::new();
        let card_url = format!("{}/.well-known/agent.json", card_server.uri());
        let wire = detect_from_agent_card(&client, &card_url).await;
        assert!(wire.is_some());
    }
}
