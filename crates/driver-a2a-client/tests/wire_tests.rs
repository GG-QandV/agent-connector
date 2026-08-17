//! crates/driver-a2a-client/tests/wire_tests.rs
//!
//! Покрывает: (1) корректность сериализации/парсинга каждого wire изолированно,
//! (2) перекрёстную несовместимость (spec-парсер не должен принять
//! SDK-обёртку и наоборот — это фиксирует контракт, а не баг),
//! (3) строгую фильтрацию kind в spec-парсере (регрессия найденного гапа),
//! (4) маппинг -32601 и -32010 на осмысленные ошибки,
//! (5) живой contract-тест через mock-сервер для обоих wire.

use driver_a2a_client::error::A2aClientError;
use driver_a2a_client::wire::sdk::A2aSdkWire;
use driver_a2a_client::wire::spec::A2aSpecWire;
use driver_a2a_client::wire::{A2aOperation, A2aWire, NormalizedPart, NormalizedState};
use driver_a2a_client::{A2aClientConfig, A2aClientDriver, A2aWireFormat};
use serde_json::json;

// ---------- SDK wire: unit ----------

#[test]
fn sdk_build_params_uses_proto_shape() {
    let wire = A2aSdkWire;
    let parts = vec![NormalizedPart::text("ping")];
    let op = A2aOperation::SendMessage {
        parts: &parts,
        context_id: Some("ctx-1"),
        task_id: None,
    };

    assert_eq!(wire.jsonrpc_method(&op), "SendMessage");
    let params = wire.build_params(&op);
    assert_eq!(params["message"]["role"], "ROLE_USER");
    assert_eq!(params["message"]["parts"][0]["text"], "ping");
    assert!(params["message"]["parts"][0].get("kind").is_none());
    assert_eq!(params["contextId"], "ctx-1");
}

#[test]
fn sdk_parse_task_requires_wrapper() {
    let wire = A2aSdkWire;
    let flat = json!({ "id": "task-1", "status": { "state": "TASK_STATE_COMPLETED" } });
    let err = wire.parse_task(&flat).unwrap_err();
    assert!(matches!(err, A2aClientError::ProtocolError(_)));
}

#[test]
fn sdk_parse_task_completed_with_artifacts() {
    let wire = A2aSdkWire;
    let payload = json!({
        "task": {
            "id": "task-100",
            "contextId": "ctx-1",
            "status": { "state": "TASK_STATE_COMPLETED" },
            "artifacts": [{ "parts": [{ "text": "pong" }] }]
        }
    });
    let task = wire.parse_task(&payload).expect("must parse");
    assert_eq!(task.id, "task-100");
    assert_eq!(task.state, NormalizedState::Completed);
    assert_eq!(task.output_parts[0].text.as_deref(), Some("pong"));
}

#[test]
fn sdk_parse_task_unknown_state_is_explicit_error_not_silent_default() {
    let wire = A2aSdkWire;
    let payload = json!({
        "task": { "id": "task-1", "status": { "state": "TASK_STATE_TYPO" } }
    });
    let err = wire.parse_task(&payload).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unknown task state"));
}

#[test]
fn sdk_parse_task_concatenates_all_status_message_parts() {
    // Регрессия, найденная живым E2E (adapterd SDK): hermes отвечает
    // несколькими parts в status.message.parts ("SD"/"K"/"_OK"). Раньше
    // бралась только parts[0] — остальной текст терялся. Spec-wire уже
    // конкатенирует все текстовые части; теперь sdk-wire делает то же.
    let wire = A2aSdkWire;
    let payload = json!({
        "task": {
            "id": "task-sdk-parts",
            "contextId": "ctx-1",
            "status": {
                "state": "TASK_STATE_COMPLETED",
                "message": {
                    "messageId": "m-1",
                    "role": "ROLE_AGENT",
                    "parts": [
                        { "text": "SD" },
                        { "text": "K" },
                        { "text": "_OK" }
                    ]
                }
            }
        }
    });
    let task = wire.parse_task(&payload).expect("must parse");
    assert_eq!(
        task.status_message.as_deref(),
        Some("SDK_OK"),
        "all status message parts must be concatenated"
    );
}

// ---------- Spec wire: unit ----------

#[test]
fn spec_build_params_uses_kind_tagged_parts() {
    let wire = A2aSpecWire;
    let parts = vec![NormalizedPart::text("ping")];
    let op = A2aOperation::SendMessage {
        parts: &parts,
        context_id: Some("ctx-1"),
        task_id: None,
    };

    assert_eq!(wire.jsonrpc_method(&op), "message/send");
    let params = wire.build_params(&op);
    assert_eq!(params["message"]["role"], "user");
    assert_eq!(params["message"]["parts"][0]["kind"], "text");
    assert_eq!(params["message"]["parts"][0]["text"], "ping");
}

#[test]
fn spec_parse_task_is_flat_no_wrapper() {
    let wire = A2aSpecWire;
    let payload = json!({
        "id": "task-200",
        "context_id": "ctx-1",
        "status": { "state": "completed" },
        "artifacts": [{ "parts": [{ "kind": "text", "text": "pong" }] }]
    });
    let task = wire.parse_task(&payload).expect("must parse");
    assert_eq!(task.id, "task-200");
    assert_eq!(task.state, NormalizedState::Completed);
    assert_eq!(task.output_parts[0].text.as_deref(), Some("pong"));
}

/// Регрессия найденного в аудите гапа: part без kind:"text", у которого
/// случайно оказалось поле "text" где-то не в том месте, не должен
/// попадать в output как текст. Строгая проверка тега kind.
#[test]
fn spec_parse_ignores_non_text_kind_even_if_text_field_present() {
    let wire = A2aSpecWire;
    let payload = json!({
        "id": "task-201",
        "status": { "state": "completed" },
        "artifacts": [{
            "parts": [
                { "kind": "file", "file": { "uri": "s3://x" }, "text": "should-not-leak" }
            ]
        }]
    });
    let task = wire.parse_task(&payload).expect("must parse");
    assert_eq!(task.output_parts.len(), 1);
    assert!(task.output_parts[0].text.is_none());
    assert_eq!(task.output_parts[0].uri.as_deref(), Some("s3://x"));
}

// ---------- Перекрёстная несовместимость (контракт, не баг) ----------

#[test]
fn sdk_wire_rejects_spec_shaped_response() {
    let wire = A2aSdkWire;
    // Ответ шлюза: плоский, lowercase state — sdk-парсер должен явно упасть
    // на отсутствии обёртки "task", а не тихо вернуть мусорный NormalizedTask.
    let spec_shaped = json!({ "id": "task-1", "status": { "state": "completed" } });
    let err = wire.parse_task(&spec_shaped).unwrap_err();
    assert!(matches!(err, A2aClientError::ProtocolError(_)));
}

#[test]
fn spec_wire_rejects_sdk_shaped_response() {
    let wire = A2aSpecWire;
    // Ответ adapterd: обёртка {task:...} — у плоского парсера result.get("id")
    // не найдёт id на верхнем уровне и должен упасть с понятной ошибкой,
    // а не спутать вложенный task.id.
    let sdk_shaped =
        json!({ "task": { "id": "task-1", "status": { "state": "TASK_STATE_COMPLETED" } } });
    let err = wire.parse_task(&sdk_shaped).unwrap_err();
    assert!(matches!(err, A2aClientError::ProtocolError(_)));
}

// ---------- Маппинг ошибок ----------

#[tokio::test]
async fn method_not_found_hint_mentions_wire_format_and_method() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32601, "message": "Method not found" }
        })))
        .mount(&server)
        .await;

    let driver = A2aClientDriver::new(A2aClientConfig {
        endpoint: format!("{}/rpc", server.uri()),
        token: None,
        wire_format: A2aWireFormat::Sdk,
        timeout_secs: 5,
        agent_card_url: None,
    })
    .expect("driver builds");

    let err = driver.invoke("hello", None, None).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("SendMessage"));
    assert!(msg.contains("wire_format=sdk"));
}

#[tokio::test]
async fn context_lost_error_is_distinct_from_generic_remote_error() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32010, "message": "context expired" }
        })))
        .mount(&server)
        .await;

    let driver = A2aClientDriver::new(A2aClientConfig {
        endpoint: format!("{}/rpc", server.uri()),
        token: None,
        wire_format: A2aWireFormat::Spec,
        timeout_secs: 5,
        agent_card_url: None,
    })
    .expect("driver builds");

    let err = driver
        .invoke("hello", Some("ctx-stale"), None)
        .await
        .unwrap_err();
    assert!(matches!(err, A2aClientError::ContextLost { .. }));
    assert!(err.to_string().contains("fresh context_id"));
}

// ---------- Contract: полный invoke через mock-сервер, оба wire ----------

#[tokio::test]
async fn full_invoke_completes_via_sdk_wire() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "task": {
                    "id": "task-1",
                    "status": { "state": "TASK_STATE_COMPLETED" },
                    "artifacts": [{ "parts": [{ "text": "hi from sdk" }] }]
                }
            }
        })))
        .mount(&server)
        .await;

    let driver = A2aClientDriver::new(A2aClientConfig {
        endpoint: format!("{}/rpc", server.uri()),
        token: None,
        wire_format: A2aWireFormat::Sdk,
        timeout_secs: 5,
        agent_card_url: None,
    })
    .expect("driver builds");

    let task = driver.invoke("hello", None, None).await.expect("completes");
    assert_eq!(task.state, NormalizedState::Completed);
    assert_eq!(task.output_parts[0].text.as_deref(), Some("hi from sdk"));
}

#[tokio::test]
async fn full_invoke_completes_via_spec_wire() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "id": "task-2",
                "status": { "state": "completed" },
                "artifacts": [{ "parts": [{ "kind": "text", "text": "hi from spec" }] }]
            }
        })))
        .mount(&server)
        .await;

    let driver = A2aClientDriver::new(A2aClientConfig {
        endpoint: format!("{}/rpc", server.uri()),
        token: None,
        wire_format: A2aWireFormat::Spec,
        timeout_secs: 5,
        agent_card_url: None,
    })
    .expect("driver builds");

    let task = driver.invoke("hello", None, None).await.expect("completes");
    assert_eq!(task.state, NormalizedState::Completed);
    assert_eq!(task.output_parts[0].text.as_deref(), Some("hi from spec"));
}
