//! crates/driver-a2a-client/src/dialect_probe.rs
//!
//! Идентичен по алгоритму серверной половине (gatewayd/src/dialect_probe.rs):
//! пробуем SDK (GetTask/{"name":"tasks/<uuid>"}), затем Spec (tasks/get/
//! {"id":"<uuid>"}); решение — по маркеру "method_not_found:" в тексте
//! ошибки (см. error.rs::from_jsonrpc_error — тот же принцип), не по коду.

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

    const METHOD_NOT_FOUND_MARKER: &str = "method_not_found:";

    if let Some(error) = body.get("error") {
        let message = error.get("message").and_then(Value::as_str).unwrap_or("");
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
}
