//! crates/driver-a2a-client/src/wire/sdk.rs
//!
//! Официальный a2a-rs SDK JSON-RPC слой: методы SendMessage/GetTask/
//! CancelTask, proto-поля (TASK_STATE_*, ROLE_*), обёртка {"task": ...}.
//! Источник имён и состояний: a2a/src/jsonrpc.rs:138, a2a/src/types.rs
//! (см. TZ-driver-a2a-wire-format.md §3 — сверено с живым тестом adapterd,
//! который вернул -32700 на part {"kind": ...} и ожидает TASK_STATE_*
//! в верхнем регистре с подчёркиванием, включая "TASK_STATE_CANCELLED").

use super::{A2aOperation, A2aWire, NormalizedPart, NormalizedState, NormalizedTask};
use crate::error::A2aClientError;
use serde_json::{json, Map, Value};

pub struct A2aSdkWire;

fn part_to_sdk(p: &NormalizedPart) -> Value {
    if let Some(text) = &p.text {
        json!({ "text": text })
    } else if let Some(uri) = &p.uri {
        json!({ "url": uri, "media_type": p.mime_type })
    } else if let Some(raw) = &p.raw {
        json!({ "raw": raw })
    } else {
        json!({ "text": "" })
    }
}

fn sdk_part_to_normalized(p: &Value) -> NormalizedPart {
    if let Some(text) = p.get("text").and_then(Value::as_str) {
        NormalizedPart::text(text)
    } else if let Some(url) = p.get("url").and_then(Value::as_str) {
        NormalizedPart {
            text: None,
            raw: None,
            uri: Some(url.to_string()),
            mime_type: p
                .get("media_type")
                .and_then(Value::as_str)
                .map(str::to_string),
        }
    } else {
        NormalizedPart::default()
    }
}

impl A2aWire for A2aSdkWire {
    fn name(&self) -> &'static str {
        "sdk"
    }

    fn jsonrpc_method(&self, op: &A2aOperation<'_>) -> &'static str {
        match op {
            A2aOperation::SendMessage { .. } => "SendMessage",
            A2aOperation::GetTask { .. } => "GetTask",
            A2aOperation::CancelTask { .. } => "CancelTask",
        }
    }

    fn build_params(&self, op: &A2aOperation<'_>) -> Value {
        match op {
            A2aOperation::SendMessage {
                parts,
                context_id,
                task_id,
            } => {
                let sdk_parts: Vec<Value> = parts.iter().map(part_to_sdk).collect();
                let message = json!({ "role": "ROLE_USER", "parts": sdk_parts });

                let mut params = Map::new();
                params.insert("message".to_string(), message);
                if let Some(cid) = context_id {
                    params.insert("contextId".to_string(), json!(cid));
                }
                if let Some(tid) = task_id {
                    params.insert("taskId".to_string(), json!(tid));
                }
                Value::Object(params)
            }
            A2aOperation::GetTask { task_id } | A2aOperation::CancelTask { task_id } => {
                json!({ "name": format!("tasks/{task_id}") })
            }
        }
    }

    fn parse_task(&self, result: &Value) -> Result<NormalizedTask, A2aClientError> {
        let task = result.get("task").ok_or_else(|| {
            A2aClientError::ProtocolError(
                "sdk wire: expected 'result.task' wrapper, field missing".into(),
            )
        })?;

        let id = task
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| A2aClientError::ProtocolError("sdk wire: task.id missing".into()))?
            .to_string();

        let context_id = task
            .get("contextId")
            .and_then(Value::as_str)
            .map(str::to_string);

        let state_raw = task
            .pointer("/status/state")
            .and_then(Value::as_str)
            .unwrap_or("TASK_STATE_UNSPECIFIED");

        let state = match state_raw {
            "TASK_STATE_SUBMITTED" => NormalizedState::Submitted,
            "TASK_STATE_WORKING" => NormalizedState::Working,
            "TASK_STATE_INPUT_REQUIRED" => NormalizedState::InputRequired,
            "TASK_STATE_AUTH_REQUIRED" => NormalizedState::AuthRequired,
            "TASK_STATE_COMPLETED" => NormalizedState::Completed,
            "TASK_STATE_FAILED" => NormalizedState::Failed,
            "TASK_STATE_CANCELLED" => NormalizedState::Canceled,
            "TASK_STATE_REJECTED" => NormalizedState::Rejected,
            other => {
                return Err(A2aClientError::ProtocolError(format!(
                    "sdk wire: unknown task state '{other}' — verify against a2a/src/types.rs \
                     TaskState serde before treating this as terminal"
                )))
            }
        };

        let status_message = task
            .pointer("/status/message/parts/0/text")
            .and_then(Value::as_str)
            .map(str::to_string);

        let mut output_parts = Vec::new();
        if let Some(artifacts) = task.get("artifacts").and_then(Value::as_array) {
            for artifact in artifacts {
                if let Some(parts) = artifact.get("parts").and_then(Value::as_array) {
                    output_parts.extend(parts.iter().map(sdk_part_to_normalized));
                }
            }
        }

        Ok(NormalizedTask {
            id,
            context_id,
            state,
            status_message,
            output_parts,
        })
    }
}
