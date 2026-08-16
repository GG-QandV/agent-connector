//! crates/driver-a2a-client/src/wire/spec.rs
//!
//! Семантический A2A JSON-RPC шлюза ACP-A2A_gateway: методы message/send,
//! tasks/get, tasks/cancel; плоский Task (без обёртки {task:...});
//! lowercase role/state; part помечен полем "kind".
//! Источник: shliuz-opisanie-a2a-2.md §2 (подтверждено живыми запросами
//! 2026-08-17) — states: submitted|working|input_required|auth_required|
//! completed|failed|canceled|rejected.

use super::{A2aOperation, A2aWire, NormalizedPart, NormalizedState, NormalizedTask};
use crate::error::A2aClientError;
use serde_json::{json, Map, Value};

pub struct A2aSpecWire;

fn part_to_spec(p: &NormalizedPart) -> Value {
    if let Some(text) = &p.text {
        json!({ "kind": "text", "text": text })
    } else if let Some(uri) = &p.uri {
        json!({ "kind": "file", "file": { "uri": uri, "mimeType": p.mime_type } })
    } else if let Some(raw) = &p.raw {
        json!({ "kind": "data", "data": raw })
    } else {
        json!({ "kind": "text", "text": "" })
    }
}

/// Строго проверяет тег `kind`. Не подглядывает в наличие "text" как
/// fallback — иначе file/data-часть с случайным полем "text" в metadata
/// была бы неверно классифицирована как текстовая (см. аудит: гап,
/// найденный в первой версии этого модуля).
fn spec_part_to_normalized(p: &Value) -> Option<NormalizedPart> {
    match p.get("kind").and_then(Value::as_str) {
        Some("text") => p
            .get("text")
            .and_then(Value::as_str)
            .map(NormalizedPart::text),
        Some("file") => {
            let uri = p.pointer("/file/uri").and_then(Value::as_str)?.to_string();
            let mime_type = p
                .pointer("/file/mimeType")
                .and_then(Value::as_str)
                .map(str::to_string);
            Some(NormalizedPart {
                text: None,
                raw: None,
                uri: Some(uri),
                mime_type,
            })
        }
        _ => None,
    }
}

impl A2aWire for A2aSpecWire {
    fn name(&self) -> &'static str {
        "spec"
    }

    fn jsonrpc_method(&self, op: &A2aOperation<'_>) -> &'static str {
        match op {
            A2aOperation::SendMessage { .. } => "message/send",
            A2aOperation::GetTask { .. } => "tasks/get",
            A2aOperation::CancelTask { .. } => "tasks/cancel",
        }
    }

    fn build_params(&self, op: &A2aOperation<'_>) -> Value {
        match op {
            A2aOperation::SendMessage {
                parts,
                context_id,
                task_id: _,
            } => {
                let spec_parts: Vec<Value> = parts.iter().map(part_to_spec).collect();
                let mut message = Map::new();
                message.insert("role".to_string(), json!("user"));
                message.insert("parts".to_string(), json!(spec_parts));
                if let Some(cid) = context_id {
                    message.insert("contextId".to_string(), json!(cid));
                }
                json!({ "message": Value::Object(message) })
            }
            A2aOperation::GetTask { task_id } | A2aOperation::CancelTask { task_id } => {
                json!({ "id": task_id })
            }
        }
    }

    fn parse_task(&self, result: &Value) -> Result<NormalizedTask, A2aClientError> {
        // Шлюз всегда отдаёт плоский Task — обёртки {task:...} здесь нет
        // ни для одной из трёх операций (message/send, tasks/get, tasks/cancel).
        let id = result
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| A2aClientError::ProtocolError("spec wire: task.id missing".into()))?
            .to_string();

        let context_id = result
            .get("context_id")
            .and_then(Value::as_str)
            .map(str::to_string);

        let state_raw = result
            .pointer("/status/state")
            .and_then(Value::as_str)
            .unwrap_or("submitted");

        let state = match state_raw {
            "submitted" => NormalizedState::Submitted,
            "working" => NormalizedState::Working,
            "input_required" => NormalizedState::InputRequired,
            "auth_required" => NormalizedState::AuthRequired,
            "completed" => NormalizedState::Completed,
            "failed" => NormalizedState::Failed,
            "canceled" => NormalizedState::Canceled,
            "rejected" => NormalizedState::Rejected,
            other => {
                return Err(A2aClientError::ProtocolError(format!(
                    "spec wire: unknown task state '{other}' — verify against \
                     protocol/src/a2a.rs kebab-case state list"
                )))
            }
        };

        let status_message = result
            .pointer("/status/message/parts/0/text")
            .and_then(Value::as_str)
            .map(str::to_string);

        let mut output_parts = Vec::new();
        if let Some(artifacts) = result.get("artifacts").and_then(Value::as_array) {
            for artifact in artifacts {
                if let Some(parts) = artifact.get("parts").and_then(Value::as_array) {
                    output_parts.extend(parts.iter().filter_map(spec_part_to_normalized));
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
