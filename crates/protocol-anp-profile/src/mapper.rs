//! Mapping between `agent-connector.anp-task.v1` wire DTOs and the
//! canonical `adapter-core` command/event model.
//!
//! Direction of mapping:
//! - outbound initiator: `TaskInvoke`/`TaskCancel`/`TaskProvideInput` →
//!   `adapter_model::CoreCommand`;
//! - inbound events: `TaskEvent` → `adapter_model::CoreEvent` (canonical
//!   lifecycle) or `DriverEvent`-style progress for the peer side.
//!
//! The local Core `TaskId` remains canonical. The remote ANP `task_id` is
//! kept as correlation metadata and is **not** substituted into
//! `adapter_model::TaskId`.

use adapter_model::{ArtifactRef as CoreArtifactRef, CoreEvent, CoreEventKind};
use uuid::Uuid;

use crate::dto::*;
use crate::ProfileError;

/// Result of mapping a wire DTO into a core command.
#[derive(Debug)]
pub enum MappedCommand {
    Invoke(adapter_model::InvokeRequest),
    Cancel {
        task_id: adapter_model::TaskId,
        reason: Option<String>,
    },
    ProvideInput {
        task_id: adapter_model::TaskId,
        input: Vec<adapter_model::Part>,
    },
}

/// Maps a `TaskInvoke` into a core `Invoke` request.
///
/// The ANP `task_id` becomes the explicit core `task_id` when it parses as a
/// UUID; otherwise the core generates one and the ANP id is preserved as
/// correlation metadata in `context`.
pub fn invoke_to_command(
    inv: &TaskInvoke,
    caller_id: &adapter_model::CallerId,
) -> Result<MappedCommand, ProfileError> {
    crate::validation::validate_invoke(inv)?;
    let parsed_task_id = Uuid::parse_str(inv.task_id.as_str()).ok();
    let mut context = serde_json::json!({
        "anp": {
            "task_id": inv.task_id.as_str(),
            "operation_id": inv.operation_id.0,
            "message_id": inv.message_id.0,
        }
    });
    context["caller"] = serde_json::json!(caller_id.0);
    Ok(MappedCommand::Invoke(adapter_model::InvokeRequest {
        task_id: parsed_task_id,
        agent_id: Some(adapter_model::AgentId(inv.agent.clone())),
        skill_id: None,
        idempotency_key: inv.operation_id.0.clone(),
        session_id: None,
        input: vec![adapter_model::Part::Json {
            value: inv.payload.clone(),
        }],
        context,
        deadline: None,
    }))
}

/// Maps a `TaskCancel` into a core `Cancel` command.
pub fn cancel_to_command(cancel: &TaskCancel) -> Result<MappedCommand, ProfileError> {
    crate::validation::validate_task_id(&cancel.task_id)?;
    crate::validation::validate_operation_id(&cancel.operation_id)?;
    crate::validation::validate_message_id(&cancel.message_id)?;
    let task_id = Uuid::parse_str(cancel.task_id.as_str())
        .ok()
        .ok_or_else(|| ProfileError::InvalidField {
            field: "task_id",
            reason: "must be a UUID to target a local core task".into(),
        })?;
    Ok(MappedCommand::Cancel {
        task_id,
        reason: None,
    })
}

/// Maps a `TaskProvideInput` into a core `ProvideInput` command.
pub fn provide_input_to_command(
    pi: &TaskProvideInput,
    expected_input_request_id: &str,
) -> Result<MappedCommand, ProfileError> {
    crate::validation::validate_provide_input(pi, expected_input_request_id)?;
    let task_id =
        Uuid::parse_str(pi.task_id.as_str())
            .ok()
            .ok_or_else(|| ProfileError::InvalidField {
                field: "task_id",
                reason: "must be a UUID to target a local core task".into(),
            })?;
    Ok(MappedCommand::ProvideInput {
        task_id,
        input: vec![adapter_model::Part::Json {
            value: pi.payload.clone(),
        }],
    })
}

/// Maps a validated `TaskEvent` into the canonical `CoreEvent`.
pub fn event_to_core_event(
    ev: &TaskEvent,
    task_id: adapter_model::TaskId,
    at: chrono::DateTime<chrono::Utc>,
) -> Result<CoreEvent, ProfileError> {
    crate::validation::validate_event(ev)?;
    let seq = ev.seq().unwrap_or(0);
    let kind = match ev {
        TaskEvent::Accepted(_) => CoreEventKind::Accepted { queued: false },
        TaskEvent::Status(e) => match e.state {
            TaskState::Completed => CoreEventKind::Completed { output: vec![] },
            TaskState::Failed => CoreEventKind::Failed {
                error: adapter_model::PublicError {
                    code: "anp.task_failed".into(),
                    message: "remote task failed".into(),
                    retryable: false,
                },
            },
            TaskState::Cancelled => CoreEventKind::Cancelled,
        },
        TaskEvent::InputRequired(e) => CoreEventKind::InputRequired {
            request: adapter_model::InputRequest {
                question: e.prompt.clone(),
                schema: None,
            },
        },
        TaskEvent::Progress(e) => {
            let text = e
                .payload
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("progress")
                .to_string();
            CoreEventKind::Progress {
                message: text,
                percent: None,
            }
        }
        TaskEvent::Artifact(e) => CoreEventKind::Artifact {
            artifact: to_core_artifact(&e.artifact),
        },
        TaskEvent::Completed(e) => CoreEventKind::Completed {
            output: e.artifacts.iter().map(to_core_part).collect::<Vec<_>>(),
        },
        TaskEvent::Failed(e) => CoreEventKind::Failed {
            error: adapter_model::PublicError {
                code: "anp.task_failed".into(),
                message: e.error.clone(),
                retryable: false,
            },
        },
        TaskEvent::Cancelled(_) => CoreEventKind::Cancelled,
    };
    Ok(CoreEvent {
        task_id,
        seq,
        at,
        kind,
    })
}

fn to_core_artifact(a: &ArtifactRef) -> CoreArtifactRef {
    CoreArtifactRef {
        id: a.uri.clone(),
        name: a.uri.rsplit('/').next().unwrap_or("artifact").to_string(),
        mime_type: a
            .mime_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".into()),
        size_bytes: 0,
        uri: Some(a.uri.clone()),
    }
}

fn to_core_part(a: &ArtifactRef) -> adapter_model::Part {
    adapter_model::Part::FileRef {
        uri: a.uri.clone(),
        mime_type: a.mime_type.clone(),
    }
}
