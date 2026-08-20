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
//!
//! The mapper never generates request ids: `operation_id`/`message_id` come
//! from the wire payload unchanged.

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
    let parsed_task_id = Uuid::parse_str(inv.envelope.task_id.as_str()).ok();
    let mut context = serde_json::json!({
        "anp": {
            "task_id": inv.envelope.task_id.as_str(),
            "operation_id": inv.envelope.operation_id.0,
            "message_id": inv.envelope.message_id.0,
        }
    });
    context["caller"] = serde_json::json!(caller_id.0);
    Ok(MappedCommand::Invoke(adapter_model::InvokeRequest {
        task_id: parsed_task_id,
        agent_id: Some(adapter_model::AgentId(inv.agent.clone())),
        skill_id: None,
        idempotency_key: inv.envelope.operation_id.0.clone(),
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
    crate::validation::validate_event(&TaskEvent::TaskCancel(cancel.clone()))?;
    let task_id = Uuid::parse_str(cancel.envelope.task_id.as_str())
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
    let task_id = Uuid::parse_str(pi.envelope.task_id.as_str())
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
        TaskEvent::TaskAccepted(_) => CoreEventKind::Accepted { queued: false },
        TaskEvent::TaskStatus(e) => match e.state {
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
        TaskEvent::TaskInputRequired(e) => CoreEventKind::InputRequired {
            request: adapter_model::InputRequest {
                question: e.prompt.clone(),
                schema: None,
            },
        },
        TaskEvent::TaskProgress(e) => {
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
        TaskEvent::TaskArtifact(e) => CoreEventKind::Artifact {
            artifact: to_core_artifact(&e.artifact),
        },
        TaskEvent::TaskCompleted(e) => CoreEventKind::Completed {
            output: e.artifacts.iter().map(to_core_part).collect::<Vec<_>>(),
        },
        TaskEvent::TaskFailed(e) => CoreEventKind::Failed {
            error: adapter_model::PublicError {
                code: "anp.task_failed".into(),
                message: e.error.clone(),
                retryable: false,
            },
        },
        TaskEvent::TaskCancelled(_) => CoreEventKind::Cancelled,
        TaskEvent::TaskInvoke(_)
        | TaskEvent::TaskGetStatus(_)
        | TaskEvent::TaskEvents(_)
        | TaskEvent::TaskCancel(_)
        | TaskEvent::TaskProvideInput(_) => {
            return Err(ProfileError::InvalidField {
                field: "message_type",
                reason: "outbound operation is not a core event".into(),
            })
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use adapter_model::{CallerId, TaskId as CoreTaskId};
    use chrono::{TimeZone, Utc};

    fn env() -> MessageEnvelope {
        MessageEnvelope::new(
            TaskId("t-1".into()),
            OperationId("op-1".into()),
            MessageId("m-1".into()),
        )
    }

    fn caller() -> adapter_model::CallerId {
        CallerId("caller-1".into())
    }

    #[test]
    fn invoke_maps_to_core_invoke() {
        let inv = TaskInvoke {
            envelope: env(),
            agent: "assistant".into(),
            payload: serde_json::json!({"query": "hello"}),
        };
        let cmd = invoke_to_command(&inv, &caller()).unwrap();
        match cmd {
            MappedCommand::Invoke(req) => {
                assert_eq!(
                    req.agent_id,
                    Some(adapter_model::AgentId("assistant".into()))
                );
                assert_eq!(req.idempotency_key, "op-1");
                assert_eq!(req.context["anp"]["task_id"], "t-1");
                assert_eq!(req.context["caller"], "caller-1");
                assert!(matches!(req.input[0], adapter_model::Part::Json { .. }));
            }
            other => panic!("expected Invoke, got {other:?}"),
        }
    }

    #[test]
    fn cancel_maps_to_core_cancel() {
        let cancel = TaskCancel {
            envelope: MessageEnvelope::new(
                TaskId("00000000-0000-0000-0000-000000000001".into()),
                OperationId("op-1".into()),
                MessageId("m-1".into()),
            ),
            payload: serde_json::json!({}),
        };
        let cmd = cancel_to_command(&cancel).unwrap();
        match cmd {
            MappedCommand::Cancel { task_id, reason } => {
                assert_eq!(task_id.to_string(), "00000000-0000-0000-0000-000000000001");
                assert!(reason.is_none());
            }
            other => panic!("expected Cancel, got {other:?}"),
        }
    }

    #[test]
    fn provide_input_maps_to_core_provide_input() {
        let pi = TaskProvideInput {
            envelope: MessageEnvelope::new(
                TaskId("00000000-0000-0000-0000-000000000001".into()),
                OperationId("op-1".into()),
                MessageId("m-1".into()),
            ),
            input_request_id: "req-1".into(),
            payload: serde_json::json!({"answer": 42}),
        };
        let cmd = provide_input_to_command(&pi, "req-1").unwrap();
        match cmd {
            MappedCommand::ProvideInput { task_id, input } => {
                assert_eq!(task_id.to_string(), "00000000-0000-0000-0000-000000000001");
                assert!(matches!(input[0], adapter_model::Part::Json { .. }));
            }
            other => panic!("expected ProvideInput, got {other:?}"),
        }
    }

    #[test]
    fn provide_input_wrong_request_id_rejected() {
        let pi = TaskProvideInput {
            envelope: env(),
            input_request_id: "req-1".into(),
            payload: serde_json::json!({}),
        };
        assert!(matches!(
            provide_input_to_command(&pi, "req-2"),
            Err(ProfileError::InvalidField {
                field: "input_request_id",
                ..
            })
        ));
    }

    fn core_id() -> CoreTaskId {
        CoreTaskId::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
    }

    fn at() -> chrono::DateTime<chrono::Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    #[test]
    fn accepted_maps_to_accepted_event() {
        let ev = TaskEvent::TaskAccepted(TaskAccepted {
            envelope: env(),
            payload: serde_json::json!({}),
        });
        let core = event_to_core_event(&ev, core_id(), at()).unwrap();
        assert!(matches!(
            core.kind,
            CoreEventKind::Accepted { queued: false }
        ));
    }

    #[test]
    fn progress_maps_to_progress_event() {
        let ev = TaskEvent::TaskProgress(TaskProgress {
            envelope: env(),
            seq: 1,
            payload: serde_json::json!({"message": "working"}),
        });
        let core = event_to_core_event(&ev, core_id(), at()).unwrap();
        assert!(matches!(
            core.kind,
            CoreEventKind::Progress { ref message, .. } if message == "working"
        ));
        assert_eq!(core.seq, 1);
    }

    #[test]
    fn artifact_maps_to_artifact_event() {
        let ev = TaskEvent::TaskArtifact(TaskArtifact {
            envelope: env(),
            seq: 2,
            artifact: ArtifactRef {
                uri: "https://peer.example/a.md".into(),
                mime_type: Some("text/markdown".into()),
            },
            payload: serde_json::json!({}),
        });
        let core = event_to_core_event(&ev, core_id(), at()).unwrap();
        assert!(matches!(
            core.kind,
            CoreEventKind::Artifact { ref artifact }
                if artifact.uri.as_deref() == Some("https://peer.example/a.md")
        ));
    }

    #[test]
    fn input_required_maps_to_input_required_event() {
        let ev = TaskEvent::TaskInputRequired(TaskInputRequired {
            envelope: env(),
            seq: 3,
            input_request_id: "req-1".into(),
            prompt: "give me the number".into(),
            payload: serde_json::json!({}),
        });
        let core = event_to_core_event(&ev, core_id(), at()).unwrap();
        assert!(matches!(
            core.kind,
            CoreEventKind::InputRequired { ref request }
                if request.question == "give me the number"
        ));
    }

    #[test]
    fn completed_maps_to_completed_event() {
        let ev = TaskEvent::TaskCompleted(TaskCompleted {
            envelope: env(),
            seq: 5,
            is_final: true,
            artifacts: vec![ArtifactRef {
                uri: "https://peer.example/summary.md".into(),
                mime_type: Some("text/markdown".into()),
            }],
            payload: serde_json::json!({}),
        });
        let core = event_to_core_event(&ev, core_id(), at()).unwrap();
        assert!(matches!(
            core.kind,
            CoreEventKind::Completed { ref output } if output.len() == 1
        ));
    }

    #[test]
    fn failed_maps_to_failed_event() {
        let ev = TaskEvent::TaskFailed(TaskFailed {
            envelope: env(),
            seq: 5,
            is_final: true,
            error: "boom".into(),
            payload: serde_json::json!({}),
        });
        let core = event_to_core_event(&ev, core_id(), at()).unwrap();
        assert!(matches!(
            core.kind,
            CoreEventKind::Failed { ref error } if error.message == "boom"
        ));
    }

    #[test]
    fn cancelled_maps_to_cancelled_event() {
        let ev = TaskEvent::TaskCancelled(TaskCancelled {
            envelope: env(),
            seq: 5,
            is_final: true,
            payload: serde_json::json!({}),
        });
        let core = event_to_core_event(&ev, core_id(), at()).unwrap();
        assert!(matches!(core.kind, CoreEventKind::Cancelled));
    }

    #[test]
    fn outbound_operations_are_not_core_events() {
        let ev = TaskEvent::TaskInvoke(TaskInvoke {
            envelope: env(),
            agent: "a".into(),
            payload: serde_json::json!({}),
        });
        assert!(event_to_core_event(&ev, core_id(), at()).is_err());
    }

    #[test]
    fn duplicate_seq_rejected() {
        let prev = Some(2u64);
        assert!(matches!(
            crate::validation::check_monotonic(prev, 2),
            Err(ProfileError::InvalidSequence { .. })
        ));
    }

    #[test]
    fn gap_is_recoverable_not_rejected() {
        // A forward gap (seq jump) is not a validation error: ReliableTaskStream
        // resolves gaps via durable catch-up. check_monotonic only rejects
        // non-increasing sequences.
        let prev = Some(2u64);
        assert!(crate::validation::check_monotonic(prev, 5).is_ok());
        // Backward / duplicate seq is rejected.
        assert!(matches!(
            crate::validation::check_monotonic(Some(5), 2),
            Err(ProfileError::InvalidSequence { .. })
        ));
    }

    #[test]
    fn terminal_event_followed_by_event_rejected() {
        assert!(crate::validation::ensure_not_after_terminal(true).is_err());
        assert!(crate::validation::ensure_not_after_terminal(false).is_ok());
    }

    #[test]
    fn payload_size_limit_enforced() {
        let big = serde_json::json!({"blob": "x".repeat(2 * 1024 * 1024)});
        let inv = TaskInvoke {
            envelope: env(),
            agent: "a".into(),
            payload: big,
        };
        assert!(matches!(
            invoke_to_command(&inv, &caller()),
            Err(ProfileError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn operation_and_message_ids_pass_through_unchanged() {
        let inv = TaskInvoke {
            envelope: env(),
            agent: "a".into(),
            payload: serde_json::json!({}),
        };
        let cmd = invoke_to_command(&inv, &caller()).unwrap();
        match cmd {
            MappedCommand::Invoke(req) => {
                assert_eq!(req.idempotency_key, "op-1");
                assert_eq!(req.context["anp"]["message_id"], "m-1");
            }
            other => panic!("expected Invoke, got {other:?}"),
        }
    }
}
