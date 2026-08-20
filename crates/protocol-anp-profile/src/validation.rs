//! Validation rules for `agent-connector.anp-task.v1`.

use crate::dto::*;
use crate::{ProfileError, ProfileResult, MAX_PAYLOAD_BYTES, PROFILE_ID, PROFILE_VERSION};

/// Validates that a profile id/version pair matches this profile.
pub fn validate_profile(profile_id: &str, version: u32) -> ProfileResult<()> {
    if profile_id != PROFILE_ID {
        return Err(ProfileError::UnsupportedProfileId(profile_id.to_string()));
    }
    if version != PROFILE_VERSION {
        return Err(ProfileError::VersionMismatch {
            expected: PROFILE_VERSION,
            actual: version,
        });
    }
    Ok(())
}

pub(crate) fn validate_task_id(task_id: &TaskId) -> ProfileResult<()> {
    if task_id.0.trim().is_empty() {
        return Err(ProfileError::InvalidField {
            field: "task_id",
            reason: "must not be empty".into(),
        });
    }
    Ok(())
}

pub(crate) fn validate_operation_id(op: &OperationId) -> ProfileResult<()> {
    if op.0.trim().is_empty() {
        return Err(ProfileError::InvalidField {
            field: "operation_id",
            reason: "must not be empty".into(),
        });
    }
    Ok(())
}

pub(crate) fn validate_message_id(msg: &MessageId) -> ProfileResult<()> {
    if msg.0.trim().is_empty() {
        return Err(ProfileError::InvalidField {
            field: "message_id",
            reason: "must not be empty".into(),
        });
    }
    Ok(())
}

fn validate_payload(payload: &serde_json::Value) -> ProfileResult<()> {
    let size = serde_json::to_vec(payload)?.len();
    if size > MAX_PAYLOAD_BYTES {
        return Err(ProfileError::PayloadTooLarge {
            actual: size,
            max: MAX_PAYLOAD_BYTES,
        });
    }
    Ok(())
}

fn validate_seq(seq: Seq) -> ProfileResult<()> {
    if seq == 0 {
        return Err(ProfileError::InvalidSequence {
            reason: "seq must be >= 1".into(),
        });
    }
    Ok(())
}

/// Validates a `TaskInvoke` payload.
pub fn validate_invoke(inv: &TaskInvoke) -> ProfileResult<()> {
    validate_task_id(&inv.task_id)?;
    validate_operation_id(&inv.operation_id)?;
    validate_message_id(&inv.message_id)?;
    if inv.agent.trim().is_empty() {
        return Err(ProfileError::InvalidField {
            field: "agent",
            reason: "must not be empty".into(),
        });
    }
    validate_payload(&inv.payload)
}

/// Validates a `TaskProvideInput` against the input request it answers.
pub fn validate_provide_input(pi: &TaskProvideInput, input_request_id: &str) -> ProfileResult<()> {
    validate_task_id(&pi.task_id)?;
    validate_operation_id(&pi.operation_id)?;
    validate_message_id(&pi.message_id)?;
    if pi.input_request_id != input_request_id {
        return Err(ProfileError::InvalidField {
            field: "input_request_id",
            reason: format!("does not match expected input request `{input_request_id}`"),
        });
    }
    validate_payload(&pi.payload)
}

/// Validates a wire event envelope.
///
/// Enforces:
/// - task/message ids present;
/// - `seq >= 1` on seq-bearing events;
/// - terminal events must not be followed by more events (caller enforces
///   ordering with a monotonic check, but we verify terminal markers carry
///   a valid seq);
/// - payload size caps.
pub fn validate_event(ev: &TaskEvent) -> ProfileResult<()> {
    validate_task_id(ev.task_id())?;
    validate_message_id(ev.message_id())?;
    if let Some(seq) = ev.seq() {
        validate_seq(seq)?;
    }
    match ev {
        TaskEvent::Progress(e) => validate_payload(&e.payload)?,
        TaskEvent::Completed(e) => {
            if e.artifacts.iter().any(|a| a.uri.trim().is_empty()) {
                return Err(ProfileError::InvalidField {
                    field: "artifacts[].uri",
                    reason: "must not be empty".into(),
                });
            }
        }
        TaskEvent::Failed(e) => {
            if e.error.trim().is_empty() {
                return Err(ProfileError::InvalidField {
                    field: "error",
                    reason: "must not be empty".into(),
                });
            }
        }
        TaskEvent::InputRequired(e) if e.input_request_id.trim().is_empty() => {
            return Err(ProfileError::InvalidField {
                field: "input_request_id",
                reason: "must not be empty".into(),
            });
        }
        _ => {}
    }
    Ok(())
}

/// Checks that an incoming event is monotonic w.r.t. the previous seq.
pub fn check_monotonic(prev: Option<Seq>, next: Seq) -> ProfileResult<()> {
    match prev {
        None => validate_seq(next),
        Some(p) if next > p => Ok(()),
        Some(p) => Err(ProfileError::InvalidSequence {
            reason: format!("seq {next} must be strictly greater than previous {p}"),
        }),
    }
}

/// Verifies no event follows a terminal one.
pub fn ensure_not_after_terminal(prev_terminal: bool) -> ProfileResult<()> {
    if prev_terminal {
        return Err(ProfileError::TerminalRuleViolation(
            "event after terminal event".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::{ArtifactRef, MessageId, OperationId, TaskId};

    fn valid_invoke() -> TaskInvoke {
        TaskInvoke {
            task_id: TaskId("t-1".into()),
            operation_id: OperationId("op-1".into()),
            message_id: MessageId("m-1".into()),
            agent: "assistant".into(),
            payload: serde_json::json!({"query": "hello"}),
        }
    }

    #[test]
    fn invoke_valid() {
        assert!(validate_invoke(&valid_invoke()).is_ok());
    }

    #[test]
    fn invoke_empty_agent_rejected() {
        let mut inv = valid_invoke();
        inv.agent = "".into();
        assert!(matches!(
            validate_invoke(&inv),
            Err(ProfileError::InvalidField { field: "agent", .. })
        ));
    }

    #[test]
    fn profile_mismatch_rejected() {
        assert!(matches!(
            validate_profile("agent-connector.other.v1", 1),
            Err(ProfileError::UnsupportedProfileId(_))
        ));
        assert!(matches!(
            validate_profile(PROFILE_ID, 2),
            Err(ProfileError::VersionMismatch { .. })
        ));
    }

    #[test]
    fn seq_zero_rejected() {
        assert!(matches!(
            validate_seq(0),
            Err(ProfileError::InvalidSequence { .. })
        ));
    }

    #[test]
    fn monotonic_check() {
        assert!(check_monotonic(None, 1).is_ok());
        assert!(check_monotonic(Some(1), 2).is_ok());
        assert!(matches!(
            check_monotonic(Some(2), 2),
            Err(ProfileError::InvalidSequence { .. })
        ));
        assert!(matches!(
            check_monotonic(Some(5), 3),
            Err(ProfileError::InvalidSequence { .. })
        ));
    }

    #[test]
    fn event_after_terminal_rejected() {
        assert!(ensure_not_after_terminal(true).is_err());
        assert!(ensure_not_after_terminal(false).is_ok());
    }

    #[test]
    fn terminal_with_empty_error_rejected() {
        let ev = TaskEvent::Failed(TaskFailed {
            task_id: TaskId("t-1".into()),
            seq: 1,
            message_id: MessageId("m-1".into()),
            error: "".into(),
        });
        assert!(matches!(
            validate_event(&ev),
            Err(ProfileError::InvalidField { field: "error", .. })
        ));
    }

    #[test]
    fn completed_with_empty_artifact_uri_rejected() {
        let ev = TaskEvent::Completed(TaskCompleted {
            task_id: TaskId("t-1".into()),
            seq: 2,
            message_id: MessageId("m-2".into()),
            artifacts: vec![ArtifactRef {
                uri: "".into(),
                mime_type: None,
            }],
        });
        assert!(matches!(
            validate_event(&ev),
            Err(ProfileError::InvalidField { .. })
        ));
    }

    #[test]
    fn is_terminal_classification() {
        assert!(TaskEvent::Completed(TaskCompleted {
            task_id: TaskId("t".into()),
            seq: 1,
            message_id: MessageId("m".into()),
            artifacts: vec![],
        })
        .is_terminal());
        assert!(!TaskEvent::Progress(TaskProgress {
            task_id: TaskId("t".into()),
            seq: 1,
            message_id: MessageId("m".into()),
            payload: serde_json::json!({}),
        })
        .is_terminal());
    }
}
