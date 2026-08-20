//! Validation rules for `agent-connector.anp-task.v1`.
//!
//! Rules mirror `docs/schemas/anp-task-v1.schema.json`: unknown profile is
//! rejected, negative/zero `seq` is rejected, terminal events require
//! `final=true`, resume requests require `after_seq`, terminal payload rules
//! are enforced.

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

/// Validates the common envelope: profile, version, ids, payload presence.
pub fn validate_envelope(env: &MessageEnvelope, payload: &serde_json::Value) -> ProfileResult<()> {
    validate_profile(&env.profile, env.version)?;
    validate_task_id(&env.task_id)?;
    validate_operation_id(&env.operation_id)?;
    validate_message_id(&env.message_id)?;
    validate_payload(payload)
}

/// Validates a `TaskInvoke` payload.
pub fn validate_invoke(inv: &TaskInvoke) -> ProfileResult<()> {
    validate_envelope(&inv.envelope, &inv.payload)?;
    if inv.agent.trim().is_empty() {
        return Err(ProfileError::InvalidField {
            field: "agent",
            reason: "must not be empty".into(),
        });
    }
    Ok(())
}

/// Validates a `TaskProvideInput` against the input request it answers.
pub fn validate_provide_input(pi: &TaskProvideInput, input_request_id: &str) -> ProfileResult<()> {
    validate_envelope(&pi.envelope, &pi.payload)?;
    if pi.input_request_id != input_request_id {
        return Err(ProfileError::InvalidField {
            field: "input_request_id",
            reason: format!("does not match expected input request `{input_request_id}`"),
        });
    }
    Ok(())
}

/// Validates a `TaskEventsRequest` resume cursor.
pub fn validate_events_request(req: &TaskEventsRequest) -> ProfileResult<()> {
    validate_envelope(&req.envelope, &req.payload)?;
    // after_seq is a cursor; 0 = full history is allowed. Only negative is
    // impossible in u64; schema enforces minimum 0.
    Ok(())
}

/// Validates a wire message envelope.
///
/// Enforces:
/// - profile/version/id present;
/// - `seq >= 1` on seq-bearing events;
/// - `final=true` on terminal events (schema terminal-final rule);
/// - payload size caps and terminal payload rules.
pub fn validate_event(ev: &TaskEvent) -> ProfileResult<()> {
    validate_task_id(ev.task_id())?;
    validate_operation_id(ev.operation_id())?;
    validate_message_id(ev.message_id())?;
    if let Some(seq) = ev.seq() {
        validate_seq(seq)?;
    }
    match ev {
        TaskEvent::TaskInvoke(e) => validate_invoke(e)?,
        TaskEvent::TaskCancel(e) => validate_envelope(&e.envelope, &e.payload)?,
        TaskEvent::TaskGetStatus(e) => validate_envelope(&e.envelope, &e.payload)?,
        TaskEvent::TaskEvents(e) => validate_events_request(e)?,
        TaskEvent::TaskProvideInput(e) => validate_provide_input(e, &e.input_request_id)?,
        TaskEvent::TaskAccepted(e) => validate_envelope(&e.envelope, &e.payload)?,
        TaskEvent::TaskStatus(e) => validate_envelope(&e.envelope, &e.payload)?,
        TaskEvent::TaskInputRequired(e) => {
            validate_envelope(&e.envelope, &e.payload)?;
            if e.input_request_id.trim().is_empty() {
                return Err(ProfileError::InvalidField {
                    field: "input_request_id",
                    reason: "must not be empty".into(),
                });
            }
            if e.prompt.trim().is_empty() {
                return Err(ProfileError::InvalidField {
                    field: "prompt",
                    reason: "must not be empty".into(),
                });
            }
        }
        TaskEvent::TaskProgress(e) => validate_envelope(&e.envelope, &e.payload)?,
        TaskEvent::TaskArtifact(e) => {
            validate_envelope(&e.envelope, &e.payload)?;
            if e.artifact.uri.trim().is_empty() {
                return Err(ProfileError::InvalidField {
                    field: "artifacts[].uri",
                    reason: "must not be empty".into(),
                });
            }
        }
        TaskEvent::TaskCompleted(e) => {
            validate_envelope(&e.envelope, &e.payload)?;
            if !e.is_final {
                return Err(ProfileError::TerminalRuleViolation(
                    "task.completed must set final=true".into(),
                ));
            }
            if e.artifacts.iter().any(|a| a.uri.trim().is_empty()) {
                return Err(ProfileError::InvalidField {
                    field: "artifacts[].uri",
                    reason: "must not be empty".into(),
                });
            }
        }
        TaskEvent::TaskFailed(e) => {
            validate_envelope(&e.envelope, &e.payload)?;
            if !e.is_final {
                return Err(ProfileError::TerminalRuleViolation(
                    "task.failed must set final=true".into(),
                ));
            }
            if e.error.trim().is_empty() {
                return Err(ProfileError::InvalidField {
                    field: "error",
                    reason: "must not be empty".into(),
                });
            }
        }
        TaskEvent::TaskCancelled(e) => {
            validate_envelope(&e.envelope, &e.payload)?;
            if !e.is_final {
                return Err(ProfileError::TerminalRuleViolation(
                    "task.cancelled must set final=true".into(),
                ));
            }
        }
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

    fn env() -> MessageEnvelope {
        MessageEnvelope::new(
            TaskId("t-1".into()),
            OperationId("op-1".into()),
            MessageId("m-1".into()),
        )
    }

    fn valid_invoke() -> TaskInvoke {
        TaskInvoke {
            envelope: env(),
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
    fn envelope_unknown_profile_rejected() {
        let mut env = env();
        env.profile = "agent-connector.other.v1".into();
        let ev = TaskEvent::TaskProgress(TaskProgress {
            envelope: env,
            seq: 1,
            payload: serde_json::json!({}),
        });
        assert!(matches!(
            validate_event(&ev),
            Err(ProfileError::UnsupportedProfileId(_))
        ));
    }

    #[test]
    fn seq_zero_rejected() {
        let ev = TaskEvent::TaskProgress(TaskProgress {
            envelope: env(),
            seq: 0,
            payload: serde_json::json!({}),
        });
        assert!(matches!(
            validate_event(&ev),
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
        let ev = TaskEvent::TaskFailed(TaskFailed {
            envelope: env(),
            seq: 1,
            is_final: true,
            error: "".into(),
            payload: serde_json::json!({}),
        });
        assert!(matches!(
            validate_event(&ev),
            Err(ProfileError::InvalidField { field: "error", .. })
        ));
    }

    #[test]
    fn completed_missing_final_rejected() {
        let ev = TaskEvent::TaskCompleted(TaskCompleted {
            envelope: env(),
            seq: 2,
            is_final: false,
            artifacts: vec![],
            payload: serde_json::json!({}),
        });
        assert!(matches!(
            validate_event(&ev),
            Err(ProfileError::TerminalRuleViolation(_))
        ));
    }

    #[test]
    fn completed_with_empty_artifact_uri_rejected() {
        let ev = TaskEvent::TaskCompleted(TaskCompleted {
            envelope: env(),
            seq: 2,
            is_final: true,
            artifacts: vec![ArtifactRef {
                uri: "".into(),
                mime_type: None,
            }],
            payload: serde_json::json!({}),
        });
        assert!(matches!(
            validate_event(&ev),
            Err(ProfileError::InvalidField { .. })
        ));
    }

    #[test]
    fn is_terminal_classification() {
        assert!(TaskEvent::TaskCompleted(TaskCompleted {
            envelope: env(),
            seq: 1,
            is_final: true,
            artifacts: vec![],
            payload: serde_json::json!({}),
        })
        .is_terminal());
        assert!(!TaskEvent::TaskProgress(TaskProgress {
            envelope: env(),
            seq: 1,
            payload: serde_json::json!({}),
        })
        .is_terminal());
    }
}
