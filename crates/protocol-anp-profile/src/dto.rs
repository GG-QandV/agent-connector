//! Wire DTOs for `agent-connector.anp-task.v1`.
//!
//! The canonical wire contract is `docs/schemas/anp-task-v1.schema.json`.
//! These DTOs are aligned to it: a single envelope (`profile`, `version`,
//! `task_id`, `operation_id`, `message_id`) plus a `message_type`-tagged
//! body. Field names, enum values and required-ness must not diverge from
//! the schema without an ADR update.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{PROFILE_ID, PROFILE_VERSION};

/// Local task identifier assigned by the sender (stable across messages).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(pub String);

impl TaskId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Operation identifier — dedups idempotent operations.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OperationId(pub String);

/// Message identifier — unique per sender, enables ack/dedup.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub String);

impl MessageId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

/// Event sequence — strictly increasing per task, no gaps, no duplicates.
pub type Seq = u64;

/// Common envelope shared by every wire message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageEnvelope {
    /// Always `agent-connector.anp-task.v1` (schema `const`).
    pub profile: String,
    /// Profile version (schema `const` = 1).
    pub version: u32,
    pub task_id: TaskId,
    pub operation_id: OperationId,
    pub message_id: MessageId,
}

impl MessageEnvelope {
    pub fn new(task_id: TaskId, operation_id: OperationId, message_id: MessageId) -> Self {
        Self {
            profile: PROFILE_ID.to_string(),
            version: PROFILE_VERSION,
            task_id,
            operation_id,
            message_id,
        }
    }
}

/// Invoke a task on the peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInvoke {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    /// Agent name/ID the peer should route to.
    pub agent: String,
    /// JSON-encoded invoke payload (the app-level request body).
    pub payload: serde_json::Value,
}

/// Request to cancel a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCancel {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    pub payload: serde_json::Value,
}

/// Status request for a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskGetStatus {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    pub payload: serde_json::Value,
}

/// Resume request: fetch events with seq > after_seq (0 = full history).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEventsRequest {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    pub after_seq: Seq,
    pub payload: serde_json::Value,
}

/// Initiator supplies the requested input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProvideInput {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    /// Must match the `input_request_id` from `TaskInputRequired`.
    pub input_request_id: String,
    pub payload: serde_json::Value,
}

/// Positive acceptance of a `TaskInvoke`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAccepted {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    pub payload: serde_json::Value,
}

/// Status report for a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskStatus {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    pub seq: Seq,
    pub state: TaskState,
    pub payload: serde_json::Value,
}

/// Peer asks the initiator for more input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInputRequired {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    pub seq: Seq,
    /// Stable request id so the reply can be correlated.
    pub input_request_id: String,
    /// Prompt/description for the required input.
    pub prompt: String,
    pub payload: serde_json::Value,
}

/// Non-terminal progress update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProgress {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    pub seq: Seq,
    pub payload: serde_json::Value,
}

/// Artifact produced by the task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskArtifact {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    pub seq: Seq,
    pub artifact: ArtifactRef,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub uri: String,
    pub mime_type: Option<String>,
}

/// Terminal event: completed successfully.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCompleted {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    pub seq: Seq,
    #[serde(rename = "final", default = "default_final_true")]
    pub is_final: bool,
    pub artifacts: Vec<ArtifactRef>,
    pub payload: serde_json::Value,
}

/// Terminal event: failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskFailed {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    pub seq: Seq,
    #[serde(rename = "final", default = "default_final_true")]
    pub is_final: bool,
    pub error: String,
    pub payload: serde_json::Value,
}

/// Terminal event: cancelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCancelled {
    #[serde(flatten)]
    pub envelope: MessageEnvelope,
    pub seq: Seq,
    #[serde(rename = "final", default = "default_final_true")]
    pub is_final: bool,
    pub payload: serde_json::Value,
}

fn default_final_true() -> bool {
    true
}

/// Terminal state of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Completed,
    Failed,
    Cancelled,
}

/// Message type discriminant — mirrors the schema `message_type` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    TaskInvoke,
    TaskCancel,
    TaskGetStatus,
    TaskEvents,
    TaskProvideInput,
    TaskAccepted,
    TaskStatus,
    TaskInputRequired,
    TaskProgress,
    TaskArtifact,
    TaskCompleted,
    TaskFailed,
    TaskCancelled,
}

impl MessageType {
    pub fn as_str(self) -> &'static str {
        use MessageType::*;
        match self {
            TaskInvoke => "task.invoke",
            TaskCancel => "task.cancel",
            TaskGetStatus => "task.get_status",
            TaskEvents => "task.events",
            TaskProvideInput => "task.provide_input",
            TaskAccepted => "task.accepted",
            TaskStatus => "task.status",
            TaskInputRequired => "task.input_required",
            TaskProgress => "task.progress",
            TaskArtifact => "task.artifact",
            TaskCompleted => "task.completed",
            TaskFailed => "task.failed",
            TaskCancelled => "task.cancelled",
        }
    }
}

/// Unified wire message (schema `message_type`-tagged).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "message_type", rename_all = "snake_case")]
pub enum TaskEvent {
    #[serde(rename = "task.invoke")]
    TaskInvoke(TaskInvoke),
    #[serde(rename = "task.cancel")]
    TaskCancel(TaskCancel),
    #[serde(rename = "task.get_status")]
    TaskGetStatus(TaskGetStatus),
    #[serde(rename = "task.events")]
    TaskEvents(TaskEventsRequest),
    #[serde(rename = "task.provide_input")]
    TaskProvideInput(TaskProvideInput),
    #[serde(rename = "task.accepted")]
    TaskAccepted(TaskAccepted),
    #[serde(rename = "task.status")]
    TaskStatus(TaskStatus),
    #[serde(rename = "task.input_required")]
    TaskInputRequired(TaskInputRequired),
    #[serde(rename = "task.progress")]
    TaskProgress(TaskProgress),
    #[serde(rename = "task.artifact")]
    TaskArtifact(TaskArtifact),
    #[serde(rename = "task.completed")]
    TaskCompleted(TaskCompleted),
    #[serde(rename = "task.failed")]
    TaskFailed(TaskFailed),
    #[serde(rename = "task.cancelled")]
    TaskCancelled(TaskCancelled),
}

impl TaskEvent {
    pub fn message_type(&self) -> MessageType {
        use MessageType::*;
        match self {
            TaskEvent::TaskInvoke(_) => TaskInvoke,
            TaskEvent::TaskCancel(_) => TaskCancel,
            TaskEvent::TaskGetStatus(_) => TaskGetStatus,
            TaskEvent::TaskEvents(_) => TaskEvents,
            TaskEvent::TaskProvideInput(_) => TaskProvideInput,
            TaskEvent::TaskAccepted(_) => TaskAccepted,
            TaskEvent::TaskStatus(_) => TaskStatus,
            TaskEvent::TaskInputRequired(_) => TaskInputRequired,
            TaskEvent::TaskProgress(_) => TaskProgress,
            TaskEvent::TaskArtifact(_) => TaskArtifact,
            TaskEvent::TaskCompleted(_) => TaskCompleted,
            TaskEvent::TaskFailed(_) => TaskFailed,
            TaskEvent::TaskCancelled(_) => TaskCancelled,
        }
    }

    pub fn envelope(&self) -> &MessageEnvelope {
        match self {
            TaskEvent::TaskInvoke(e) => &e.envelope,
            TaskEvent::TaskCancel(e) => &e.envelope,
            TaskEvent::TaskGetStatus(e) => &e.envelope,
            TaskEvent::TaskEvents(e) => &e.envelope,
            TaskEvent::TaskProvideInput(e) => &e.envelope,
            TaskEvent::TaskAccepted(e) => &e.envelope,
            TaskEvent::TaskStatus(e) => &e.envelope,
            TaskEvent::TaskInputRequired(e) => &e.envelope,
            TaskEvent::TaskProgress(e) => &e.envelope,
            TaskEvent::TaskArtifact(e) => &e.envelope,
            TaskEvent::TaskCompleted(e) => &e.envelope,
            TaskEvent::TaskFailed(e) => &e.envelope,
            TaskEvent::TaskCancelled(e) => &e.envelope,
        }
    }

    /// Sequence number of this event, if it carries one.
    pub fn seq(&self) -> Option<Seq> {
        match self {
            TaskEvent::TaskStatus(e) => Some(e.seq),
            TaskEvent::TaskInputRequired(e) => Some(e.seq),
            TaskEvent::TaskProgress(e) => Some(e.seq),
            TaskEvent::TaskArtifact(e) => Some(e.seq),
            TaskEvent::TaskCompleted(e) => Some(e.seq),
            TaskEvent::TaskFailed(e) => Some(e.seq),
            TaskEvent::TaskCancelled(e) => Some(e.seq),
            _ => None,
        }
    }

    pub fn task_id(&self) -> &TaskId {
        &self.envelope().task_id
    }

    pub fn operation_id(&self) -> &OperationId {
        &self.envelope().operation_id
    }

    pub fn message_id(&self) -> &MessageId {
        &self.envelope().message_id
    }

    /// True if this event is terminal.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskEvent::TaskCompleted(_) | TaskEvent::TaskFailed(_) | TaskEvent::TaskCancelled(_)
        )
    }
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

    #[test]
    fn message_type_matches_schema_enum() {
        let expected = [
            "task.invoke",
            "task.cancel",
            "task.get_status",
            "task.events",
            "task.provide_input",
            "task.accepted",
            "task.status",
            "task.input_required",
            "task.progress",
            "task.artifact",
            "task.completed",
            "task.failed",
            "task.cancelled",
        ];
        use MessageType::*;
        let all = [
            TaskInvoke,
            TaskCancel,
            TaskGetStatus,
            TaskEvents,
            TaskProvideInput,
            TaskAccepted,
            TaskStatus,
            TaskInputRequired,
            TaskProgress,
            TaskArtifact,
            TaskCompleted,
            TaskFailed,
            TaskCancelled,
        ];
        for (m, e) in all.iter().zip(expected) {
            assert_eq!(m.as_str(), e);
        }
    }

    #[test]
    fn serializes_with_profile_and_message_type() {
        let msg = TaskEvent::TaskInvoke(TaskInvoke {
            envelope: env(),
            agent: "assistant".into(),
            payload: serde_json::json!({}),
        });
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v["profile"], PROFILE_ID);
        assert_eq!(v["version"], PROFILE_VERSION);
        assert_eq!(v["message_type"], "task.invoke");
        assert_eq!(v["task_id"], "t-1");
        assert_eq!(v["operation_id"], "op-1");
        assert_eq!(v["message_id"], "m-1");
    }

    #[test]
    fn round_trip() {
        let msg = TaskEvent::TaskProgress(TaskProgress {
            envelope: env(),
            seq: 4,
            payload: serde_json::json!({"message": "hi"}),
        });
        let json = serde_json::to_string(&msg).unwrap();
        let back: TaskEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back, msg);
        assert_eq!(back.seq(), Some(4));
    }

    #[test]
    fn terminal_classification() {
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
