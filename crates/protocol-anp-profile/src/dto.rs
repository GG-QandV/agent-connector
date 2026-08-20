//! Wire DTOs for `agent-connector.anp-task.v1`.
//!
//! All types are serializable and carry validation rules described in the
//! `validation` module. Field names are part of the compatibility contract.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

/// Invoke a task on the peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInvoke {
    pub task_id: TaskId,
    pub operation_id: OperationId,
    pub message_id: MessageId,
    /// Agent name/ID the peer should route to.
    pub agent: String,
    /// JSON-encoded invoke payload (the app-level request body).
    pub payload: serde_json::Value,
}

/// Positive acceptance of a `TaskInvoke`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAccepted {
    pub task_id: TaskId,
    pub operation_id: OperationId,
    pub message_id: MessageId,
}

/// Status report for a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    pub task_id: TaskId,
    pub seq: Seq,
    pub message_id: MessageId,
    pub state: TaskState,
}

/// Request to cancel a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCancel {
    pub task_id: TaskId,
    pub operation_id: OperationId,
    pub message_id: MessageId,
}

/// Peer asks the initiator for more input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInputRequired {
    pub task_id: TaskId,
    pub seq: Seq,
    pub message_id: MessageId,
    /// Stable request id so the reply can be correlated.
    pub input_request_id: String,
    /// Prompt/description for the required input.
    pub prompt: String,
}

/// Initiator supplies the requested input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProvideInput {
    pub task_id: TaskId,
    pub operation_id: OperationId,
    pub message_id: MessageId,
    /// Must match the `input_request_id` from `TaskInputRequired`.
    pub input_request_id: String,
    pub payload: serde_json::Value,
}

/// Non-terminal progress update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProgress {
    pub task_id: TaskId,
    pub seq: Seq,
    pub message_id: MessageId,
    pub payload: serde_json::Value,
}

/// Artifact produced by the task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskArtifact {
    pub task_id: TaskId,
    pub seq: Seq,
    pub message_id: MessageId,
    pub artifact: ArtifactRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub uri: String,
    pub mime_type: Option<String>,
}

/// Terminal event: completed successfully.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompleted {
    pub task_id: TaskId,
    pub seq: Seq,
    pub message_id: MessageId,
    pub artifacts: Vec<ArtifactRef>,
}

/// Terminal event: failed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFailed {
    pub task_id: TaskId,
    pub seq: Seq,
    pub message_id: MessageId,
    pub error: String,
}

/// Terminal event: cancelled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCancelled {
    pub task_id: TaskId,
    pub seq: Seq,
    pub message_id: MessageId,
}

/// Terminal state of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Completed,
    Failed,
    Cancelled,
}

/// Unified wire event envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskEvent {
    Accepted(TaskAccepted),
    Status(TaskStatus),
    InputRequired(TaskInputRequired),
    Progress(TaskProgress),
    Artifact(TaskArtifact),
    Completed(TaskCompleted),
    Failed(TaskFailed),
    Cancelled(TaskCancelled),
}

impl TaskEvent {
    /// Sequence number of this event, if it carries one.
    pub fn seq(&self) -> Option<Seq> {
        match self {
            TaskEvent::Status(e) => Some(e.seq),
            TaskEvent::InputRequired(e) => Some(e.seq),
            TaskEvent::Progress(e) => Some(e.seq),
            TaskEvent::Artifact(e) => Some(e.seq),
            TaskEvent::Completed(e) => Some(e.seq),
            TaskEvent::Failed(e) => Some(e.seq),
            TaskEvent::Cancelled(e) => Some(e.seq),
            TaskEvent::Accepted(_) => None,
        }
    }

    pub fn task_id(&self) -> &TaskId {
        match self {
            TaskEvent::Accepted(e) => &e.task_id,
            TaskEvent::Status(e) => &e.task_id,
            TaskEvent::InputRequired(e) => &e.task_id,
            TaskEvent::Progress(e) => &e.task_id,
            TaskEvent::Artifact(e) => &e.task_id,
            TaskEvent::Completed(e) => &e.task_id,
            TaskEvent::Failed(e) => &e.task_id,
            TaskEvent::Cancelled(e) => &e.task_id,
        }
    }

    pub fn message_id(&self) -> &MessageId {
        match self {
            TaskEvent::Accepted(e) => &e.message_id,
            TaskEvent::Status(e) => &e.message_id,
            TaskEvent::InputRequired(e) => &e.message_id,
            TaskEvent::Progress(e) => &e.message_id,
            TaskEvent::Artifact(e) => &e.message_id,
            TaskEvent::Completed(e) => &e.message_id,
            TaskEvent::Failed(e) => &e.message_id,
            TaskEvent::Cancelled(e) => &e.message_id,
        }
    }

    /// True if this event is terminal.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskEvent::Completed(_) | TaskEvent::Failed(_) | TaskEvent::Cancelled(_)
        )
    }
}
