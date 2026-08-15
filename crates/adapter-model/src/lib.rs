//! `adapter-model` — DTO, identifiers and schema helpers for the Universal
//! Agent Adapter Runtime.
//!
//! This crate contains no runtime, no I/O, no protocol SDK and no framework
//! types. `adapter-core` depends on it; protocol mappers and storage adapters
//! convert to/from these types at their boundaries.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type TaskId = Uuid;
pub type SessionId = Uuid;
pub type EventSeq = u64;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CallerId(pub String);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Caller {
    pub id: CallerId,
    pub scopes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Part {
    Text {
        text: String,
    },
    Json {
        value: serde_json::Value,
    },
    FileRef {
        uri: String,
        mime_type: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub id: String,
    pub name: String,
    pub mime_type: String,
    pub size_bytes: u64,
    pub uri: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InputRequest {
    pub question: String,
    pub schema: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublicError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    Created,
    Accepted,
    Running,
    WaitingForInput,
    CancelRequested,
    Completed,
    Failed,
    Cancelled,
}

impl TaskState {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CoreEventKind {
    Accepted {
        queued: bool,
    },
    Progress {
        message: String,
        percent: Option<u8>,
    },
    Artifact {
        artifact: ArtifactRef,
    },
    InputRequired {
        request: InputRequest,
    },
    Completed {
        output: Vec<Part>,
    },
    Failed {
        error: PublicError,
    },
    CancelRequested {
        reason: Option<String>,
    },
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoreEvent {
    pub task_id: TaskId,
    pub seq: EventSeq,
    pub at: DateTime<Utc>,
    pub kind: CoreEventKind,
}

#[derive(Clone, Debug)]
pub struct InvokeRequest {
    /// Явный task_id (для wire-слоёв A2A/ACP, где id генерирует клиент).
    /// Если None — core генерирует новый UUID.
    pub task_id: Option<TaskId>,
    pub agent_id: Option<AgentId>,
    pub skill_id: Option<String>,
    pub idempotency_key: String,
    pub session_id: Option<SessionId>,
    pub input: Vec<Part>,
    pub context: serde_json::Value,
    pub deadline: Option<Duration>,
}

#[derive(Clone, Debug)]
pub enum CoreCommand {
    Invoke(InvokeRequest),
    Cancel {
        task_id: TaskId,
        reason: Option<String>,
    },
    ProvideInput {
        task_id: TaskId,
        input: Vec<Part>,
    },
    GetStatus {
        task_id: TaskId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskSnapshot {
    pub task_id: TaskId,
    pub session_id: Option<SessionId>,
    pub agent_id: AgentId,
    pub caller_id: CallerId,
    pub state: TaskState,
    pub revision: u64,
    pub last_seq: EventSeq,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub terminal_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub enum DispatchResult {
    Created(TaskSnapshot),
    Existing(TaskSnapshot),
    Status(TaskSnapshot),
    CancelRequested(TaskSnapshot),
    InputAccepted(TaskSnapshot),
}

#[derive(Clone, Debug)]
pub struct NewTask {
    pub task_id: TaskId,
    pub session_id: Option<SessionId>,
    pub agent_id: AgentId,
    pub caller_id: CallerId,
    pub idempotency_key: String,
    pub deadline_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub enum CreateTaskResult {
    Created(TaskSnapshot),
    Existing(TaskSnapshot),
}

#[derive(Clone, Debug)]
pub struct TaskTransition {
    pub task_id: TaskId,
    pub expected_revision: u64,
    pub allowed_states: Vec<TaskState>,
    pub next_state: TaskState,
    pub event_kind: CoreEventKind,
}

#[derive(Clone, Debug)]
pub struct AppliedTransition {
    pub snapshot: TaskSnapshot,
    pub event: CoreEvent,
}

#[derive(Clone, Debug)]
pub struct DriverCapabilities {
    pub cancellation: bool,
    pub provide_input: bool,
}

#[derive(Clone, Debug)]
pub struct AgentLimits {
    pub max_concurrent_tasks: usize,
    pub max_queued_tasks: usize,
    pub max_input_bytes: usize,
    pub max_event_bytes: usize,
    pub default_timeout: Duration,
}
