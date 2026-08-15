//! `protocol-a2a` — transport-neutral A2A <-> Adapter Core mapper.
//!
//! This module deliberately separates A2A semantics from the official SDK's
//! generated/wire types. A thin `a2a_sdk_http` module should convert official
//! `a2a`/`a2a-server` request types into these DTOs. Thus a future SDK update
//! changes only that thin boundary, not Adapter Core, stores or drivers.
//!
//! SDK pin: a2aproject/a2a-rs commit 02ee56024a485a5f184cbc55d1706918ee1ff809.
//!
//! Expected adapter-core public types:
//! Caller, CallerId, CoreCommand, DispatchResult, InvokeRequest, Part,
//! CoreEvent, CoreEventKind, TaskId, TaskSubscription, AdapterCore.

use std::sync::Arc;

use adapter_core::{
    AdapterCore, Caller, CoreCommand, CoreEvent, CoreEventKind, DispatchResult, InvokeRequest,
    Part, TaskId, TaskSubscription,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentCard {
    pub protocol_version: String,
    pub name: String,
    pub description: String,
    pub url: String,
    pub version: String,
    pub capabilities: A2aCapabilities,
    pub skills: Vec<A2aSkill>,
    pub authentication: Vec<AuthScheme>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct A2aCapabilities {
    pub streaming: bool,
    pub push_notifications: bool,
    pub state_transition_history: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct A2aSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AuthScheme {
    Bearer { scheme: String },
    Mtls,
    Oidc { issuer: String, audience: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct A2aPart {
    pub kind: String,
    pub text: Option<String>,
    pub data: Option<serde_json::Value>,
    pub uri: Option<String>,
    pub mime_type: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct A2aMessage {
    pub role: String,
    pub parts: Vec<A2aPart>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct A2aTaskRequest {
    pub idempotency_key: String,
    pub context_id: Option<String>,
    pub task_id: Option<String>,
    pub target_agent_id: Option<String>,
    pub skill_id: Option<String>,
    pub message: A2aMessage,
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct A2aTask {
    pub id: String,
    pub context_id: Option<String>,
    pub status: A2aTaskStatus,
    pub artifacts: Vec<A2aArtifact>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct A2aTaskStatus {
    pub state: String,
    pub message: Option<A2aMessage>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct A2aArtifact {
    pub name: String,
    pub mime_type: String,
    pub parts: Vec<A2aPart>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum A2aStreamEvent {
    TaskStatusUpdate {
        task_id: String,
        status: A2aTaskStatus,
        final_update: bool,
    },
    TaskArtifactUpdate {
        task_id: String,
        artifact: A2aArtifact,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum A2aMapperError {
    #[error("invalid A2A request: {0}")]
    InvalidRequest(String),
    #[error("adapter core error: {0}")]
    Core(#[from] adapter_core::CoreError),
}

/// Optional wrapper allows protocol-a2a to be tested with a fake core and avoids
/// a dependency on storage, HTTP or A2A SDK wire types.
#[async_trait]
pub trait A2aCoreService: Send + Sync {
    async fn dispatch(
        &self,
        caller: Caller,
        command: CoreCommand,
    ) -> Result<DispatchResult, adapter_core::CoreError>;
    async fn subscribe(
        &self,
        task_id: TaskId,
        after_seq: u64,
    ) -> Result<TaskSubscription, adapter_core::CoreError>;
}

#[async_trait]
impl A2aCoreService for AdapterCore {
    async fn dispatch(
        &self,
        caller: Caller,
        command: CoreCommand,
    ) -> Result<DispatchResult, adapter_core::CoreError> {
        self.dispatch(caller, command).await
    }
    async fn subscribe(
        &self,
        task_id: TaskId,
        after_seq: u64,
    ) -> Result<TaskSubscription, adapter_core::CoreError> {
        self.subscribe(task_id, after_seq).await
    }
}

pub struct A2aMapper<C: A2aCoreService> {
    core: Arc<C>,
    card: AgentCard,
}

impl<C: A2aCoreService> A2aMapper<C> {
    pub fn new(core: Arc<C>, card: AgentCard) -> Self {
        Self { core, card }
    }
    pub fn agent_card(&self) -> &AgentCard {
        &self.card
    }

    pub async fn send_task(
        &self,
        caller: Caller,
        request: A2aTaskRequest,
    ) -> Result<A2aTask, A2aMapperError> {
        let command = CoreCommand::Invoke(to_core_invoke(request)?);
        let result = self.core.dispatch(caller, command).await?;
        match result {
            DispatchResult::Created(snapshot) | DispatchResult::Existing(snapshot) => Ok(A2aTask {
                id: snapshot.task_id.to_string(),
                context_id: snapshot.session_id.map(|id| id.to_string()),
                status: status_from_snapshot(&snapshot),
                artifacts: Vec::new(),
            }),
            _ => Err(A2aMapperError::InvalidRequest(
                "unexpected core result for invoke".into(),
            )),
        }
    }

    pub async fn get_task(&self, caller: Caller, task_id: &str) -> Result<A2aTask, A2aMapperError> {
        let task_id = parse_uuid(task_id, "task_id")?;
        let result = self
            .core
            .dispatch(caller, CoreCommand::GetStatus { task_id })
            .await?;
        match result {
            DispatchResult::Status(snapshot) => Ok(A2aTask {
                id: snapshot.task_id.to_string(),
                context_id: snapshot.session_id.map(|id| id.to_string()),
                status: status_from_snapshot(&snapshot),
                artifacts: Vec::new(),
            }),
            _ => Err(A2aMapperError::InvalidRequest(
                "unexpected core result for status".into(),
            )),
        }
    }

    pub async fn cancel_task(
        &self,
        caller: Caller,
        task_id: &str,
    ) -> Result<A2aTask, A2aMapperError> {
        let task_id = parse_uuid(task_id, "task_id")?;
        let result = self
            .core
            .dispatch(
                caller,
                CoreCommand::Cancel {
                    task_id,
                    reason: None,
                },
            )
            .await?;
        match result {
            DispatchResult::CancelRequested(snapshot) => Ok(A2aTask {
                id: snapshot.task_id.to_string(),
                context_id: snapshot.session_id.map(|id| id.to_string()),
                status: status_from_snapshot(&snapshot),
                artifacts: Vec::new(),
            }),
            _ => Err(A2aMapperError::InvalidRequest(
                "unexpected core result for cancel".into(),
            )),
        }
    }

    /// Returns durable catch-up events followed by a live broadcast receiver.
    /// SDK/HTTP layer serializes this iterator as the A2A SSE/streaming binding.
    pub async fn subscribe_task(
        &self,
        caller: Caller,
        task_id: &str,
        after_seq: u64,
    ) -> Result<A2aTaskEventStream, A2aMapperError> {
        let task_id = parse_uuid(task_id, "task_id")?;
        // Authorize read ownership through core. A production PolicyEngine can
        // distinguish ReadTask from GetStatus; MVP uses the same caller identity.
        let _ = self
            .core
            .dispatch(caller, CoreCommand::GetStatus { task_id })
            .await?;
        let subscription = self.core.subscribe(task_id, after_seq).await?;
        Ok(A2aTaskEventStream {
            history: subscription.history.into_iter().map(map_event).collect(),
            receiver: subscription.receiver,
        })
    }
}

pub struct A2aTaskEventStream {
    pub history: Vec<A2aStreamEvent>,
    receiver: broadcast::Receiver<CoreEvent>,
}

impl A2aTaskEventStream {
    /// Returns `Ok(None)` after the sender side has closed. A broadcast lag is
    /// not silently ignored: the HTTP/SSE integration must re-read durable
    /// events from core using the last delivered sequence.
    pub async fn next(&mut self) -> Result<Option<A2aStreamEvent>, broadcast::error::RecvError> {
        match self.receiver.recv().await {
            Ok(event) => Ok(Some(map_event(event))),
            Err(broadcast::error::RecvError::Closed) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

fn to_core_invoke(request: A2aTaskRequest) -> Result<InvokeRequest, A2aMapperError> {
    if request.idempotency_key.trim().is_empty() {
        return Err(A2aMapperError::InvalidRequest(
            "idempotency_key is required".into(),
        ));
    }
    let input = request
        .message
        .parts
        .into_iter()
        .map(to_core_part)
        .collect::<Result<Vec<_>, _>>()?;
    let session_id = request
        .context_id
        .as_deref()
        .map(|value| parse_uuid(value, "context_id"))
        .transpose()?;
    Ok(InvokeRequest {
        task_id: request
            .task_id
            .as_deref()
            .map(parse_uuid_task)
            .transpose()?,
        agent_id: request.target_agent_id.map(adapter_core::AgentId),
        skill_id: request.skill_id,
        idempotency_key: request.idempotency_key,
        session_id,
        input,
        context: request.metadata,
        deadline: None,
    })
}

fn to_core_part(part: A2aPart) -> Result<Part, A2aMapperError> {
    match part.kind.as_str() {
        "text" => Ok(Part::Text {
            text: part.text.unwrap_or_default(),
        }),
        "data" | "json" => Ok(Part::Json {
            value: part.data.unwrap_or(serde_json::Value::Null),
        }),
        "file" | "file_ref" => Ok(Part::FileRef {
            uri: part
                .uri
                .ok_or_else(|| A2aMapperError::InvalidRequest("file part requires uri".into()))?,
            mime_type: part.mime_type,
        }),
        kind => Err(A2aMapperError::InvalidRequest(format!(
            "unsupported A2A part kind: {kind}"
        ))),
    }
}

fn map_event(event: CoreEvent) -> A2aStreamEvent {
    let task_id = event.task_id.to_string();
    match event.kind {
        CoreEventKind::Artifact { artifact } => A2aStreamEvent::TaskArtifactUpdate {
            task_id,
            artifact: A2aArtifact {
                name: artifact.name,
                mime_type: artifact.mime_type,
                parts: vec![A2aPart {
                    kind: "file_ref".into(),
                    text: None,
                    data: None,
                    uri: artifact.uri,
                    mime_type: None,
                }],
            },
        },
        kind => {
            let (state, message, final_update) = match kind {
                CoreEventKind::Accepted { .. } => ("submitted", None, false),
                CoreEventKind::Progress { message, .. } => {
                    ("working", Some(text_message(message)), false)
                }
                CoreEventKind::InputRequired { request } => (
                    "input-required",
                    Some(text_message(request.question)),
                    false,
                ),
                CoreEventKind::Completed { output } => (
                    "completed",
                    Some(A2aMessage {
                        role: "agent".into(),
                        parts: output.into_iter().map(from_core_part).collect(),
                    }),
                    true,
                ),
                CoreEventKind::Failed { error } => {
                    ("failed", Some(text_message(error.message)), true)
                }
                CoreEventKind::CancelRequested { .. } => (
                    "working",
                    Some(text_message("cancellation requested".into())),
                    false,
                ),
                CoreEventKind::Cancelled => ("canceled", None, true),
                CoreEventKind::Artifact { .. } => unreachable!(),
            };
            A2aStreamEvent::TaskStatusUpdate {
                task_id,
                status: A2aTaskStatus {
                    state: state.into(),
                    message,
                },
                final_update,
            }
        }
    }
}

fn status_from_snapshot(snapshot: &adapter_core::TaskSnapshot) -> A2aTaskStatus {
    let state = match snapshot.state {
        adapter_core::TaskState::Created | adapter_core::TaskState::Accepted => "submitted",
        adapter_core::TaskState::Running | adapter_core::TaskState::CancelRequested => "working",
        adapter_core::TaskState::WaitingForInput => "input-required",
        adapter_core::TaskState::Completed => "completed",
        adapter_core::TaskState::Failed => "failed",
        adapter_core::TaskState::Cancelled => "canceled",
    };
    A2aTaskStatus {
        state: state.into(),
        message: None,
    }
}

fn from_core_part(part: Part) -> A2aPart {
    match part {
        Part::Text { text } => A2aPart {
            kind: "text".into(),
            text: Some(text),
            data: None,
            uri: None,
            mime_type: None,
        },
        Part::Json { value } => A2aPart {
            kind: "data".into(),
            text: None,
            data: Some(value),
            uri: None,
            mime_type: None,
        },
        Part::FileRef { uri, mime_type } => A2aPart {
            kind: "file_ref".into(),
            text: None,
            data: None,
            uri: Some(uri),
            mime_type,
        },
    }
}

fn text_message(text: String) -> A2aMessage {
    A2aMessage {
        role: "agent".into(),
        parts: vec![A2aPart {
            kind: "text".into(),
            text: Some(text),
            data: None,
            uri: None,
            mime_type: None,
        }],
    }
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid, A2aMapperError> {
    value
        .parse()
        .map_err(|_| A2aMapperError::InvalidRequest(format!("{field} must be UUID")))
}

fn parse_uuid_task(value: &str) -> Result<adapter_core::TaskId, A2aMapperError> {
    parse_uuid(value, "task_id")
}
