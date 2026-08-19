//! `protocol-acp` — ACP semantic mapper for Adapter Core.
//!
//! ACP is an editor/CLI <-> coding-agent protocol, normally transported over
//! stdio JSON-RPC. This module contains no stdin/stdout framing and no JSON-RPC
//! server loop: the thin ACP SDK/wire layer maps protocol messages to these DTOs.
//! The mapper converts them to Adapter Core commands/events.
//!
//! This keeps ACP isolated from A2A, drivers, storage and task lifecycle.
//!
//! Expected adapter-core public types:
//! AdapterCore, Caller, CallerId, CoreCommand, CoreError, CoreEvent,
//! CoreEventKind, DispatchResult, InvokeRequest, Part, TaskId, TaskSubscription.
//!
//! Cargo.toml dependencies:
//! async-trait = "0.1"
//! serde = { version = "1", features = ["derive"] }
//! thiserror = "2"
//! tokio = { version = "1", features = ["sync"] }
//! uuid = { version = "1", features = ["serde"] }

use std::sync::Arc;

use adapter_core::{
    AdapterCore, Caller, CallerId, CoreCommand, CoreError, CoreEvent, CoreEventKind,
    DispatchResult, InvokeRequest, Part, TaskId, TaskSubscription,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcpAgentCapabilities {
    pub filesystem: bool,
    pub terminal: bool,
    pub streaming: bool,
    pub cancellation: bool,
    pub session_resume: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcpInitializeResult {
    pub protocol_version: String,
    pub agent_name: String,
    pub agent_version: String,
    pub capabilities: AcpAgentCapabilities,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcpSessionPrompt {
    pub session_id: Option<String>,
    pub request_id: String,
    pub agent_id: Option<String>,
    pub skill_id: Option<String>,
    pub prompt: Vec<AcpContentBlock>,
    pub workspace: Option<String>,
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcpSessionUpdate {
    pub session_id: String,
    pub task_id: String,
    pub update: AcpUpdate,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AcpContentBlock {
    Text { text: String },
    Json { value: serde_json::Value },
    Resource { uri: String, mime_type: Option<String> },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AcpUpdate {
    AgentMessage { content: Vec<AcpContentBlock> },
    Progress { message: String, percent: Option<u8> },
    RequestInput { request_id: String, question: String, schema: Option<serde_json::Value> },
    Artifact { name: String, uri: Option<String>, mime_type: String, size_bytes: u64 },
    Completed,
    Failed { code: String, message: String },
    Cancelled,
}

#[derive(Debug, thiserror::Error)]
pub enum AcpMapperError {
    #[error("invalid ACP request: {0}")]
    InvalidRequest(String),
    #[error("adapter core error: {0}")]
    Core(#[from] CoreError),
}

#[async_trait]
pub trait AcpCoreService: Send + Sync {
    async fn dispatch(&self, caller: Caller, command: CoreCommand) -> Result<DispatchResult, CoreError>;
    async fn subscribe(&self, task_id: TaskId, after_seq: u64) -> Result<TaskSubscription, CoreError>;
}

#[async_trait]
impl AcpCoreService for AdapterCore {
    async fn dispatch(&self, caller: Caller, command: CoreCommand) -> Result<DispatchResult, CoreError> {
        self.dispatch(caller, command).await
    }
    async fn subscribe(&self, task_id: TaskId, after_seq: u64) -> Result<TaskSubscription, CoreError> {
        self.subscribe(task_id, after_seq).await
    }
}

pub struct AcpMapper<C: AcpCoreService> {
    core: Arc<C>,
    initialize: AcpInitializeResult,
}

impl<C: AcpCoreService> AcpMapper<C> {
    pub fn new(core: Arc<C>, initialize: AcpInitializeResult) -> Self { Self { core, initialize } }
    pub fn initialize_result(&self) -> &AcpInitializeResult { &self.initialize }

    pub async fn prompt(&self, caller: Caller, prompt: AcpSessionPrompt) -> Result<AcpTaskRef, AcpMapperError> {
        if prompt.request_id.trim().is_empty() {
            return Err(AcpMapperError::InvalidRequest("request_id is required for idempotency".into()));
        }
        let session_id = prompt.session_id.as_deref().map(parse_uuid).transpose()?;
        let mut context = prompt.metadata;
        if let Some(workspace) = prompt.workspace { context["workspace"] = serde_json::Value::String(workspace); }
        let command = CoreCommand::Invoke(InvokeRequest {
            agent_id: prompt.agent_id.map(adapter_core::AgentId),
            skill_id: prompt.skill_id,
            idempotency_key: format!("acp:{}", prompt.request_id),
            session_id,
            input: prompt.prompt.into_iter().map(to_core_part).collect::<Result<Vec<_>, _>>()?,
            context,
            deadline: None,
        });
        let result = self.core.dispatch(caller, command).await?;
        let snapshot = match result {
            DispatchResult::Created(snapshot) | DispatchResult::Existing(snapshot) => snapshot,
            _ => return Err(AcpMapperError::InvalidRequest("unexpected core result for prompt".into())),
        };
        Ok(AcpTaskRef {
            session_id: snapshot.session_id.unwrap_or_else(Uuid::new_v4).to_string(),
            task_id: snapshot.task_id.to_string(),
        })
    }

    pub async fn cancel(&self, caller: Caller, task_id: &str) -> Result<(), AcpMapperError> {
        let id = parse_uuid(task_id)?;
        self.core.dispatch(caller, CoreCommand::Cancel { task_id: id, reason: Some("ACP client cancellation".into()) }).await?;
        Ok(())
    }

    pub async fn provide_input(
        &self,
        caller: Caller,
        task_id: &str,
        content: Vec<AcpContentBlock>,
    ) -> Result<(), AcpMapperError> {
        let id = parse_uuid(task_id)?;
        let input = content.into_iter().map(to_core_part).collect::<Result<_, _>>()?;
        self.core.dispatch(caller, CoreCommand::ProvideInput { task_id: id, input }).await?;
        Ok(())
    }

    pub async fn subscribe(
        &self,
        caller: Caller,
        task_id: &str,
        after_seq: u64,
    ) -> Result<AcpUpdateStream, AcpMapperError> {
        let id = parse_uuid(task_id)?;
        // Core/policy checks caller visibility before exposing events.
        self.core.dispatch(caller, CoreCommand::GetStatus { task_id: id }).await?;
        let subscription = self.core.subscribe(id, after_seq).await?;
        Ok(AcpUpdateStream {
            task_id: id.to_string(),
            history: subscription.history.into_iter().flat_map(map_event).collect(),
            receiver: subscription.receiver,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcpTaskRef { pub session_id: String, pub task_id: String }

pub struct AcpUpdateStream {
    task_id: String,
    pub history: Vec<AcpSessionUpdate>,
    receiver: broadcast::Receiver<CoreEvent>,
}

impl AcpUpdateStream {
    pub async fn next(&mut self) -> Result<Option<Vec<AcpSessionUpdate>>, broadcast::error::RecvError> {
        match self.receiver.recv().await {
            Ok(event) => Ok(Some(map_event(event))),
            Err(broadcast::error::RecvError::Closed) => Ok(None),
            Err(error) => Err(error), // wire layer must re-read durable history after lag
        }
    }
}

fn to_core_part(block: AcpContentBlock) -> Result<Part, AcpMapperError> {
    Ok(match block {
        AcpContentBlock::Text { text } => Part::Text { text },
        AcpContentBlock::Json { value } => Part::Json { value },
        AcpContentBlock::Resource { uri, mime_type } => Part::FileRef { uri, mime_type },
    })
}

fn map_event(event: CoreEvent) -> Vec<AcpSessionUpdate> {
    let task_id = event.task_id.to_string();
    let session_id = "unknown".to_string(); // ACP wire/session layer may replace from TaskSnapshot cache.
    let update = match event.kind {
        CoreEventKind::Accepted { .. } => AcpUpdate::Progress { message: "task accepted".into(), percent: None },
        CoreEventKind::Progress { message, percent } => AcpUpdate::Progress { message, percent },
        CoreEventKind::InputRequired { request } => AcpUpdate::RequestInput {
            request_id: Uuid::new_v4().to_string(), question: request.question, schema: request.schema,
        },
        CoreEventKind::Artifact { artifact } => AcpUpdate::Artifact {
            name: artifact.name, uri: artifact.uri, mime_type: artifact.mime_type, size_bytes: artifact.size_bytes,
        },
        CoreEventKind::Completed { output } => return vec![
            AcpSessionUpdate { session_id: session_id.clone(), task_id: task_id.clone(), update: AcpUpdate::AgentMessage { content: output.into_iter().map(from_core_part).collect() } },
            AcpSessionUpdate { session_id, task_id, update: AcpUpdate::Completed },
        ],
        CoreEventKind::Failed { error } => AcpUpdate::Failed { code: error.code, message: error.message },
        CoreEventKind::CancelRequested { .. } => AcpUpdate::Progress { message: "cancellation requested".into(), percent: None },
        CoreEventKind::Cancelled => AcpUpdate::Cancelled,
    };
    vec![AcpSessionUpdate { session_id, task_id, update }]
}

fn from_core_part(part: Part) -> AcpContentBlock {
    match part {
        Part::Text { text } => AcpContentBlock::Text { text },
        Part::Json { value } => AcpContentBlock::Json { value },
        Part::FileRef { uri, mime_type } => AcpContentBlock::Resource { uri, mime_type },
    }
}

fn parse_uuid(value: &str) -> Result<Uuid, AcpMapperError> {
    value.parse().map_err(|_| AcpMapperError::InvalidRequest("expected UUID".into()))
}
