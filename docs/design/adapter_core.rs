//! Universal Agent Adapter Runtime — framework- and transport-neutral MVP core.
//!
//! Cargo.toml dependencies:
//! async-trait = "0.1"
//! chrono = { version = "0.4", features = ["serde"] }
//! dashmap = "6"
//! serde = { version = "1", features = ["derive"] }
//! serde_json = "1"
//! thiserror = "2"
//! tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync", "time"] }
//! tokio-util = "0.7"
//! uuid = { version = "1", features = ["v4", "serde"] }
//!
//! This crate intentionally contains no HTTP, SSE, ACP, A2A, Axum, gRPC, or
//! framework-specific types. Protocol modules map their wire formats to
//! CoreCommand/CoreEvent; drivers implement AgentDriver separately.

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::{broadcast, mpsc, Mutex, Semaphore}, task::JoinHandle};
use tokio_util::sync::CancellationToken;
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
    Text { text: String },
    Json { value: serde_json::Value },
    FileRef { uri: String, mime_type: Option<String> },
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
    Accepted { queued: bool },
    Progress { message: String, percent: Option<u8> },
    Artifact { artifact: ArtifactRef },
    InputRequired { request: InputRequest },
    Completed { output: Vec<Part> },
    Failed { error: PublicError },
    CancelRequested { reason: Option<String> },
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
    Cancel { task_id: TaskId, reason: Option<String> },
    ProvideInput { task_id: TaskId, input: Vec<Part> },
    GetStatus { task_id: TaskId },
}

#[derive(Clone, Debug)]
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
}

#[derive(Clone, Debug)]
pub enum DispatchResult {
    Created(TaskSnapshot),
    Existing(TaskSnapshot),
    Status(TaskSnapshot),
    CancelRequested(TaskSnapshot),
    InputAccepted(TaskSnapshot),
}

#[derive(Error, Debug)]
pub enum CoreError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("agent not found: {0}")]
    AgentNotFound(String),
    #[error("no eligible agent")]
    NoEligibleAgent,
    #[error("task not found: {0}")]
    TaskNotFound(TaskId),
    #[error("invalid task state: expected {expected}, actual {actual:?}")]
    InvalidState { expected: &'static str, actual: TaskState },
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    #[error("driver error: {0}")]
    Driver(String),
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Clone, Debug)]
pub enum DriverEvent {
    Accepted,
    Progress { message: String, percent: Option<u8> },
    Artifact(ArtifactRef),
    InputRequired(InputRequest),
    Completed(Vec<Part>),
    Failed(PublicError),
    Cancelled,
}

#[derive(Clone, Debug)]
pub struct DriverCapabilities {
    pub cancellation: bool,
    pub provide_input: bool,
}

#[async_trait]
pub trait AgentDriver: Send + Sync {
    fn id(&self) -> &str;
    fn capabilities(&self) -> DriverCapabilities;
    async fn health(&self) -> Result<(), CoreError>;
    async fn invoke(&self, task_id: TaskId, request: InvokeRequest)
        -> Result<mpsc::Receiver<DriverEvent>, CoreError>;
    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError>;
    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<(), CoreError>;
}

#[derive(Clone, Debug)]
pub struct AgentLimits {
    pub max_concurrent_tasks: usize,
}

pub struct RegisteredAgent {
    pub id: AgentId,
    pub skills: Vec<String>,
    pub driver: Arc<dyn AgentDriver>,
    pub limits: AgentLimits,
    permits: Arc<Semaphore>,
}

impl RegisteredAgent {
    pub fn new(
        id: AgentId,
        skills: Vec<String>,
        driver: Arc<dyn AgentDriver>,
        limits: AgentLimits,
    ) -> Self {
        Self {
            id,
            skills,
            driver,
            permits: Arc::new(Semaphore::new(limits.max_concurrent_tasks)),
            limits,
        }
    }
}

pub struct AgentRegistry {
    agents: DashMap<AgentId, Arc<RegisteredAgent>>,
}

impl AgentRegistry {
    pub fn new() -> Self { Self { agents: DashMap::new() } }

    pub fn register(&self, agent: RegisteredAgent) {
        self.agents.insert(agent.id.clone(), Arc::new(agent));
    }

    pub fn resolve(&self, request: &InvokeRequest) -> Result<Arc<RegisteredAgent>, CoreError> {
        if let Some(id) = &request.agent_id {
            return self.agents.get(id).map(|a| a.clone())
                .ok_or_else(|| CoreError::AgentNotFound(id.0.clone()));
        }
        if let Some(skill) = &request.skill_id {
            return self.agents.iter()
                .find(|entry| entry.skills.iter().any(|candidate| candidate == skill))
                .map(|entry| entry.value().clone())
                .ok_or(CoreError::NoEligibleAgent);
        }
        self.agents.iter().next().map(|entry| entry.value().clone())
            .ok_or(CoreError::NoEligibleAgent)
    }
}

impl Default for AgentRegistry { fn default() -> Self { Self::new() } }

struct TaskRecord {
    snapshot: TaskSnapshot,
    idempotency_key: String,
    events: Vec<CoreEvent>,
    tx: broadcast::Sender<CoreEvent>,
    cancellation: CancellationToken,
}

pub struct TaskSubscription {
    pub history: Vec<CoreEvent>,
    pub receiver: broadcast::Receiver<CoreEvent>,
}

pub struct MemoryTaskStore {
    tasks: DashMap<TaskId, Arc<Mutex<TaskRecord>>>,
    idempotency: DashMap<(CallerId, String), TaskId>,
}

impl MemoryTaskStore {
    pub fn new() -> Self {
        Self { tasks: DashMap::new(), idempotency: DashMap::new() }
    }

    async fn create_or_get(
        &self,
        caller: &Caller,
        agent_id: AgentId,
        request: &InvokeRequest,
    ) -> (bool, Arc<Mutex<TaskRecord>>) {
        let key = (caller.id.clone(), request.idempotency_key.clone());
        if let Some(existing_id) = self.idempotency.get(&key) {
            if let Some(existing) = self.tasks.get(existing_id.value()) {
                return (false, existing.clone());
            }
        }
        let task_id = Uuid::new_v4();
        let now = Utc::now();
        let (tx, _) = broadcast::channel(256);
        let record = Arc::new(Mutex::new(TaskRecord {
            snapshot: TaskSnapshot {
                task_id,
                session_id: request.session_id,
                agent_id,
                caller_id: caller.id.clone(),
                state: TaskState::Created,
                revision: 0,
                last_seq: 0,
                created_at: now,
                updated_at: now,
            },
            idempotency_key: request.idempotency_key.clone(),
            events: Vec::new(),
            tx,
            cancellation: CancellationToken::new(),
        }));
        self.tasks.insert(task_id, record.clone());
        self.idempotency.insert(key, task_id);
        (true, record)
    }

    async fn get(&self, task_id: TaskId) -> Result<Arc<Mutex<TaskRecord>>, CoreError> {
        self.tasks.get(&task_id).map(|v| v.clone()).ok_or(CoreError::TaskNotFound(task_id))
    }

    async fn snapshot(&self, task_id: TaskId) -> Result<TaskSnapshot, CoreError> {
        Ok(self.get(task_id).await?.lock().await.snapshot.clone())
    }

    async fn subscribe(&self, task_id: TaskId, after_seq: EventSeq) -> Result<TaskSubscription, CoreError> {
        let record = self.get(task_id).await?;
        let record = record.lock().await;
        let history = record.events.iter().filter(|event| event.seq > after_seq).cloned().collect();
        Ok(TaskSubscription { history, receiver: record.tx.subscribe() })
    }

    async fn cancellation(&self, task_id: TaskId) -> Result<CancellationToken, CoreError> {
        Ok(self.get(task_id).await?.lock().await.cancellation.clone())
    }

    async fn transition(
        &self,
        task_id: TaskId,
        event_kind: CoreEventKind,
        allowed: &[TaskState],
        next: TaskState,
    ) -> Result<(CoreEvent, TaskSnapshot), CoreError> {
        let record = self.get(task_id).await?;
        let mut record = record.lock().await;
        let current = record.snapshot.state;
        if !allowed.contains(&current) {
            return Err(CoreError::InvalidState { expected: "allowed transition", actual: current });
        }
        record.snapshot.state = next;
        record.snapshot.revision += 1;
        record.snapshot.last_seq += 1;
        record.snapshot.updated_at = Utc::now();
        let event = CoreEvent {
            task_id,
            seq: record.snapshot.last_seq,
            at: record.snapshot.updated_at,
            kind: event_kind,
        };
        record.events.push(event.clone());
        let snapshot = record.snapshot.clone();
        let _ = record.tx.send(event.clone());
        Ok((event, snapshot))
    }
}

impl Default for MemoryTaskStore { fn default() -> Self { Self::new() } }

#[async_trait]
pub trait PolicyEngine: Send + Sync {
    async fn authorize(&self, caller: &Caller, command: &CoreCommand) -> Result<(), CoreError>;
}

pub struct AllowAllPolicy;

#[async_trait]
impl PolicyEngine for AllowAllPolicy {
    async fn authorize(&self, _caller: &Caller, _command: &CoreCommand) -> Result<(), CoreError> { Ok(()) }
}

pub struct AdapterCore {
    registry: Arc<AgentRegistry>,
    store: Arc<MemoryTaskStore>,
    policy: Arc<dyn PolicyEngine>,
    global_permits: Arc<Semaphore>,
    workers: DashMap<TaskId, JoinHandle<()>>,
}

impl AdapterCore {
    pub fn new(
        registry: Arc<AgentRegistry>,
        store: Arc<MemoryTaskStore>,
        policy: Arc<dyn PolicyEngine>,
        max_concurrent_tasks: usize,
    ) -> Self {
        Self {
            registry,
            store,
            policy,
            global_permits: Arc::new(Semaphore::new(max_concurrent_tasks)),
            workers: DashMap::new(),
        }
    }

    pub async fn dispatch(&self, caller: Caller, command: CoreCommand) -> Result<DispatchResult, CoreError> {
        self.policy.authorize(&caller, &command).await?;
        match command {
            CoreCommand::Invoke(request) => self.invoke(caller, request).await,
            CoreCommand::Cancel { task_id, reason } => self.cancel(task_id, reason).await,
            CoreCommand::ProvideInput { task_id, input } => self.provide_input(task_id, input).await,
            CoreCommand::GetStatus { task_id } => Ok(DispatchResult::Status(self.store.snapshot(task_id).await?)),
        }
    }

    pub async fn subscribe(&self, task_id: TaskId, after_seq: EventSeq) -> Result<TaskSubscription, CoreError> {
        self.store.subscribe(task_id, after_seq).await
    }

    async fn invoke(&self, caller: Caller, request: InvokeRequest) -> Result<DispatchResult, CoreError> {
        if request.idempotency_key.trim().is_empty() {
            return Err(CoreError::InvalidRequest("idempotency_key is required".into()));
        }
        let agent = self.registry.resolve(&request)?;
        let (created, record) = self.store.create_or_get(&caller, agent.id.clone(), &request).await;
        let task_id = record.lock().await.snapshot.task_id;
        if !created {
            return Ok(DispatchResult::Existing(self.store.snapshot(task_id).await?));
        }

        let (_, accepted) = self.store.transition(
            task_id,
            CoreEventKind::Accepted { queued: false },
            &[TaskState::Created],
            TaskState::Accepted,
        ).await?;

        let global = self.global_permits.clone().try_acquire_owned()
            .map_err(|_| CoreError::ResourceExhausted("global task limit reached".into()))?;
        let per_agent = agent.permits.clone().try_acquire_owned()
            .map_err(|_| CoreError::ResourceExhausted(format!("agent {} is busy", agent.id.0)))?;

        let store = self.store.clone();
        let worker_agent = agent.clone();
        let worker_request = request.clone();
        let worker = tokio::spawn(async move {
            let _global = global;
            let _per_agent = per_agent;
            if let Err(error) = run_task(store.clone(), worker_agent, task_id, worker_request).await {
                let _ = fail_if_active(store, task_id, error).await;
            }
        });
        self.workers.insert(task_id, worker);
        Ok(DispatchResult::Created(accepted))
    }

    async fn cancel(&self, task_id: TaskId, reason: Option<String>) -> Result<DispatchResult, CoreError> {
        let snapshot = self.store.snapshot(task_id).await?;
        if snapshot.state.terminal() {
            return Ok(DispatchResult::CancelRequested(snapshot));
        }
        let agent = self.registry.agents.get(&snapshot.agent_id)
            .map(|a| a.clone()).ok_or_else(|| CoreError::AgentNotFound(snapshot.agent_id.0.clone()))?;
        let (_, snapshot) = self.store.transition(
            task_id,
            CoreEventKind::CancelRequested { reason },
            &[TaskState::Created, TaskState::Accepted, TaskState::Running, TaskState::WaitingForInput],
            TaskState::CancelRequested,
        ).await?;
        self.store.cancellation(task_id).await?.cancel();
        if agent.driver.capabilities().cancellation {
            let _ = agent.driver.cancel(task_id).await;
        }
        Ok(DispatchResult::CancelRequested(snapshot))
    }

    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<DispatchResult, CoreError> {
        let snapshot = self.store.snapshot(task_id).await?;
        if snapshot.state != TaskState::WaitingForInput {
            return Err(CoreError::InvalidState { expected: "WaitingForInput", actual: snapshot.state });
        }
        let agent = self.registry.agents.get(&snapshot.agent_id)
            .map(|a| a.clone()).ok_or_else(|| CoreError::AgentNotFound(snapshot.agent_id.0.clone()))?;
        if !agent.driver.capabilities().provide_input {
            return Err(CoreError::InvalidRequest("agent does not support provide_input".into()));
        }
        agent.driver.provide_input(task_id, input).await?;
        let (_, snapshot) = self.store.transition(
            task_id,
            CoreEventKind::Progress { message: "input accepted".into(), percent: None },
            &[TaskState::WaitingForInput],
            TaskState::Running,
        ).await?;
        Ok(DispatchResult::InputAccepted(snapshot))
    }
}

async fn run_task(
    store: Arc<MemoryTaskStore>,
    agent: Arc<RegisteredAgent>,
    task_id: TaskId,
    request: InvokeRequest,
) -> Result<(), CoreError> {
    let mut stream = agent.driver.invoke(task_id, request).await?;
    while let Some(event) = stream.recv().await {
        apply_driver_event(store.clone(), task_id, event).await?;
        if store.snapshot(task_id).await?.state.terminal() { break; }
    }
    let state = store.snapshot(task_id).await?.state;
    if !state.terminal() {
        return Err(CoreError::Driver("driver stream closed before terminal event".into()));
    }
    Ok(())
}

async fn apply_driver_event(store: Arc<MemoryTaskStore>, task_id: TaskId, event: DriverEvent) -> Result<(), CoreError> {
    match event {
        DriverEvent::Accepted => { /* core already emitted accepted */ Ok(()) }
        DriverEvent::Progress { message, percent } => {
            let state = store.snapshot(task_id).await?.state;
            let next = if state == TaskState::Accepted { TaskState::Running } else { state };
            store.transition(task_id, CoreEventKind::Progress { message, percent }, &[TaskState::Accepted, TaskState::Running], next).await?;
            Ok(())
        }
        DriverEvent::Artifact(artifact) => {
            store.transition(task_id, CoreEventKind::Artifact { artifact }, &[TaskState::Accepted, TaskState::Running, TaskState::WaitingForInput], TaskState::Running).await?;
            Ok(())
        }
        DriverEvent::InputRequired(request) => {
            store.transition(task_id, CoreEventKind::InputRequired { request }, &[TaskState::Accepted, TaskState::Running], TaskState::WaitingForInput).await?;
            Ok(())
        }
        DriverEvent::Completed(output) => {
            store.transition(task_id, CoreEventKind::Completed { output }, &[TaskState::Accepted, TaskState::Running, TaskState::WaitingForInput], TaskState::Completed).await?;
            Ok(())
        }
        DriverEvent::Failed(error) => fail_if_active(store, task_id, CoreError::Driver(error.message)).await,
        DriverEvent::Cancelled => {
            store.transition(task_id, CoreEventKind::Cancelled, &[TaskState::CancelRequested, TaskState::Accepted, TaskState::Running, TaskState::WaitingForInput], TaskState::Cancelled).await?;
            Ok(())
        }
    }
}

async fn fail_if_active(store: Arc<MemoryTaskStore>, task_id: TaskId, error: CoreError) -> Result<(), CoreError> {
    let state = store.snapshot(task_id).await?.state;
    if state.terminal() { return Ok(()); }
    store.transition(
        task_id,
        CoreEventKind::Failed { error: PublicError { code: "driver_error".into(), message: error.to_string(), retryable: false } },
        &[TaskState::Created, TaskState::Accepted, TaskState::Running, TaskState::WaitingForInput, TaskState::CancelRequested],
        TaskState::Failed,
    ).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeDriver;

    #[async_trait]
    impl AgentDriver for FakeDriver {
        fn id(&self) -> &str { "fake" }
        fn capabilities(&self) -> DriverCapabilities {
            DriverCapabilities { cancellation: true, provide_input: true }
        }
        async fn health(&self) -> Result<(), CoreError> { Ok(()) }
        async fn invoke(&self, _task_id: TaskId, _request: InvokeRequest) -> Result<mpsc::Receiver<DriverEvent>, CoreError> {
            let (tx, rx) = mpsc::channel(8);
            tokio::spawn(async move {
                let _ = tx.send(DriverEvent::Progress { message: "working".into(), percent: Some(50) }).await;
                let _ = tx.send(DriverEvent::Completed(vec![Part::Text { text: "done".into() }])).await;
            });
            Ok(rx)
        }
        async fn cancel(&self, _task_id: TaskId) -> Result<(), CoreError> { Ok(()) }
        async fn provide_input(&self, _task_id: TaskId, _input: Vec<Part>) -> Result<(), CoreError> { Ok(()) }
    }

    fn core() -> AdapterCore {
        let registry = Arc::new(AgentRegistry::new());
        registry.register(RegisteredAgent::new(
            AgentId("reviewer".into()),
            vec!["code-review".into()],
            Arc::new(FakeDriver),
            AgentLimits { max_concurrent_tasks: 2 },
        ));
        AdapterCore::new(registry, Arc::new(MemoryTaskStore::new()), Arc::new(AllowAllPolicy), 8)
    }

    fn request(key: &str) -> InvokeRequest {
        InvokeRequest {
            agent_id: None,
            skill_id: Some("code-review".into()),
            idempotency_key: key.into(),
            session_id: None,
            input: vec![Part::Text { text: "review".into() }],
            context: serde_json::json!({}),
            deadline: None,
        }
    }

    #[tokio::test]
    async fn invokes_and_completes() {
        let core = core();
        let caller = Caller { id: CallerId("alice".into()), scopes: vec![] };
        let created = core.dispatch(caller, CoreCommand::Invoke(request("k1"))).await.unwrap();
        let task_id = match created { DispatchResult::Created(s) => s.task_id, _ => panic!("expected created") };
        tokio::time::sleep(Duration::from_millis(20)).await;
        let status = core.dispatch(Caller { id: CallerId("alice".into()), scopes: vec![] }, CoreCommand::GetStatus { task_id }).await.unwrap();
        assert!(matches!(status, DispatchResult::Status(TaskSnapshot { state: TaskState::Completed, .. })));
    }

    #[tokio::test]
    async fn idempotency_returns_same_task() {
        let core = core();
        let caller = Caller { id: CallerId("alice".into()), scopes: vec![] };
        let first = core.dispatch(caller.clone(), CoreCommand::Invoke(request("same"))).await.unwrap();
        let second = core.dispatch(caller, CoreCommand::Invoke(request("same"))).await.unwrap();
        let id1 = match first { DispatchResult::Created(s) => s.task_id, _ => panic!() };
        let id2 = match second { DispatchResult::Existing(s) => s.task_id, _ => panic!() };
        assert_eq!(id1, id2);
    }
}
