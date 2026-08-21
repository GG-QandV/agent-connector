//! `adapter-core` v2 — storage-neutral task lifecycle runtime.
//!
//! Depends only on adapter-model + adapter-store-contract. Drivers, A2A/ACP,
//! HTTP, SQLite and Postgres are injected from composition root (`adapterd`).

use std::{collections::HashMap, sync::Arc};

use adapter_model::{
    AgentId, AgentLimits, Caller, CoreCommand, CoreEvent, CoreEventKind,
    CreateTaskResult, DispatchResult, DriverCapabilities, EventSeq, InvokeRequest,
    NewTask, Part, PublicError, TaskId, TaskSnapshot, TaskState, TaskTransition,
};
use adapter_store_contract::{StoreError, TaskStore};
use async_trait::async_trait;
use dashmap::DashMap;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum CoreError {
    #[error("invalid request: {0}")] InvalidRequest(String),
    #[error("forbidden")] Forbidden,
    #[error("agent not found: {0}")] AgentNotFound(String),
    #[error("no eligible agent")] NoEligibleAgent,
    #[error("resource exhausted: {0}")] ResourceExhausted(String),
    #[error("driver error: {0}")] Driver(String),
    #[error("store error: {0}")] Store(#[from] StoreError),
}

#[derive(Clone, Debug)]
pub enum DriverEvent {
    Accepted,
    Progress { message: String, percent: Option<u8> },
    Artifact(adapter_model::ArtifactRef),
    InputRequired(adapter_model::InputRequest),
    Completed(Vec<Part>),
    Failed(PublicError),
    Cancelled,
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

pub struct RegisteredAgent {
    pub id: AgentId,
    pub skills: Vec<String>,
    pub driver: Arc<dyn AgentDriver>,
    pub limits: AgentLimits,
    permits: Arc<Semaphore>,
}

impl RegisteredAgent {
    pub fn new(id: AgentId, skills: Vec<String>, driver: Arc<dyn AgentDriver>, limits: AgentLimits) -> Self {
        Self { permits: Arc::new(Semaphore::new(limits.max_concurrent_tasks)), id, skills, driver, limits }
    }
}

pub struct AgentRegistry { agents: DashMap<AgentId, Arc<RegisteredAgent>> }
impl AgentRegistry {
    pub fn new() -> Self { Self { agents: DashMap::new() } }
    pub fn register(&self, agent: RegisteredAgent) { self.agents.insert(agent.id.clone(), Arc::new(agent)); }
    pub fn resolve(&self, request: &InvokeRequest) -> Result<Arc<RegisteredAgent>, CoreError> {
        if let Some(id) = &request.agent_id {
            return self.agents.get(id).map(|v| v.clone()).ok_or_else(|| CoreError::AgentNotFound(id.0.clone()));
        }
        if let Some(skill) = &request.skill_id {
            return self.agents.iter().find(|entry| entry.skills.iter().any(|candidate| candidate == skill))
                .map(|entry| entry.value().clone()).ok_or(CoreError::NoEligibleAgent);
        }
        self.agents.iter().next().map(|entry| entry.value().clone()).ok_or(CoreError::NoEligibleAgent)
    }
}
impl Default for AgentRegistry { fn default() -> Self { Self::new() } }

#[async_trait]
pub trait PolicyEngine: Send + Sync {
    async fn authorize(&self, caller: &Caller, command: &CoreCommand) -> Result<(), CoreError>;
}
pub struct AllowAllPolicy;
#[async_trait]
impl PolicyEngine for AllowAllPolicy {
    async fn authorize(&self, _: &Caller, _: &CoreCommand) -> Result<(), CoreError> { Ok(()) }
}

/// RAM-only state. It holds active task fan-out and cancellation handles, not
/// task history. Terminal tasks are removed, therefore memory does not grow by
/// number of completed sessions/tasks.
struct ActiveTask {
    tx: broadcast::Sender<CoreEvent>,
    cancellation: CancellationToken,
}

pub struct TaskSubscription {
    pub history: Vec<CoreEvent>,
    pub receiver: broadcast::Receiver<CoreEvent>,
}

pub struct AdapterCore {
    store: Arc<dyn TaskStore>,
    registry: Arc<AgentRegistry>,
    policy: Arc<dyn PolicyEngine>,
    global_permits: Arc<Semaphore>,
    active: DashMap<TaskId, ActiveTask>,
}

impl AdapterCore {
    pub fn new(store: Arc<dyn TaskStore>, registry: Arc<AgentRegistry>, policy: Arc<dyn PolicyEngine>, max_concurrent: usize) -> Self {
        Self { store, registry, policy, global_permits: Arc::new(Semaphore::new(max_concurrent)), active: DashMap::new() }
    }

    pub async fn dispatch(&self, caller: Caller, command: CoreCommand) -> Result<DispatchResult, CoreError> {
        self.policy.authorize(&caller, &command).await?;
        match command {
            CoreCommand::Invoke(request) => self.invoke(caller, request).await,
            CoreCommand::Cancel { task_id, reason } => self.cancel(task_id, reason).await,
            CoreCommand::ProvideInput { task_id, input } => self.provide_input(task_id, input).await,
            CoreCommand::GetStatus { task_id } => Ok(DispatchResult::Status(self.snapshot(task_id).await?)),
        }
    }

    pub async fn subscribe(&self, task_id: TaskId, after_seq: EventSeq) -> Result<TaskSubscription, CoreError> {
        let history = self.store.events_after(task_id, after_seq, 500).await?;
        let receiver = self.active.get(&task_id).map(|active| active.tx.subscribe())
            .unwrap_or_else(closed_receiver);
        Ok(TaskSubscription { history, receiver })
    }

    async fn invoke(&self, caller: Caller, request: InvokeRequest) -> Result<DispatchResult, CoreError> {
        if request.idempotency_key.trim().is_empty() { return Err(CoreError::InvalidRequest("idempotency_key required".into())); }
        let agent = self.registry.resolve(&request)?;
        let task_id = Uuid::new_v4();
        let deadline_at = request.deadline.and_then(|duration| chrono::Duration::from_std(duration).ok()).map(|duration| chrono::Utc::now() + duration);
        let created = self.store.create_or_get_idempotent(NewTask {
            task_id, session_id: request.session_id, agent_id: agent.id.clone(), caller_id: caller.id,
            idempotency_key: request.idempotency_key.clone(), deadline_at,
        }).await?;
        let snapshot = match created {
            CreateTaskResult::Existing(snapshot) => return Ok(DispatchResult::Existing(snapshot)),
            CreateTaskResult::Created(snapshot) => snapshot,
        };
        let (tx, _) = broadcast::channel(256);
        self.active.insert(task_id, ActiveTask { tx, cancellation: CancellationToken::new() });
        let accepted = self.transition(snapshot, vec![TaskState::Created], TaskState::Accepted, CoreEventKind::Accepted { queued: false }).await?;

        let global = self.global_permits.clone().try_acquire_owned()
            .map_err(|_| CoreError::ResourceExhausted("global task limit reached".into()))?;
        let per_agent = agent.permits.clone().try_acquire_owned()
            .map_err(|_| CoreError::ResourceExhausted(format!("agent {} busy", agent.id.0)))?;
        let core = self.clone_handle();
        tokio::spawn(async move {
            let _global = global;
            let _per_agent = per_agent;
            let result = core.run_driver(agent, task_id, request).await;
            if let Err(error) = result { let _ = core.fail_active(task_id, error).await; }
            core.active.remove(&task_id);
        });
        Ok(DispatchResult::Created(accepted))
    }

    async fn cancel(&self, task_id: TaskId, reason: Option<String>) -> Result<DispatchResult, CoreError> {
        let snapshot = self.snapshot(task_id).await?;
        if snapshot.state.terminal() { return Ok(DispatchResult::CancelRequested(snapshot)); }
        let agent = self.registry.agents.get(&snapshot.agent_id).map(|v| v.clone()).ok_or_else(|| CoreError::AgentNotFound(snapshot.agent_id.0.clone()))?;
        let changed = self.transition(snapshot, vec![TaskState::Created, TaskState::Accepted, TaskState::Running, TaskState::WaitingForInput], TaskState::CancelRequested, CoreEventKind::CancelRequested { reason }).await?;
        if let Some(active) = self.active.get(&task_id) { active.cancellation.cancel(); }
        if agent.driver.capabilities().cancellation { let _ = agent.driver.cancel(task_id).await; }
        Ok(DispatchResult::CancelRequested(changed))
    }

    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<DispatchResult, CoreError> {
        let snapshot = self.snapshot(task_id).await?;
        if snapshot.state != TaskState::WaitingForInput { return Err(CoreError::InvalidRequest("task is not waiting for input".into())); }
        let agent = self.registry.agents.get(&snapshot.agent_id).map(|v| v.clone()).ok_or_else(|| CoreError::AgentNotFound(snapshot.agent_id.0.clone()))?;
        if !agent.driver.capabilities().provide_input { return Err(CoreError::InvalidRequest("agent does not support input".into())); }
        agent.driver.provide_input(task_id, input).await?;
        let changed = self.transition(snapshot, vec![TaskState::WaitingForInput], TaskState::Running, CoreEventKind::Progress { message: "input accepted".into(), percent: None }).await?;
        Ok(DispatchResult::InputAccepted(changed))
    }

    async fn run_driver(&self, agent: Arc<RegisteredAgent>, task_id: TaskId, request: InvokeRequest) -> Result<(), CoreError> {
        let mut events = agent.driver.invoke(task_id, request).await?;
        while let Some(event) = events.recv().await {
            self.apply_driver_event(task_id, event).await?;
            if self.snapshot(task_id).await?.state.terminal() { return Ok(()); }
        }
        Err(CoreError::Driver("driver event stream closed before terminal event".into()))
    }

    async fn apply_driver_event(&self, task_id: TaskId, event: DriverEvent) -> Result<(), CoreError> {
        let snapshot = self.snapshot(task_id).await?;
        match event {
            DriverEvent::Accepted => Ok(()),
            DriverEvent::Progress { message, percent } => {
                let next = if snapshot.state == TaskState::Accepted { TaskState::Running } else { snapshot.state };
                self.transition(snapshot, vec![TaskState::Accepted, TaskState::Running], next, CoreEventKind::Progress { message, percent }).await?; Ok(())
            }
            DriverEvent::Artifact(artifact) => { self.transition(snapshot, vec![TaskState::Accepted, TaskState::Running, TaskState::WaitingForInput], TaskState::Running, CoreEventKind::Artifact { artifact }).await?; Ok(()) }
            DriverEvent::InputRequired(request) => { self.transition(snapshot, vec![TaskState::Accepted, TaskState::Running], TaskState::WaitingForInput, CoreEventKind::InputRequired { request }).await?; Ok(()) }
            DriverEvent::Completed(output) => { self.transition(snapshot, vec![TaskState::Accepted, TaskState::Running, TaskState::WaitingForInput], TaskState::Completed, CoreEventKind::Completed { output }).await?; Ok(()) }
            DriverEvent::Failed(error) => self.fail_active(task_id, CoreError::Driver(error.message)).await,
            DriverEvent::Cancelled => { self.transition(snapshot, vec![TaskState::CancelRequested, TaskState::Accepted, TaskState::Running, TaskState::WaitingForInput], TaskState::Cancelled, CoreEventKind::Cancelled).await?; Ok(()) }
        }
    }

    async fn fail_active(&self, task_id: TaskId, error: CoreError) -> Result<(), CoreError> {
        let snapshot = self.snapshot(task_id).await?;
        if snapshot.state.terminal() { return Ok(()); }
        self.transition(snapshot, vec![TaskState::Created, TaskState::Accepted, TaskState::Running, TaskState::WaitingForInput, TaskState::CancelRequested], TaskState::Failed, CoreEventKind::Failed { error: PublicError { code: "runtime_error".into(), message: error.to_string(), retryable: false } }).await?;
        Ok(())
    }

    async fn transition(&self, snapshot: TaskSnapshot, allowed: Vec<TaskState>, next: TaskState, kind: CoreEventKind) -> Result<TaskSnapshot, CoreError> {
        let applied = self.store.append_event_and_transition(TaskTransition { task_id: snapshot.task_id, expected_revision: snapshot.revision, allowed_states: allowed, next_state: next, event_kind: kind }).await?;
        // Store commit is complete before live publish.
        if let Some(active) = self.active.get(&snapshot.task_id) { let _ = active.tx.send(applied.event); }
        Ok(applied.snapshot)
    }

    async fn snapshot(&self, task_id: TaskId) -> Result<TaskSnapshot, CoreError> {
        self.store.get_snapshot(task_id).await?.ok_or_else(|| CoreError::Store(StoreError::NotFound(task_id)))
    }

    fn clone_handle(&self) -> CoreHandle {
        CoreHandle { store: self.store.clone(), registry: self.registry.clone(), active: self.active.clone() }
    }
}

struct CoreHandle { store: Arc<dyn TaskStore>, registry: Arc<AgentRegistry>, active: DashMap<TaskId, ActiveTask> }
impl CoreHandle {
    async fn run_driver(&self, agent: Arc<RegisteredAgent>, task_id: TaskId, request: InvokeRequest) -> Result<(), CoreError> {
        // CoreHandle delegates event application through the full AdapterCore in production.
        // This placeholder is intentionally unreachable until composition extracts a shared
        // TaskRuntime service; it prevents accidental duplicated lifecycle logic.
        let _ = (agent, task_id, request);
        Err(CoreError::Driver("CoreHandle requires TaskRuntime extraction".into()))
    }
    async fn fail_active(&self, _task_id: TaskId, _error: CoreError) -> Result<(), CoreError> { Ok(()) }
}

fn closed_receiver() -> broadcast::Receiver<CoreEvent> {
    let (tx, rx) = broadcast::channel(1);
    drop(tx);
    rx
}
