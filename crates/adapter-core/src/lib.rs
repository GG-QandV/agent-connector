//! Replacement for `adapter_core_v2.rs`.
//! This version uses Arc<CoreInner>, so worker tasks own a clone of AdapterCore
//! and execute the real lifecycle methods. Do not use the previous v2 draft.

pub use adapter_model::{
    AgentId, AgentLimits, ArtifactRef, Caller, CallerId, CoreCommand, CoreEvent, CoreEventKind,
    CreateTaskResult, DispatchResult, DriverCapabilities, EventSeq, InputRequest, InvokeRequest,
    NewTask, Part, PublicError, TaskId, TaskSnapshot, TaskState, TaskTransition,
};
use adapter_store_contract::{StoreError, TaskStore};
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod bearer_token;
pub use bearer_token::{BearerTokenPolicy, BearerTokenPolicyError, TokenGrant};

#[derive(Error, Debug)]
pub enum CoreError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("agent not found: {0}")]
    AgentNotFound(String),
    #[error("no eligible agent")]
    NoEligibleAgent,
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    #[error("driver error: {0}")]
    Driver(String),
    #[error("task timed out")]
    Timeout,
    #[error("store error: {0}")]
    Store(#[from] StoreError),
}

#[derive(Clone, Debug)]
pub enum DriverEvent {
    Accepted,
    Progress {
        message: String,
        percent: Option<u8>,
    },
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
    async fn invoke(
        &self,
        task_id: TaskId,
        request: InvokeRequest,
    ) -> Result<mpsc::Receiver<DriverEvent>, CoreError>;
    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError>;
    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<(), CoreError>;
}

pub struct RegisteredAgent {
    pub id: AgentId,
    /// Список skills, доступных через этот агент. Хранится в `std::sync::RwLock`
    /// (не tokio) намеренно: `AgentCardProducer::card()` — синхронный метод
    /// чужого SDK trait, он должен читать skills без `.await`. `std::sync::RwLock`
    /// даёт синхронный `read()`, запись — короткое присваивание нового Vec
    /// (микросекунды), блокировки thread здесь приемлемы.
    skills: std::sync::RwLock<Vec<String>>,
    pub driver: Arc<dyn AgentDriver>,
    pub limits: AgentLimits,
    permits: Arc<Semaphore>,
    queue_permits: Arc<Semaphore>,
}
impl RegisteredAgent {
    pub fn new(
        id: AgentId,
        skills: Vec<String>,
        driver: Arc<dyn AgentDriver>,
        limits: AgentLimits,
    ) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limits.max_concurrent_tasks)),
            queue_permits: Arc::new(Semaphore::new(limits.max_queued_tasks)),
            id,
            skills: std::sync::RwLock::new(skills),
            driver,
            limits,
        }
    }

    /// Снапшот текущего списка skills (клонирование под коротким read-lock).
    pub fn skills(&self) -> Vec<String> {
        self.skills
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Проверка одного skill без клонирования всего Vec — hot path resolve().
    pub fn has_skill(&self, skill: &str) -> bool {
        self.skills
            .read()
            .map(|guard| guard.iter().any(|candidate| candidate == skill))
            .unwrap_or(false)
    }

    /// Точка входа для hot-update: вызывается driver-mcp при получении
    /// notifications/tools/list_changed.
    pub fn update_skills(&self, new_skills: Vec<String>) {
        if let Ok(mut guard) = self.skills.write() {
            *guard = new_skills;
        }
    }
}

pub struct AgentRegistry {
    agents: DashMap<AgentId, Arc<RegisteredAgent>>,
}
impl AgentRegistry {
    pub fn new() -> Self {
        Self {
            agents: DashMap::new(),
        }
    }
    pub fn register(&self, agent: RegisteredAgent) {
        self.agents.insert(agent.id.clone(), Arc::new(agent));
    }
    pub fn get(&self, id: &AgentId) -> Option<Arc<RegisteredAgent>> {
        self.agents.get(id).map(|v| v.clone())
    }
    /// Все зарегистрированные агенты (для agent card и операций-обзора).
    pub fn agents(&self) -> Vec<Arc<RegisteredAgent>> {
        self.agents
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }
    pub fn resolve(&self, request: &InvokeRequest) -> Result<Arc<RegisteredAgent>, CoreError> {
        if let Some(id) = &request.agent_id {
            return self
                .get(id)
                .ok_or_else(|| CoreError::AgentNotFound(id.0.clone()));
        }
        if let Some(skill) = &request.skill_id {
            return self
                .agents
                .iter()
                .find(|entry| entry.value().has_skill(skill))
                .map(|entry| entry.value().clone())
                .ok_or(CoreError::NoEligibleAgent);
        }
        self.agents
            .iter()
            .next()
            .map(|entry| entry.value().clone())
            .ok_or(CoreError::NoEligibleAgent)
    }
}
impl Default for AgentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
pub trait PolicyEngine: Send + Sync {
    async fn authorize(&self, caller: &Caller, command: &CoreCommand) -> Result<(), CoreError>;
}
pub struct AllowAllPolicy;
#[async_trait]
impl PolicyEngine for AllowAllPolicy {
    async fn authorize(&self, _: &Caller, _: &CoreCommand) -> Result<(), CoreError> {
        Ok(())
    }
}

struct ActiveTask {
    tx: broadcast::Sender<CoreEvent>,
    cancellation: CancellationToken,
}
pub struct TaskSubscription {
    pub history: Vec<CoreEvent>,
    pub receiver: broadcast::Receiver<CoreEvent>,
    /// Максимальный `seq` среди событий в `history`. События из live
    /// `receiver` с `seq <= history_end_seq` — это дубликаты (успели попасть
    /// и в history, и в broadcast), их должен отфильтровать потребитель.
    pub history_end_seq: Option<EventSeq>,
}

/// По умолчанию каждый caller может держать одновременно столько же задач,
/// сколько глобальный пул. Вызывается только для `with_caller_quota` /
/// remote profile; локальный `new()` сохраняет прежнее поведение.
const DEFAULT_CALLER_MAX_CONCURRENT: usize = 1;

struct CoreInner {
    store: Arc<dyn TaskStore>,
    registry: Arc<AgentRegistry>,
    policy: Arc<dyn PolicyEngine>,
    global_permits: Arc<Semaphore>,
    active: DashMap<TaskId, ActiveTask>,
    // Per-caller concurrency quota. Инициализируется лениво при первом
    // invoke от конкретного caller. Значение по умолчанию — из CoreConfig,
    // не из AgentLimits: caller quota не привязана к конкретному агенту.
    per_caller_permits: DashMap<CallerId, Arc<Semaphore>>,
    default_caller_max_concurrent: usize,
}

#[derive(Clone)]
pub struct AdapterCore {
    inner: Arc<CoreInner>,
}

impl AdapterCore {
    pub fn new(
        store: Arc<dyn TaskStore>,
        registry: Arc<AgentRegistry>,
        policy: Arc<dyn PolicyEngine>,
        max_concurrent: usize,
    ) -> Self {
        Self::with_caller_quota(
            store,
            registry,
            policy,
            max_concurrent,
            DEFAULT_CALLER_MAX_CONCURRENT,
        )
    }

    /// Как `new()`, но с явным per-caller concurrency limit. Используйте это
    /// в remote profile, где один caller не должен вытеснять остальных.
    pub fn with_caller_quota(
        store: Arc<dyn TaskStore>,
        registry: Arc<AgentRegistry>,
        policy: Arc<dyn PolicyEngine>,
        max_concurrent: usize,
        default_caller_max_concurrent: usize,
    ) -> Self {
        Self {
            inner: Arc::new(CoreInner {
                store,
                registry,
                policy,
                global_permits: Arc::new(Semaphore::new(max_concurrent)),
                active: DashMap::new(),
                per_caller_permits: DashMap::new(),
                default_caller_max_concurrent,
            }),
        }
    }

    fn caller_permits(&self, caller_id: &CallerId) -> Arc<Semaphore> {
        self.inner
            .per_caller_permits
            .entry(caller_id.clone())
            .or_insert_with(|| Arc::new(Semaphore::new(self.inner.default_caller_max_concurrent)))
            .clone()
    }

    pub async fn dispatch(
        &self,
        caller: Caller,
        command: CoreCommand,
    ) -> Result<DispatchResult, CoreError> {
        self.inner.policy.authorize(&caller, &command).await?;
        match command {
            CoreCommand::Invoke(request) => self.invoke(caller, request).await,
            CoreCommand::Cancel { task_id, reason } => self.cancel(task_id, reason).await,
            CoreCommand::ProvideInput { task_id, input } => {
                self.provide_input(task_id, input).await
            }
            CoreCommand::GetStatus { task_id } => {
                Ok(DispatchResult::Status(self.snapshot(task_id).await?))
            }
        }
    }

    pub async fn subscribe(
        &self,
        task_id: TaskId,
        after_seq: EventSeq,
    ) -> Result<TaskSubscription, CoreError> {
        // Порядок операций (history ДО live subscribe) — намеренный и
        // соответствует canonical design: "при reconnect transport должен
        // запросить events_after из store, ЗАТЕМ подписаться на live events".
        //
        // Это НЕ race condition при условии, что TaskStore::events_after
        // читает под READ COMMITTED-подобной semantics (обычный SQL SELECT на
        // момент вызова), а не устаревший snapshot. transition() пишет в
        // store и делает tx.send() последовательно в одной функции, store
        // write первым — значит любое событие, отправленное между чтением
        // history и открытием receiver, либо (а) уже видно в history благодаря
        // записи в store ДО broadcast, либо (б) будет получено через receiver
        // как дубликат. Потери не возникает — возможен только дубликат,
        // который потребитель фильтрует по `history_end_seq`.
        let history = self
            .inner
            .store
            .events_after(task_id, after_seq, 500)
            .await?;
        let receiver = self
            .inner
            .active
            .get(&task_id)
            .map(|entry| entry.tx.subscribe());
        let history_end_seq = history.last().map(|event| event.seq);
        Ok(TaskSubscription {
            history,
            receiver: receiver.unwrap_or_else(closed_receiver),
            history_end_seq,
        })
    }

    /// Pull-модель чтения истории событий без live-подписки.
    /// Используется для `session/update` (snapshot-ответ): не создаёт
    /// broadcast-receiver, поэтому нет «мёртвого» канала.
    pub async fn history(
        &self,
        task_id: TaskId,
        after_seq: EventSeq,
    ) -> Result<Vec<CoreEvent>, CoreError> {
        Ok(self
            .inner
            .store
            .events_after(task_id, after_seq, 500)
            .await?)
    }

    async fn invoke(
        &self,
        caller: Caller,
        request: InvokeRequest,
    ) -> Result<DispatchResult, CoreError> {
        if request.idempotency_key.trim().is_empty() {
            return Err(CoreError::InvalidRequest("idempotency_key required".into()));
        }
        let agent = self.inner.registry.resolve(&request)?;
        let task_id = request.task_id.unwrap_or_else(Uuid::new_v4);
        let deadline_at = request
            .deadline
            .and_then(|duration| chrono::Duration::from_std(duration).ok())
            .map(|duration| chrono::Utc::now() + duration);
        let caller_id = caller.id.clone();
        let initial = self
            .inner
            .store
            .create_or_get_idempotent(NewTask {
                task_id,
                session_id: request.session_id,
                agent_id: agent.id.clone(),
                caller_id,
                idempotency_key: request.idempotency_key.clone(),
                deadline_at,
            })
            .await?;
        let snapshot = match initial {
            CreateTaskResult::Existing(s) => return Ok(DispatchResult::Existing(s)),
            CreateTaskResult::Created(s) => s,
        };
        let (tx, _) = broadcast::channel(256);
        self.inner.active.insert(
            task_id,
            ActiveTask {
                tx,
                cancellation: CancellationToken::new(),
            },
        );
        let accepted = self
            .transition(
                snapshot,
                vec![TaskState::Created],
                TaskState::Accepted,
                CoreEventKind::Accepted { queued: false },
            )
            .await?;

        // Все проверки лимитов выполняются ДО вызова driver. Очередь —
        // отдельный bounded semaphore: превышение max_queued_tasks даёт
        // typed reject, а не ожидание слота.
        let queue = match agent.queue_permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.fail_active(
                    task_id,
                    CoreError::ResourceExhausted(format!(
                        "agent {} queue full (max_queued_tasks={})",
                        agent.id.0, agent.limits.max_queued_tasks
                    )),
                )
                .await?;
                return Err(CoreError::ResourceExhausted(format!(
                    "agent {} queue full",
                    agent.id.0
                )));
            }
        };
        // Per-caller quota между queue и global: первым делом задача не
        // должна занимать глобальный слот, пока не пройдёт лимит своего
        // caller'а. Acquire вне очереди (try_acquire) — как и глобальный
        // лимит, typed reject вместо ожидания.
        let caller_permit = match self.caller_permits(&caller.id).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                drop(queue);
                self.fail_active(
                    task_id,
                    CoreError::ResourceExhausted(format!(
                        "caller {} at concurrency limit",
                        caller.id.0
                    )),
                )
                .await?;
                return Err(CoreError::ResourceExhausted(format!(
                    "caller {} at concurrency limit",
                    caller.id.0
                )));
            }
        };
        let global = match self.inner.global_permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                drop(caller_permit);
                drop(queue);
                self.fail_active(
                    task_id,
                    CoreError::ResourceExhausted("global task limit reached".into()),
                )
                .await?;
                return Err(CoreError::ResourceExhausted(
                    "global task limit reached".into(),
                ));
            }
        };
        let per_agent = match agent.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                drop(caller_permit);
                drop(queue);
                drop(global);
                self.fail_active(
                    task_id,
                    CoreError::ResourceExhausted(format!("agent {} busy", agent.id.0)),
                )
                .await?;
                return Err(CoreError::ResourceExhausted(format!(
                    "agent {} busy",
                    agent.id.0
                )));
            }
        };
        drop(queue); // задача вышла из очереди и получила слот исполнения
        let timeout = agent.limits.default_timeout;
        let core = self.clone();
        tokio::spawn(async move {
            let _caller = caller_permit;
            let _global = global;
            let _agent = per_agent;
            // Timeout отменяет driver: если default_timeout истёк, вызываем
            // cancel у driver и переводим задачу в terminal timeout state.
            // Сам timeout-future отбрасывается — driver не остаётся висеть.
            match tokio::time::timeout(timeout, core.run_driver(agent.clone(), task_id, request))
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    let _ = core.fail_active(task_id, error).await;
                }
                Err(_) => {
                    let _ = agent.driver.cancel(task_id).await;
                    let _ = core.fail_active(task_id, CoreError::Timeout).await;
                }
            }
            core.inner.active.remove(&task_id);
        });
        Ok(DispatchResult::Created(accepted))
    }

    async fn cancel(
        &self,
        task_id: TaskId,
        reason: Option<String>,
    ) -> Result<DispatchResult, CoreError> {
        let snapshot = self.snapshot(task_id).await?;
        if snapshot.state.terminal() {
            return Ok(DispatchResult::CancelRequested(snapshot));
        }
        let agent = self
            .inner
            .registry
            .get(&snapshot.agent_id)
            .ok_or_else(|| CoreError::AgentNotFound(snapshot.agent_id.0.clone()))?;
        let changed = self
            .transition(
                snapshot,
                vec![
                    TaskState::Created,
                    TaskState::Accepted,
                    TaskState::Running,
                    TaskState::WaitingForInput,
                ],
                TaskState::CancelRequested,
                CoreEventKind::CancelRequested { reason },
            )
            .await?;
        if let Some(active) = self.inner.active.get(&task_id) {
            active.cancellation.cancel();
        }
        if agent.driver.capabilities().cancellation {
            let _ = agent.driver.cancel(task_id).await;
        }
        Ok(DispatchResult::CancelRequested(changed))
    }

    async fn provide_input(
        &self,
        task_id: TaskId,
        input: Vec<Part>,
    ) -> Result<DispatchResult, CoreError> {
        let snapshot = self.snapshot(task_id).await?;
        if snapshot.state != TaskState::WaitingForInput {
            return Err(CoreError::InvalidRequest(
                "task is not waiting for input".into(),
            ));
        }
        let agent = self
            .inner
            .registry
            .get(&snapshot.agent_id)
            .ok_or_else(|| CoreError::AgentNotFound(snapshot.agent_id.0.clone()))?;
        if !agent.driver.capabilities().provide_input {
            return Err(CoreError::InvalidRequest(
                "agent does not support input".into(),
            ));
        }
        agent.driver.provide_input(task_id, input).await?;
        let changed = self
            .transition(
                snapshot,
                vec![TaskState::WaitingForInput],
                TaskState::Running,
                CoreEventKind::Progress {
                    message: "input accepted".into(),
                    percent: None,
                },
            )
            .await?;
        Ok(DispatchResult::InputAccepted(changed))
    }

    async fn run_driver(
        &self,
        agent: Arc<RegisteredAgent>,
        task_id: TaskId,
        request: InvokeRequest,
    ) -> Result<(), CoreError> {
        let mut stream = agent.driver.invoke(task_id, request).await?;
        while let Some(event) = stream.recv().await {
            self.apply_driver_event(task_id, event).await?;
            if self.snapshot(task_id).await?.state.terminal() {
                return Ok(());
            }
        }
        if !self.snapshot(task_id).await?.state.terminal() {
            return Err(CoreError::Driver(
                "driver stream closed before terminal event".into(),
            ));
        }
        Ok(())
    }

    async fn apply_driver_event(
        &self,
        task_id: TaskId,
        event: DriverEvent,
    ) -> Result<(), CoreError> {
        let snapshot = self.snapshot(task_id).await?;
        match event {
            DriverEvent::Accepted => Ok(()),
            DriverEvent::Progress { message, percent } => {
                let next = if snapshot.state == TaskState::Accepted {
                    TaskState::Running
                } else {
                    snapshot.state
                };
                self.transition(
                    snapshot,
                    vec![TaskState::Accepted, TaskState::Running],
                    next,
                    CoreEventKind::Progress { message, percent },
                )
                .await?;
                Ok(())
            }
            DriverEvent::Artifact(artifact) => {
                self.transition(
                    snapshot,
                    vec![
                        TaskState::Accepted,
                        TaskState::Running,
                        TaskState::WaitingForInput,
                    ],
                    TaskState::Running,
                    CoreEventKind::Artifact { artifact },
                )
                .await?;
                Ok(())
            }
            DriverEvent::InputRequired(request) => {
                self.transition(
                    snapshot,
                    vec![TaskState::Accepted, TaskState::Running],
                    TaskState::WaitingForInput,
                    CoreEventKind::InputRequired { request },
                )
                .await?;
                Ok(())
            }
            DriverEvent::Completed(output) => {
                self.transition(
                    snapshot,
                    vec![
                        TaskState::Accepted,
                        TaskState::Running,
                        TaskState::WaitingForInput,
                    ],
                    TaskState::Completed,
                    CoreEventKind::Completed { output },
                )
                .await?;
                Ok(())
            }
            DriverEvent::Failed(error) => {
                self.fail_active(task_id, CoreError::Driver(error.message))
                    .await
            }
            DriverEvent::Cancelled => {
                self.transition(
                    snapshot,
                    vec![
                        TaskState::CancelRequested,
                        TaskState::Accepted,
                        TaskState::Running,
                        TaskState::WaitingForInput,
                    ],
                    TaskState::Cancelled,
                    CoreEventKind::Cancelled,
                )
                .await?;
                Ok(())
            }
        }
    }

    async fn fail_active(&self, task_id: TaskId, error: CoreError) -> Result<(), CoreError> {
        let snapshot = self.snapshot(task_id).await?;
        if snapshot.state.terminal() {
            return Ok(());
        }
        self.transition(
            snapshot,
            vec![
                TaskState::Created,
                TaskState::Accepted,
                TaskState::Running,
                TaskState::WaitingForInput,
                TaskState::CancelRequested,
            ],
            TaskState::Failed,
            CoreEventKind::Failed {
                error: PublicError {
                    code: if matches!(error, CoreError::Timeout) {
                        "timeout".into()
                    } else {
                        "runtime_error".into()
                    },
                    message: error.to_string(),
                    retryable: false,
                },
            },
        )
        .await?;
        Ok(())
    }

    async fn transition(
        &self,
        snapshot: TaskSnapshot,
        allowed: Vec<TaskState>,
        next: TaskState,
        kind: CoreEventKind,
    ) -> Result<TaskSnapshot, CoreError> {
        let applied = self
            .inner
            .store
            .append_event_and_transition(TaskTransition {
                task_id: snapshot.task_id,
                expected_revision: snapshot.revision,
                allowed_states: allowed,
                next_state: next,
                event_kind: kind,
            })
            .await?;
        if let Some(active) = self.inner.active.get(&snapshot.task_id) {
            let _ = active.tx.send(applied.event);
        }
        Ok(applied.snapshot)
    }

    async fn snapshot(&self, task_id: TaskId) -> Result<TaskSnapshot, CoreError> {
        self.inner
            .store
            .get_snapshot(task_id)
            .await?
            .ok_or(CoreError::Store(StoreError::NotFound(task_id)))
    }
}

fn closed_receiver() -> broadcast::Receiver<CoreEvent> {
    let (tx, rx) = broadcast::channel(1);
    drop(tx);
    rx
}

#[cfg(test)]
mod tests {
    use super::*;
    use adapter_model::{AgentLimits, InvokeRequest};
    use memory_task_store::MemoryTaskStore;
    use std::time::Duration;

    /// Driver, блокирующий invoke: задача "зависает" до завершения теста,
    /// чтобы второй invoke того же caller гарантированно упёрся в quota.
    struct BlockingDriver {
        release: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl AgentDriver for BlockingDriver {
        fn id(&self) -> &str {
            "blocking"
        }
        fn capabilities(&self) -> DriverCapabilities {
            DriverCapabilities {
                cancellation: true,
                provide_input: true,
            }
        }
        async fn health(&self) -> Result<(), CoreError> {
            Ok(())
        }
        async fn invoke(
            &self,
            _task_id: TaskId,
            _request: InvokeRequest,
        ) -> Result<mpsc::Receiver<DriverEvent>, CoreError> {
            let (tx, rx) = mpsc::channel(8);
            let release = self.release.clone();
            tokio::spawn(async move {
                release.notified().await;
                let _ = tx.send(DriverEvent::Completed(Vec::new())).await;
            });
            Ok(rx)
        }
        async fn cancel(&self, _task_id: TaskId) -> Result<(), CoreError> {
            Ok(())
        }
        async fn provide_input(
            &self,
            _task_id: TaskId,
            _input: Vec<Part>,
        ) -> Result<(), CoreError> {
            Ok(())
        }
    }

    fn test_core(default_caller_max_concurrent: usize) -> Arc<AdapterCore> {
        let store: Arc<dyn TaskStore> = Arc::new(MemoryTaskStore::new());
        let registry = Arc::new(AgentRegistry::new());
        let release = Arc::new(tokio::sync::Notify::new());
        registry.register(RegisteredAgent::new(
            AgentId("blocking".into()),
            vec!["skill".into()],
            Arc::new(BlockingDriver { release }),
            AgentLimits {
                max_concurrent_tasks: 16,
                max_queued_tasks: 64,
                max_input_bytes: 1024 * 1024,
                max_event_bytes: 256 * 1024,
                default_timeout: Duration::from_secs(30),
            },
        ));
        Arc::new(AdapterCore::with_caller_quota(
            store,
            registry,
            Arc::new(AllowAllPolicy),
            16,
            default_caller_max_concurrent,
        ))
    }

    async fn invoke_async(
        core: &AdapterCore,
        caller: &str,
        key: &str,
    ) -> Result<DispatchResult, CoreError> {
        core.dispatch(
            Caller {
                id: CallerId(caller.into()),
                scopes: Vec::new(),
            },
            CoreCommand::Invoke(InvokeRequest {
                task_id: None,
                agent_id: None,
                skill_id: None,
                idempotency_key: key.into(),
                session_id: None,
                input: Vec::new(),
                context: serde_json::Value::Null,
                deadline: None,
            }),
        )
        .await
    }

    #[tokio::test]
    async fn caller_quota_rejects_second_concurrent_task_from_same_caller() {
        let core = test_core(1);
        // Первый invoke создаёт задачу и удерживает единственный permit
        // caller'а (driver ждёт release — задача жива до конца теста).
        assert!(invoke_async(&core, "caller-a", "k1").await.is_ok());
        // Второй invoke того же caller — quota exhausted.
        let err = invoke_async(&core, "caller-a", "k2")
            .await
            .expect_err("expected quota rejection");
        assert!(matches!(err, CoreError::ResourceExhausted(_)));
        // Другой caller не затронут.
        assert!(invoke_async(&core, "caller-b", "k3").await.is_ok());
    }

    #[tokio::test]
    async fn caller_quota_is_per_caller_not_global() {
        let core = test_core(2);
        assert!(invoke_async(&core, "caller-a", "k1").await.is_ok());
        assert!(invoke_async(&core, "caller-a", "k2").await.is_ok());
        // Третий упёрся в quota=2.
        let err = invoke_async(&core, "caller-a", "k3")
            .await
            .expect_err("expected quota rejection");
        assert!(matches!(err, CoreError::ResourceExhausted(_)));
    }

    #[test]
    fn update_skills_reflects_in_snapshot_and_has_skill() {
        let agent = RegisteredAgent::new(
            AgentId("hot-update".into()),
            vec!["old-skill".into()],
            Arc::new(BlockingDriver {
                release: Arc::new(tokio::sync::Notify::new()),
            }),
            AgentLimits {
                max_concurrent_tasks: 1,
                max_queued_tasks: 4,
                max_input_bytes: 1024,
                max_event_bytes: 256,
                default_timeout: Duration::from_secs(30),
            },
        );
        assert!(agent.has_skill("old-skill"));
        assert!(!agent.has_skill("new-skill"));

        // Hot-update (driver-mcp при tools/list_changed).
        agent.update_skills(vec!["new-skill".into()]);

        assert!(!agent.has_skill("old-skill"), "old skill must be replaced");
        assert!(agent.has_skill("new-skill"), "new skill must be visible");
        assert_eq!(agent.skills(), vec!["new-skill".to_string()]);
    }

    #[tokio::test]
    async fn resolve_uses_updated_skills_after_hot_update() {
        let registry = AgentRegistry::new();
        registry.register(RegisteredAgent::new(
            AgentId("hot-update".into()),
            vec!["old-skill".into()],
            Arc::new(BlockingDriver {
                release: Arc::new(tokio::sync::Notify::new()),
            }),
            AgentLimits {
                max_concurrent_tasks: 1,
                max_queued_tasks: 4,
                max_input_bytes: 1024,
                max_event_bytes: 256,
                default_timeout: Duration::from_secs(30),
            },
        ));

        let request_old = InvokeRequest {
            task_id: None,
            agent_id: None,
            skill_id: Some("old-skill".into()),
            idempotency_key: "k".into(),
            session_id: None,
            input: Vec::new(),
            context: serde_json::Value::Null,
            deadline: None,
        };
        assert!(registry.resolve(&request_old).is_ok());

        // Hot-update: старый skill исчезает, появляется новый.
        let agent = registry.get(&AgentId("hot-update".into())).unwrap();
        agent.update_skills(vec!["new-skill".into()]);

        assert!(
            registry.resolve(&request_old).is_err(),
            "old skill must no longer resolve after hot update"
        );
        let request_new = InvokeRequest {
            task_id: None,
            agent_id: None,
            skill_id: Some("new-skill".into()),
            idempotency_key: "k2".into(),
            session_id: None,
            input: Vec::new(),
            context: serde_json::Value::Null,
            deadline: None,
        };
        assert!(
            registry.resolve(&request_new).is_ok(),
            "new skill must resolve after hot update"
        );
    }
}
