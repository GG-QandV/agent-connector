//! DIFF 1/2 — crates/adapter-core/src/lib.rs
//! Изменяет ТОЛЬКО RegisteredAgent/AgentRegistry::resolve(). Остальной
//! файл (AdapterCore, invoke/cancel/provide_input/apply_driver_event,
//! CoreError, DriverEvent) — без изменений, привожу здесь как контекст,
//! чтобы diff был вставляем без угадывания окружающего кода.

use adapter_model::{
    AgentId, AgentLimits, ArtifactRef, Caller, CallerId, CoreCommand, CoreEvent, CoreEventKind,
    CreateTaskResult, DispatchResult, DriverCapabilities, EventSeq, InputRequest, InvokeRequest,
    NewTask, Part, PublicError, TaskId, TaskSnapshot, TaskState, TaskTransition,
};
use adapter_store_contract::{StoreError, TaskStore};
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore}; // ДОБАВЛЕНО: RwLock
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// --- CoreError, DriverEvent, AgentDriver trait — БЕЗ ИЗМЕНЕНИЙ ---
// (см. текущую версию файла — не переписываю здесь, чтобы не создавать
// риск случайного расхождения с error-вариантами, которые не менялись)

/// БЫЛО:
///   pub struct RegisteredAgent {
///       pub id: AgentId,
///       pub skills: Vec<String>,          // <- ЗАМОРОЖЕНО в Arc, менять нельзя
///       pub driver: Arc<dyn AgentDriver>,
///       pub limits: AgentLimits,
///       permits: Arc<Semaphore>,
///       queue_permits: Arc<Semaphore>,
///   }
///
/// СТАЛО: skills — приватное поле за RwLock, доступ только через методы.
/// Публичный API register()/get()/agents() НЕ меняется — только тип поля
/// и то, что чтение/запись skills стало async.
pub struct RegisteredAgent {
    pub id: AgentId,
    skills: Arc<RwLock<Vec<String>>>, // ИЗМЕНЕНО: было `pub skills: Vec<String>`
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
            skills: Arc::new(RwLock::new(skills)), // ИЗМЕНЕНО: оборачиваем в RwLock
            driver,
            limits,
        }
    }

    /// НОВОЕ: снапшот текущего списка skills. Заменяет прямой доступ к
    /// публичному полю `agent.skills` — вызывающий код (ACP/A2A мапперы,
    /// если они читали skills напрямую) должен перейти на `.skills().await`.
    pub async fn skills(&self) -> Vec<String> {
        self.skills.read().await.clone()
    }

    /// НОВОЕ: проверка одного skill без клонирования всего Vec — то, что
    /// использует AgentRegistry::resolve() в hot path.
    pub async fn has_skill(&self, skill: &str) -> bool {
        self.skills.read().await.iter().any(|s| s == skill)
    }

    /// НОВОЕ: точка входа для hot-update — вызывается driver-mcp при
    /// получении notifications/tools/list_changed.
    pub async fn update_skills(&self, new_skills: Vec<String>) {
        *self.skills.write().await = new_skills;
    }
}

pub struct AgentRegistry {
    agents: DashMap<AgentId, Arc<RegisteredAgent>>,
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self { agents: DashMap::new() }
    }

    pub fn register(&self, agent: RegisteredAgent) {
        self.agents.insert(agent.id.clone(), Arc::new(agent));
    }

    pub fn get(&self, id: &AgentId) -> Option<Arc<RegisteredAgent>> {
        self.agents.get(id).map(|v| v.clone())
    }

    pub fn agents(&self) -> Vec<Arc<RegisteredAgent>> {
        self.agents.iter().map(|entry| entry.value().clone()).collect()
    }

    /// ИЗМЕНЕНО: было sync fn, стало async fn — единственная сигнатурная
    /// правка, видимая вызывающему коду (AdapterCore::invoke() уже async,
    /// так что `.resolve(request).await?` вместо `.resolve(request)?` —
    /// правка одной строки в invoke(), см. DIFF ниже).
    ///
    /// БЫЛО:
    ///   pub fn resolve(&self, request: &InvokeRequest) -> Result<Arc<RegisteredAgent>, CoreError> {
    ///       ...
    ///       .find(|entry| entry.skills.iter().any(|candidate| candidate == skill))
    ///       ...
    ///   }
    pub async fn resolve(&self, request: &InvokeRequest) -> Result<Arc<RegisteredAgent>, CoreError> {
        if let Some(id) = &request.agent_id {
            return self.get(id).ok_or_else(|| CoreError::AgentNotFound(id.0.clone()));
        }
        if let Some(skill) = &request.skill_id {
            // ИЗМЕНЕНО: было синхронное entry.skills.iter().any(...),
            // теперь async has_skill() — итерируем агентов sync (DashMap
            // сам sync), но проверку skill делаем через async метод.
            for entry in self.agents.iter() {
                if entry.value().has_skill(skill).await {
                    return Ok(entry.value().clone());
                }
            }
            return Err(CoreError::NoEligibleAgent);
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

// --- PolicyEngine, AllowAllPolicy, ActiveTask, TaskSubscription,
//     CoreInner — БЕЗ ИЗМЕНЕНИЙ ---

// ============================================================
// AdapterCore::invoke() — ЕДИНСТВЕННАЯ строка требует правки из-за
// resolve() стал async. Показан минимальный diff внутри invoke(),
// остальное тело функции (idempotency check, task_id, deadline,
// store.create_or_get_idempotent, transition to Accepted, semaphores,
// spawn) — БЕЗ ИЗМЕНЕНИЙ.
// ============================================================
impl AdapterCore {
    async fn invoke(&self, caller: Caller, request: InvokeRequest) -> Result<DispatchResult, CoreError> {
        if request.idempotency_key.trim().is_empty() {
            return Err(CoreError::InvalidRequest("idempotency_key required".into()));
        }
        // БЫЛО: let agent = self.inner.registry.resolve(&request)?;
        let agent = self.inner.registry.resolve(&request).await?; // ИЗМЕНЕНО: добавлен .await

        // ... остальное тело invoke() без изменений ...
        unimplemented!("см. текущий файл для полного тела — здесь показана только изменённая строка")
    }
}

// Заглушки для компиляции этого diff-файла изолированно — в реальном
// файле эти типы уже определены выше по файлу, не дублировать.
struct AdapterCore { inner: Arc<()> }
enum CoreError { AgentNotFound(String), NoEligibleAgent, InvalidRequest(String) }
trait AgentDriver: Send + Sync {}
