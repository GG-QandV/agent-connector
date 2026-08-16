//! `AdapterTaskStore` — реализация `a2a-server::TaskStore` поверх
//! `adapter-store-contract::TaskStore`.
//!
//! `a2a-server::DefaultRequestHandler` использует этот store для
//! create/update/get/list A2A `Task`. Мы маппим `TaskSnapshot` <-> `Task`.
//! Задача считается существующей, если её snapshot не терминален или task_id
//! известен store'у.

use a2a::*;
use a2a_server::TaskStore;
use adapter_core::TaskId;
use adapter_store_contract::{StoreError, TaskStore as AdapterTaskStoreContract};
use async_trait::async_trait;
use std::sync::Arc;
use tracing::debug;

pub struct AdapterTaskStore {
    inner: Arc<dyn AdapterTaskStoreContract>,
}

impl Clone for AdapterTaskStore {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl AdapterTaskStore {
    pub fn new(inner: Arc<dyn AdapterTaskStoreContract>) -> Self {
        Self { inner }
    }
}

fn store_error_to_a2a(error: StoreError) -> A2AError {
    match error {
        StoreError::NotFound(id) => A2AError::task_not_found(&id.to_string()),
        StoreError::Conflict => A2AError::internal("concurrent mutation conflict"),
        StoreError::InvalidTransition(msg) => A2AError::internal(msg),
        StoreError::Unavailable(msg) | StoreError::Corrupt(msg) | StoreError::Internal(msg) => {
            A2AError::internal(msg)
        }
    }
}

#[async_trait]
impl TaskStore for AdapterTaskStore {
    async fn create(&self, task: Task) -> Result<u64, A2AError> {
        let task_id: TaskId = task
            .id
            .parse()
            .map_err(|_| A2AError::invalid_request("task_id must be a UUID"))?;
        let snapshot = self
            .inner
            .get_snapshot(task_id)
            .await
            .map_err(store_error_to_a2a)?;
        // A2A-level create: DefaultRequestHandler сам ведёт lifecycle через
        // executor-события, store здесь только читается. Возвращаем реальную
        // версию существующего snapshot; если задачи ещё нет в store —
        // фиксируем это debug-логом (ожидаемо: executor создаёт события
        // асинхронно) и возвращаем начальную версию.
        match snapshot {
            Some(snapshot) => Ok(snapshot.revision),
            None => {
                debug!(task_id = %task_id, "create: task not yet in store");
                Ok(1)
            }
        }
    }

    async fn update(&self, task: Task) -> Result<u64, A2AError> {
        let task_id: TaskId = task
            .id
            .parse()
            .map_err(|_| A2AError::invalid_request("task_id must be a UUID"))?;
        let snapshot = self
            .inner
            .get_snapshot(task_id)
            .await
            .map_err(store_error_to_a2a)?;
        match snapshot {
            Some(snapshot) => {
                // store ведёт состояние через CoreEvent'ы; A2A-level update
                // не меняет snapshot. Логируем рассинхрон, если SDK-состояние
                // разошлось с реальным.
                let sdk_state = task_state_to_sdk(&task.status.state, snapshot.state);
                if sdk_state != snapshot.state {
                    debug!(
                        task_id = %task_id,
                        sdk_state = ?sdk_state,
                        stored_state = ?snapshot.state,
                        "update: SDK task state diverged from store snapshot"
                    );
                }
                Ok(snapshot.revision)
            }
            // SDK-контракт: update на несуществующей задаче -> TASK_NOT_FOUND,
            // чтобы DefaultRequestHandler (save_task) перешёл к create.
            None => Err(A2AError::task_not_found(&task_id.to_string())),
        }
    }

    async fn get(&self, task_id: &str) -> Result<Option<Task>, A2AError> {
        let task_id: TaskId = task_id
            .parse()
            .map_err(|_| A2AError::invalid_request("task_id must be a UUID"))?;
        let snapshot = self
            .inner
            .get_snapshot(task_id)
            .await
            .map_err(store_error_to_a2a)?;
        Ok(snapshot.map(task_to_a2a))
    }

    async fn list(&self, req: &ListTasksRequest) -> Result<ListTasksResponse, A2AError> {
        // AdapterCore не хранит индекс "все задачи" без контекста; для MVP
        // возвращаем пустой список. Полноценный list — отдельная задача.
        let _ = req;
        Ok(ListTasksResponse {
            tasks: Vec::new(),
            next_page_token: String::new(),
            page_size: 0,
            total_size: 0,
        })
    }
}

fn task_to_a2a(snapshot: adapter_core::TaskSnapshot) -> Task {
    let state = match snapshot.state {
        adapter_model::TaskState::Created
        | adapter_model::TaskState::Accepted
        | adapter_model::TaskState::Running => TaskState::Working,
        adapter_model::TaskState::WaitingForInput => TaskState::InputRequired,
        // CancelRequested — запрошена отмена, задача ещё может работать;
        // Canceled — терминальная отмена. A2A не различает их отдельными
        // состояниями, но для терминальности stream важен именно Canceled.
        adapter_model::TaskState::CancelRequested => TaskState::Working,
        adapter_model::TaskState::Cancelled => TaskState::Canceled,
        adapter_model::TaskState::Completed => TaskState::Completed,
        adapter_model::TaskState::Failed => TaskState::Failed,
    };
    Task {
        id: snapshot.task_id.to_string(),
        context_id: snapshot
            .session_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| snapshot.task_id.to_string()),
        status: TaskStatus {
            state,
            message: None,
            timestamp: Some(snapshot.updated_at),
        },
        artifacts: None,
        history: None,
        metadata: None,
    }
}

/// Обратный маппинг: A2A `TaskState` -> наш `TaskState` для сравнения
/// с реальным snapshot в `update`.
fn task_state_to_sdk(
    state: &TaskState,
    fallback: adapter_model::TaskState,
) -> adapter_model::TaskState {
    match state {
        TaskState::Unspecified => fallback,
        TaskState::Working => adapter_model::TaskState::Running,
        TaskState::InputRequired => adapter_model::TaskState::WaitingForInput,
        TaskState::Canceled => adapter_model::TaskState::Cancelled,
        TaskState::Completed => adapter_model::TaskState::Completed,
        TaskState::Failed => adapter_model::TaskState::Failed,
        TaskState::Submitted | TaskState::Rejected | TaskState::AuthRequired => fallback,
    }
}
