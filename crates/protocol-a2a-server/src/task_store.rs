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
        let _snapshot = self
            .inner
            .get_snapshot(task_id)
            .await
            .map_err(store_error_to_a2a)?;
        // A2A-level create: DefaultRequestHandler сам ведёт lifecycle через
        // executor-события. Здесь достаточно вернуть версию 0/1.
        Ok(1)
    }

    async fn update(&self, task: Task) -> Result<u64, A2AError> {
        let _task_id: TaskId = task
            .id
            .parse()
            .map_err(|_| A2AError::invalid_request("task_id must be a UUID"))?;
        Ok(1)
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
        adapter_model::TaskState::CancelRequested | adapter_model::TaskState::Cancelled => {
            TaskState::Canceled
        }
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
