//! `adapter-store-contract` — common durable/in-memory storage contract.
//!
//! SqliteTaskStore, PostgresTaskStore and MemoryTaskStore implement this trait.
//! Adapter Core only depends on this contract; it never imports sqlx.

use std::time::Duration;

use adapter_model::{
    AppliedTransition, CoreEvent, CreateTaskResult, EventSeq, NewTask,
    TaskId, TaskSnapshot, TaskTransition,
};
use async_trait::async_trait;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct Lease {
    pub task_id: TaskId,
    pub owner_id: String,
    pub expires_in: Duration,
}

#[derive(Clone, Debug)]
pub struct RetentionPolicy {
    pub task_ttl: Duration,
    pub event_ttl: Duration,
    pub idempotency_ttl: Duration,
    pub cleanup_batch_size: u32,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            task_ttl: Duration::from_secs(7 * 24 * 3600),
            event_ttl: Duration::from_secs(7 * 24 * 3600),
            idempotency_ttl: Duration::from_secs(24 * 3600),
            cleanup_batch_size: 1_000,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CleanupReport {
    pub tasks_deleted: u64,
    pub events_deleted: u64,
    pub idempotency_deleted: u64,
}

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("task not found: {0}")]
    NotFound(adapter_model::TaskId),
    #[error("concurrent mutation conflict")]
    Conflict,
    #[error("invalid task transition: {0}")]
    InvalidTransition(String),
    #[error("storage unavailable: {0}")]
    Unavailable(String),
    #[error("storage corruption: {0}")]
    Corrupt(String),
    #[error("storage internal error: {0}")]
    Internal(String),
}

#[async_trait]
pub trait TaskStore: Send + Sync {
    async fn create_or_get_idempotent(&self, task: NewTask)
        -> Result<CreateTaskResult, StoreError>;

    async fn get_snapshot(&self, task_id: TaskId)
        -> Result<Option<TaskSnapshot>, StoreError>;

    /// Atomically validates task state/revision, stores one event and changes
    /// snapshot state. The event must be durable before this method returns.
    async fn append_event_and_transition(&self, transition: TaskTransition)
        -> Result<AppliedTransition, StoreError>;

    async fn events_after(&self, task_id: TaskId, after_seq: EventSeq, limit: u32)
        -> Result<Vec<CoreEvent>, StoreError>;

    /// Required only for multi-instance/Postgres profile. Memory/SQLite single
    /// daemon implementations may always return `true` for a local owner.
    async fn acquire_lease(&self, lease: Lease) -> Result<bool, StoreError>;
    async fn renew_lease(&self, lease: Lease) -> Result<bool, StoreError>;

    async fn cleanup(&self, policy: &RetentionPolicy)
        -> Result<CleanupReport, StoreError>;
}
