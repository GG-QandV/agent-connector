//! `storage-memory` — in-memory TaskStore for tests, demos and ephemeral mode.
//!
//! It implements the same contract as SQLite/Postgres. It is not durable and
//! must not be used for remote production mode: process restart loses tasks,
//! event history and idempotency records.

use std::{collections::HashMap, sync::Arc};

use adapter_model::{
    AppliedTransition, CoreEvent, CreateTaskResult, EventSeq, NewTask, TaskId, TaskSnapshot,
    TaskTransition,
};
use adapter_store_contract::{CleanupReport, Lease, RetentionPolicy, StoreError, TaskStore};
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::Mutex;

#[derive(Default)]
struct State {
    tasks: HashMap<TaskId, TaskSnapshot>,
    events: HashMap<TaskId, Vec<CoreEvent>>,
    idempotency: HashMap<(adapter_model::CallerId, String), TaskId>,
    leases: HashMap<TaskId, LeaseRecord>,
}

struct LeaseRecord {
    owner_id: String,
    expires_at: chrono::DateTime<Utc>,
}

#[derive(Clone, Default)]
pub struct MemoryTaskStore {
    state: Arc<Mutex<State>>,
}

impl MemoryTaskStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TaskStore for MemoryTaskStore {
    async fn create_or_get_idempotent(
        &self,
        task: NewTask,
    ) -> Result<CreateTaskResult, StoreError> {
        let mut state = self.state.lock().await;
        let key = (task.caller_id.clone(), task.idempotency_key.clone());
        if let Some(task_id) = state.idempotency.get(&key) {
            let existing =
                state.tasks.get(task_id).cloned().ok_or_else(|| {
                    StoreError::Corrupt("idempotency references absent task".into())
                })?;
            return Ok(CreateTaskResult::Existing(existing));
        }
        let now = Utc::now();
        let snapshot = TaskSnapshot {
            task_id: task.task_id,
            session_id: task.session_id,
            agent_id: task.agent_id,
            caller_id: task.caller_id,
            state: adapter_model::TaskState::Created,
            revision: 0,
            last_seq: 0,
            created_at: now,
            updated_at: now,
            terminal_at: None,
        };
        state.idempotency.insert(key, task.task_id);
        state.tasks.insert(task.task_id, snapshot.clone());
        state.events.insert(task.task_id, Vec::new());
        Ok(CreateTaskResult::Created(snapshot))
    }

    async fn get_snapshot(&self, task_id: TaskId) -> Result<Option<TaskSnapshot>, StoreError> {
        Ok(self.state.lock().await.tasks.get(&task_id).cloned())
    }

    async fn append_event_and_transition(
        &self,
        transition: TaskTransition,
    ) -> Result<AppliedTransition, StoreError> {
        let mut state = self.state.lock().await;
        let snapshot = state
            .tasks
            .get_mut(&transition.task_id)
            .ok_or(StoreError::NotFound(transition.task_id))?;
        if snapshot.revision != transition.expected_revision {
            return Err(StoreError::Conflict);
        }
        if !transition.allowed_states.contains(&snapshot.state) {
            return Err(StoreError::InvalidTransition(format!(
                "current={:?}",
                snapshot.state
            )));
        }
        let now = Utc::now();
        snapshot.state = transition.next_state;
        snapshot.revision += 1;
        snapshot.last_seq += 1;
        snapshot.updated_at = now;
        if snapshot.state.terminal() {
            snapshot.terminal_at = Some(now);
        }
        let seq = snapshot.last_seq;
        let snapshot = snapshot.clone();
        let event = CoreEvent {
            task_id: transition.task_id,
            seq,
            at: now,
            kind: transition.event_kind,
        };
        state
            .events
            .entry(transition.task_id)
            .or_default()
            .push(event.clone());
        Ok(AppliedTransition { snapshot, event })
    }

    async fn events_after(
        &self,
        task_id: TaskId,
        after_seq: EventSeq,
        limit: u32,
    ) -> Result<Vec<CoreEvent>, StoreError> {
        let state = self.state.lock().await;
        if !state.tasks.contains_key(&task_id) {
            return Err(StoreError::NotFound(task_id));
        }
        Ok(state
            .events
            .get(&task_id)
            .into_iter()
            .flatten()
            .filter(|event| event.seq > after_seq)
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn acquire_lease(&self, lease: Lease) -> Result<bool, StoreError> {
        let mut state = self.state.lock().await;
        let now = Utc::now();
        let expires_at = now
            + chrono::Duration::from_std(lease.expires_in)
                .map_err(|error| StoreError::Internal(error.to_string()))?;
        match state.leases.get(&lease.task_id) {
            Some(existing) if existing.expires_at > now && existing.owner_id != lease.owner_id => {
                Ok(false)
            }
            _ => {
                state.leases.insert(
                    lease.task_id,
                    LeaseRecord {
                        owner_id: lease.owner_id,
                        expires_at,
                    },
                );
                Ok(true)
            }
        }
    }

    async fn renew_lease(&self, lease: Lease) -> Result<bool, StoreError> {
        let mut state = self.state.lock().await;
        let now = Utc::now();
        let expires_at = now
            + chrono::Duration::from_std(lease.expires_in)
                .map_err(|error| StoreError::Internal(error.to_string()))?;
        match state.leases.get_mut(&lease.task_id) {
            Some(existing) if existing.owner_id == lease.owner_id && existing.expires_at >= now => {
                existing.expires_at = expires_at;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn cleanup(&self, policy: &RetentionPolicy) -> Result<CleanupReport, StoreError> {
        let mut state = self.state.lock().await;
        let now = Utc::now();
        let task_cutoff = now
            - chrono::Duration::from_std(policy.task_ttl)
                .map_err(|error| StoreError::Internal(error.to_string()))?;
        let event_cutoff = now
            - chrono::Duration::from_std(policy.event_ttl)
                .map_err(|error| StoreError::Internal(error.to_string()))?;
        let expired_tasks: Vec<TaskId> = state
            .tasks
            .iter()
            .filter_map(|(id, snapshot)| {
                snapshot
                    .terminal_at
                    .filter(|at| *at < task_cutoff)
                    .map(|_| *id)
            })
            .take(policy.cleanup_batch_size as usize)
            .collect();
        let mut report = CleanupReport::default();
        for task_id in expired_tasks {
            if let Some(snapshot) = state.tasks.remove(&task_id) {
                state
                    .idempotency
                    .remove(&(snapshot.caller_id, format!("memory:{}", task_id)));
                state.events.remove(&task_id);
                state.leases.remove(&task_id);
                report.tasks_deleted += 1;
            }
        }
        for events in state.events.values_mut() {
            let before = events.len();
            events.retain(|event| event.at >= event_cutoff);
            report.events_deleted += (before - events.len()) as u64;
        }
        Ok(report)
    }
}
