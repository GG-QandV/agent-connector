//! `storage-sqlite` — durable single-node task/event store for Adapter Core.
//!
//! Design goals:
//! - SQLite is the default embedded storage backend for one Adapter Daemon.
//! - WAL + short transactions + a process-local write mutex serialize writes.
//! - Critical state transition and event journal insert are atomic.
//! - Event replay is cursor-based (`seq > after_seq`).
//! - Completed tasks/events/artifact metadata are removed by retention policy.
//!
//! Cargo.toml dependencies:
//! async-trait = "0.1"
//! chrono = { version = "0.4", features = ["serde"] }
//! serde = { version = "1", features = ["derive"] }
//! serde_json = "1"
//! sqlx = { version = "0.8", features = ["runtime-tokio-rustls", "sqlite", "chrono", "uuid"] }
//! thiserror = "2"
//! tokio = { version = "1", features = ["sync"] }
//! uuid = { version = "1", features = ["serde"] }
//!
//! Integration note:
//! The first generated `adapter_core.rs` uses `MemoryTaskStore` directly.
//! Production integration should extract a `TaskStore` trait from that core and
//! make this type its SQLite implementation. This module intentionally contains
//! no HTTP/A2A/ACP/driver dependencies.

use std::{path::Path, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous}, Row, Sqlite, SqlitePool, Transaction};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

pub type TaskId = Uuid;
pub type SessionId = Uuid;
pub type EventSeq = u64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    pub fn terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    fn as_db(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::WaitingForInput => "waiting_for_input",
            Self::CancelRequested => "cancel_requested",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn from_db(value: &str) -> Result<Self, StoreError> {
        match value {
            "created" => Ok(Self::Created),
            "accepted" => Ok(Self::Accepted),
            "running" => Ok(Self::Running),
            "waiting_for_input" => Ok(Self::WaitingForInput),
            "cancel_requested" => Ok(Self::CancelRequested),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(StoreError::Corrupt(format!("unknown task state: {other}"))),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskSnapshot {
    pub task_id: TaskId,
    pub session_id: Option<SessionId>,
    pub agent_id: String,
    pub caller_id: String,
    pub idempotency_key: String,
    pub state: TaskState,
    pub revision: u64,
    pub last_seq: EventSeq,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub terminal_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredEvent {
    pub task_id: TaskId,
    pub seq: EventSeq,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct NewTask {
    pub task_id: TaskId,
    pub session_id: Option<SessionId>,
    pub agent_id: String,
    pub caller_id: String,
    pub idempotency_key: String,
    pub deadline_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub enum CreateTaskResult {
    Created(TaskSnapshot),
    Existing(TaskSnapshot),
}

#[derive(Clone, Debug)]
pub struct Transition {
    pub task_id: TaskId,
    pub expected_revision: u64,
    pub allowed_states: Vec<TaskState>,
    pub next_state: TaskState,
    pub event_type: String,
    pub event_payload: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct AppliedTransition {
    pub snapshot: TaskSnapshot,
    pub event: StoredEvent,
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

#[derive(Clone, Debug)]
pub struct CleanupReport {
    pub tasks_deleted: u64,
    pub events_deleted: u64,
    pub idempotency_deleted: u64,
}

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("task not found: {0}")]
    NotFound(TaskId),
    #[error("idempotency conflict")]
    IdempotencyConflict,
    #[error("concurrent transition conflict")]
    Conflict,
    #[error("invalid transition: current={current:?}, allowed={allowed:?}")]
    InvalidTransition { current: TaskState, allowed: Vec<TaskState> },
    #[error("database data is corrupt: {0}")]
    Corrupt(String),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub struct SqliteTaskStore {
    pool: SqlitePool,
    // SQLite WAL permits many readers but only one active writer. This mutex
    // creates a predictable single write path inside one Adapter Daemon.
    write_guard: Arc<Mutex<()>>,
}

impl SqliteTaskStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5))
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .min_connections(1)
            .connect_with(options)
            .await?;
        let store = Self { pool, write_guard: Arc::new(Mutex::new(())) };
        store.migrate().await?;
        Ok(store)
    }

    pub fn pool(&self) -> &SqlitePool { &self.pool }

    pub async fn migrate(&self) -> Result<(), StoreError> {
        let _guard = self.write_guard.lock().await;
        sqlx::query("PRAGMA journal_mode = WAL").execute(&self.pool).await?;
        sqlx::query("PRAGMA synchronous = NORMAL").execute(&self.pool).await?;
        sqlx::query("PRAGMA foreign_keys = ON").execute(&self.pool).await?;
        sqlx::query("PRAGMA busy_timeout = 5000").execute(&self.pool).await?;

        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
        "#).execute(&self.pool).await?;

        let version: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
            .fetch_one(&self.pool).await?;
        if version < 1 {
            let mut tx = self.pool.begin().await?;
            sqlx::query(r#"
                CREATE TABLE tasks (
                    task_id TEXT PRIMARY KEY NOT NULL,
                    session_id TEXT NULL,
                    agent_id TEXT NOT NULL,
                    caller_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    state TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 0,
                    last_seq INTEGER NOT NULL DEFAULT 0,
                    deadline_at TEXT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    terminal_at TEXT NULL
                )
            "#).execute(&mut *tx).await?;
            sqlx::query(r#"
                CREATE UNIQUE INDEX ux_tasks_caller_idempotency
                ON tasks(caller_id, idempotency_key)
            "#).execute(&mut *tx).await?;
            sqlx::query(r#"
                CREATE INDEX ix_tasks_active
                ON tasks(state, updated_at)
            "#).execute(&mut *tx).await?;
            sqlx::query(r#"
                CREATE INDEX ix_tasks_session
                ON tasks(session_id, updated_at)
            "#).execute(&mut *tx).await?;
            sqlx::query(r#"
                CREATE TABLE task_events (
                    task_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(task_id, seq),
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
                )
            "#).execute(&mut *tx).await?;
            sqlx::query(r#"
                CREATE INDEX ix_task_events_retention
                ON task_events(created_at)
            "#).execute(&mut *tx).await?;
            sqlx::query(r#"
                CREATE TABLE task_leases (
                    task_id TEXT PRIMARY KEY NOT NULL,
                    owner_id TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
                )
            "#).execute(&mut *tx).await?;
            sqlx::query(
                "INSERT INTO schema_migrations(version, applied_at) VALUES(1, ?)",
            ).bind(Utc::now().to_rfc3339()).execute(&mut *tx).await?;
            tx.commit().await?;
        }
        Ok(())
    }

    /// Atomically creates task or returns an existing task for the same
    /// `(caller_id, idempotency_key)` pair.
    pub async fn create_or_get_idempotent(&self, task: NewTask) -> Result<CreateTaskResult, StoreError> {
        let _guard = self.write_guard.lock().await;
        let mut tx = self.pool.begin().await?;
        if let Some(row) = sqlx::query("SELECT * FROM tasks WHERE caller_id = ? AND idempotency_key = ?")
            .bind(&task.caller_id)
            .bind(&task.idempotency_key)
            .fetch_optional(&mut *tx).await? {
            let snapshot = snapshot_from_row(&row)?;
            tx.commit().await?;
            return Ok(CreateTaskResult::Existing(snapshot));
        }

        let now = Utc::now();
        sqlx::query(r#"
            INSERT INTO tasks(
                task_id, session_id, agent_id, caller_id, idempotency_key,
                state, revision, last_seq, deadline_at, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, 'created', 0, 0, ?, ?, ?)
        "#)
            .bind(task.task_id.to_string())
            .bind(task.session_id.map(|id| id.to_string()))
            .bind(&task.agent_id)
            .bind(&task.caller_id)
            .bind(&task.idempotency_key)
            .bind(task.deadline_at.map(|value| value.to_rfc3339()))
            .bind(now.to_rfc3339())
            .bind(now.to_rfc3339())
            .execute(&mut *tx).await?;
        let row = sqlx::query("SELECT * FROM tasks WHERE task_id = ?")
            .bind(task.task_id.to_string()).fetch_one(&mut *tx).await?;
        let snapshot = snapshot_from_row(&row)?;
        tx.commit().await?;
        Ok(CreateTaskResult::Created(snapshot))
    }

    pub async fn get_snapshot(&self, task_id: TaskId) -> Result<Option<TaskSnapshot>, StoreError> {
        let row = sqlx::query("SELECT * FROM tasks WHERE task_id = ?")
            .bind(task_id.to_string()).fetch_optional(&self.pool).await?;
        row.map(|row| snapshot_from_row(&row)).transpose()
    }

    /// Durable state transition. The task state, revision, sequence and event
    /// are committed in the same SQLite transaction before any caller can
    /// publish the event to SSE/A2A/ACP.
    pub async fn append_event_and_transition(
        &self,
        transition: Transition,
    ) -> Result<AppliedTransition, StoreError> {
        let _guard = self.write_guard.lock().await;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT * FROM tasks WHERE task_id = ?")
            .bind(transition.task_id.to_string())
            .fetch_optional(&mut *tx).await?
            .ok_or(StoreError::NotFound(transition.task_id))?;
        let current = snapshot_from_row(&row)?;
        if current.revision != transition.expected_revision {
            return Err(StoreError::Conflict);
        }
        if !transition.allowed_states.contains(&current.state) {
            return Err(StoreError::InvalidTransition {
                current: current.state,
                allowed: transition.allowed_states,
            });
        }

        let now = Utc::now();
        let next_seq = current.last_seq + 1;
        let next_revision = current.revision + 1;
        let terminal_at = transition.next_state.terminal().then(|| now.to_rfc3339());
        let payload_json = serde_json::to_string(&transition.event_payload)?;

        // Revision in WHERE protects this transaction if a future storage
        // implementation bypasses the process-local write guard.
        let updated = sqlx::query(r#"
            UPDATE tasks
            SET state = ?, revision = ?, last_seq = ?, updated_at = ?,
                terminal_at = COALESCE(terminal_at, ?)
            WHERE task_id = ? AND revision = ?
        "#)
            .bind(transition.next_state.as_db())
            .bind(next_revision as i64)
            .bind(next_seq as i64)
            .bind(now.to_rfc3339())
            .bind(terminal_at)
            .bind(transition.task_id.to_string())
            .bind(current.revision as i64)
            .execute(&mut *tx).await?
            .rows_affected();
        if updated != 1 {
            return Err(StoreError::Conflict);
        }
        sqlx::query(r#"
            INSERT INTO task_events(task_id, seq, event_type, payload_json, created_at)
            VALUES(?, ?, ?, ?, ?)
        "#)
            .bind(transition.task_id.to_string())
            .bind(next_seq as i64)
            .bind(&transition.event_type)
            .bind(payload_json)
            .bind(now.to_rfc3339())
            .execute(&mut *tx).await?;

        let row = sqlx::query("SELECT * FROM tasks WHERE task_id = ?")
            .bind(transition.task_id.to_string()).fetch_one(&mut *tx).await?;
        let snapshot = snapshot_from_row(&row)?;
        let event = StoredEvent {
            task_id: transition.task_id,
            seq: next_seq,
            event_type: transition.event_type,
            payload: transition.event_payload,
            created_at: now,
        };
        tx.commit().await?;
        Ok(AppliedTransition { snapshot, event })
    }

    pub async fn events_after(
        &self,
        task_id: TaskId,
        after_seq: EventSeq,
        limit: u32,
    ) -> Result<Vec<StoredEvent>, StoreError> {
        let rows = sqlx::query(r#"
            SELECT task_id, seq, event_type, payload_json, created_at
            FROM task_events
            WHERE task_id = ? AND seq > ?
            ORDER BY seq ASC
            LIMIT ?
        "#)
            .bind(task_id.to_string())
            .bind(after_seq as i64)
            .bind(limit.min(10_000) as i64)
            .fetch_all(&self.pool).await?;
        rows.iter().map(event_from_row).collect()
    }

    pub async fn acquire_lease(
        &self,
        task_id: TaskId,
        owner_id: &str,
        ttl: Duration,
    ) -> Result<bool, StoreError> {
        let _guard = self.write_guard.lock().await;
        let now = Utc::now();
        let expires = now + chrono::Duration::from_std(ttl)
            .map_err(|error| StoreError::Corrupt(error.to_string()))?;
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(r#"
            INSERT INTO task_leases(task_id, owner_id, expires_at, updated_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
              owner_id = excluded.owner_id,
              expires_at = excluded.expires_at,
              updated_at = excluded.updated_at
            WHERE task_leases.expires_at < ? OR task_leases.owner_id = excluded.owner_id
        "#)
            .bind(task_id.to_string())
            .bind(owner_id)
            .bind(expires.to_rfc3339())
            .bind(now.to_rfc3339())
            .bind(now.to_rfc3339())
            .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn renew_lease(
        &self,
        task_id: TaskId,
        owner_id: &str,
        ttl: Duration,
    ) -> Result<bool, StoreError> {
        let _guard = self.write_guard.lock().await;
        let now = Utc::now();
        let expires = now + chrono::Duration::from_std(ttl)
            .map_err(|error| StoreError::Corrupt(error.to_string()))?;
        let updated = sqlx::query(r#"
            UPDATE task_leases SET expires_at = ?, updated_at = ?
            WHERE task_id = ? AND owner_id = ? AND expires_at >= ?
        "#)
            .bind(expires.to_rfc3339())
            .bind(now.to_rfc3339())
            .bind(task_id.to_string())
            .bind(owner_id)
            .bind(now.to_rfc3339())
            .execute(&self.pool).await?.rows_affected();
        Ok(updated == 1)
    }

    pub async fn cleanup(&self, policy: &RetentionPolicy) -> Result<CleanupReport, StoreError> {
        let _guard = self.write_guard.lock().await;
        let now = Utc::now();
        let task_cutoff = now - chrono::Duration::from_std(policy.task_ttl)
            .map_err(|error| StoreError::Corrupt(error.to_string()))?;
        let event_cutoff = now - chrono::Duration::from_std(policy.event_ttl)
            .map_err(|error| StoreError::Corrupt(error.to_string()))?;
        let key_cutoff = now - chrono::Duration::from_std(policy.idempotency_ttl)
            .map_err(|error| StoreError::Corrupt(error.to_string()))?;
        let mut tx = self.pool.begin().await?;

        // First remove old non-terminal-independent events. Terminal task
        // deletion below cascades any remaining events through FK.
        let events_deleted = sqlx::query(r#"
            DELETE FROM task_events WHERE rowid IN (
              SELECT rowid FROM task_events WHERE created_at < ? LIMIT ?
            )
        "#).bind(event_cutoff.to_rfc3339()).bind(policy.cleanup_batch_size as i64)
            .execute(&mut *tx).await?.rows_affected();

        // Idempotency is represented by tasks in this MVP schema. A terminal
        // task becomes deletable only after both task and idempotency TTLs.
        let effective_cutoff = std::cmp::min(task_cutoff, key_cutoff);
        let tasks_deleted = sqlx::query(r#"
            DELETE FROM tasks WHERE task_id IN (
              SELECT task_id FROM tasks
              WHERE terminal_at IS NOT NULL AND terminal_at < ?
              LIMIT ?
            )
        "#).bind(effective_cutoff.to_rfc3339()).bind(policy.cleanup_batch_size as i64)
            .execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(CleanupReport { tasks_deleted, events_deleted, idempotency_deleted: tasks_deleted })
    }

    /// WAL file can grow if long-lived readers prevent checkpoints. This method
    /// is intentionally called by a maintenance job, never after every write.
    pub async fn checkpoint_passive(&self) -> Result<(), StoreError> {
        sqlx::query("PRAGMA wal_checkpoint(PASSIVE)").execute(&self.pool).await?;
        Ok(())
    }
}

fn snapshot_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<TaskSnapshot, StoreError> {
    let parse_uuid = |column: &str| -> Result<Uuid, StoreError> {
        row.try_get::<String, _>(column)?.parse()
            .map_err(|error| StoreError::Corrupt(format!("{column}: {error}")))
    };
    let parse_time = |column: &str| -> Result<DateTime<Utc>, StoreError> {
        row.try_get::<String, _>(column)?.parse()
            .map_err(|error| StoreError::Corrupt(format!("{column}: {error}")))
    };
    let session_raw: Option<String> = row.try_get("session_id")?;
    let terminal_raw: Option<String> = row.try_get("terminal_at")?;
    Ok(TaskSnapshot {
        task_id: parse_uuid("task_id")?,
        session_id: session_raw.map(|value| value.parse()
            .map_err(|error| StoreError::Corrupt(format!("session_id: {error}")))).transpose()?,
        agent_id: row.try_get("agent_id")?,
        caller_id: row.try_get("caller_id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        state: TaskState::from_db(&row.try_get::<String, _>("state")?)?,
        revision: row.try_get::<i64, _>("revision")? as u64,
        last_seq: row.try_get::<i64, _>("last_seq")? as u64,
        created_at: parse_time("created_at")?,
        updated_at: parse_time("updated_at")?,
        terminal_at: terminal_raw.map(|value| value.parse()
            .map_err(|error| StoreError::Corrupt(format!("terminal_at: {error}")))).transpose()?,
    })
}

fn event_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<StoredEvent, StoreError> {
    let task_id: String = row.try_get("task_id")?;
    let created_at: String = row.try_get("created_at")?;
    let payload_json: String = row.try_get("payload_json")?;
    Ok(StoredEvent {
        task_id: task_id.parse().map_err(|error| StoreError::Corrupt(format!("task_id: {error}")))?,
        seq: row.try_get::<i64, _>("seq")? as u64,
        event_type: row.try_get("event_type")?,
        payload: serde_json::from_str(&payload_json)?,
        created_at: created_at.parse().map_err(|error| StoreError::Corrupt(format!("created_at: {error}")))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> SqliteTaskStore {
        let options = SqliteConnectOptions::new().filename(":memory:").foreign_keys(true);
        let pool = SqlitePoolOptions::new().max_connections(1).connect_with(options).await.unwrap();
        let store = SqliteTaskStore { pool, write_guard: Arc::new(Mutex::new(())) };
        store.migrate().await.unwrap();
        store
    }

    fn new_task() -> NewTask {
        NewTask {
            task_id: Uuid::new_v4(),
            session_id: None,
            agent_id: "reviewer".into(),
            caller_id: "alice".into(),
            idempotency_key: "key-1".into(),
            deadline_at: None,
        }
    }

    #[tokio::test]
    async fn idempotency_returns_original_task() {
        let store = store().await;
        let task = new_task();
        let id = task.task_id;
        assert!(matches!(store.create_or_get_idempotent(task.clone()).await.unwrap(), CreateTaskResult::Created(_)));
        let mut duplicate = task;
        duplicate.task_id = Uuid::new_v4();
        match store.create_or_get_idempotent(duplicate).await.unwrap() {
            CreateTaskResult::Existing(snapshot) => assert_eq!(snapshot.task_id, id),
            _ => panic!("expected existing task"),
        }
    }

    #[tokio::test]
    async fn transition_is_atomic_and_replayable() {
        let store = store().await;
        let task = new_task();
        let snapshot = match store.create_or_get_idempotent(task).await.unwrap() {
            CreateTaskResult::Created(snapshot) => snapshot,
            _ => panic!(),
        };
        let applied = store.append_event_and_transition(Transition {
            task_id: snapshot.task_id,
            expected_revision: 0,
            allowed_states: vec![TaskState::Created],
            next_state: TaskState::Accepted,
            event_type: "accepted".into(),
            event_payload: serde_json::json!({"queued": false}),
        }).await.unwrap();
        assert_eq!(applied.snapshot.state, TaskState::Accepted);
        assert_eq!(applied.event.seq, 1);
        let events = store.events_after(snapshot.task_id, 0, 10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "accepted");
    }
}
