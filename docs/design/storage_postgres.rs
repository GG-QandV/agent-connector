//! `storage-postgres` — durable multi-instance task/event store for Adapter Core.
//!
//! Use this backend only when one SQLite-backed Adapter Daemon is no longer
//! enough: multiple adapter instances, distributed task ownership, HA, or high
//! concurrent write volume. The runtime selects this backend via installer/config;
//! it never installs or manages PostgreSQL containers itself.
//!
//! Cargo.toml dependencies:
//! chrono = { version = "0.4", features = ["serde"] }
//! serde = { version = "1", features = ["derive"] }
//! serde_json = "1"
//! sqlx = { version = "0.8", features = ["runtime-tokio-rustls", "postgres", "chrono", "uuid"] }
//! thiserror = "2"
//! tokio = { version = "1", features = ["sync"] }
//! uuid = { version = "1", features = ["serde"] }
//!
//! This file intentionally mirrors `storage_sqlite.rs` domain DTOs. During
//! workspace integration they should move to `adapter-model`, and both stores
//! should implement the single `TaskStore` trait from `adapter-core`.

use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::{PgConnectOptions, PgPoolOptions}, PgPool, Postgres, Row, Transaction};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

pub type TaskId = Uuid;
pub type SessionId = Uuid;
pub type EventSeq = u64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    Created, Accepted, Running, WaitingForInput, CancelRequested, Completed, Failed, Cancelled,
}

impl TaskState {
    pub fn terminal(&self) -> bool { matches!(self, Self::Completed | Self::Failed | Self::Cancelled) }
    fn as_db(&self) -> &'static str {
        match self {
            Self::Created => "created", Self::Accepted => "accepted", Self::Running => "running",
            Self::WaitingForInput => "waiting_for_input", Self::CancelRequested => "cancel_requested",
            Self::Completed => "completed", Self::Failed => "failed", Self::Cancelled => "cancelled",
        }
    }
    fn from_db(value: &str) -> Result<Self, StoreError> {
        match value {
            "created" => Ok(Self::Created), "accepted" => Ok(Self::Accepted), "running" => Ok(Self::Running),
            "waiting_for_input" => Ok(Self::WaitingForInput), "cancel_requested" => Ok(Self::CancelRequested),
            "completed" => Ok(Self::Completed), "failed" => Ok(Self::Failed), "cancelled" => Ok(Self::Cancelled),
            value => Err(StoreError::Corrupt(format!("unknown task state: {value}"))),
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
pub enum CreateTaskResult { Created(TaskSnapshot), Existing(TaskSnapshot) }

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
pub struct AppliedTransition { pub snapshot: TaskSnapshot, pub event: StoredEvent }

#[derive(Clone, Debug)]
pub struct RetentionPolicy {
    pub task_ttl: Duration,
    pub event_ttl: Duration,
    pub cleanup_batch_size: i64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self { task_ttl: Duration::from_secs(7 * 24 * 3600), event_ttl: Duration::from_secs(7 * 24 * 3600), cleanup_batch_size: 10_000 }
    }
}

#[derive(Clone, Debug)]
pub struct CleanupReport { pub tasks_deleted: u64, pub events_deleted: u64 }

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("task not found: {0}")]
    NotFound(TaskId),
    #[error("concurrent transition conflict")]
    Conflict,
    #[error("invalid transition: current={current:?}, allowed={allowed:?}")]
    InvalidTransition { current: TaskState, allowed: Vec<TaskState> },
    #[error("database data is corrupt: {0}")]
    Corrupt(String),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid postgres schema: {0}")]
    InvalidSchema(String),
}

/// Per-process transaction gate is optional for PostgreSQL correctness because
/// row locks and optimistic revision protect concurrent workers. It is used
/// only to avoid a local cleanup/migration stampede; never held over network I/O.
pub struct PostgresTaskStore {
    pool: PgPool,
    schema: String,
    maintenance_guard: Arc<Mutex<()>>,
}

impl PostgresTaskStore {
    pub async fn connect(dsn: &str, schema: impl Into<String>, max_connections: u32) -> Result<Self, StoreError> {
        let schema = validate_schema(schema.into())?;
        let options: PgConnectOptions = dsn.parse()
            .map_err(|error| StoreError::Corrupt(format!("invalid postgres DSN: {error}")))?;
        let pool = PgPoolOptions::new().max_connections(max_connections).connect_with(options).await?;
        let store = Self { pool, schema, maintenance_guard: Arc::new(Mutex::new(())) };
        store.migrate().await?;
        Ok(store)
    }

    pub fn pool(&self) -> &PgPool { &self.pool }
    pub fn schema(&self) -> &str { &self.schema }

    fn table(&self, name: &str) -> String { format!("\"{}\".\"{}\"", self.schema, name) }

    pub async fn migrate(&self) -> Result<(), StoreError> {
        let _guard = self.maintenance_guard.lock().await;
        let schema = format!("\"{}\"", self.schema);
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}")).execute(&self.pool).await?;
        sqlx::query(&format!(r#"
            CREATE TABLE IF NOT EXISTS {schema}.schema_migrations (
                version BIGINT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL
            )
        "#)).execute(&self.pool).await?;
        let migrations = self.table("schema_migrations");
        let version: i64 = sqlx::query_scalar(&format!("SELECT COALESCE(MAX(version), 0) FROM {migrations}"))
            .fetch_one(&self.pool).await?;
        if version < 1 {
            let mut tx = self.pool.begin().await?;
            let tasks = self.table("tasks");
            let events = self.table("task_events");
            let leases = self.table("task_leases");
            sqlx::query(&format!(r#"
                CREATE TABLE {tasks} (
                    task_id UUID PRIMARY KEY,
                    session_id UUID NULL,
                    agent_id TEXT NOT NULL,
                    caller_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    state TEXT NOT NULL,
                    revision BIGINT NOT NULL DEFAULT 0,
                    last_seq BIGINT NOT NULL DEFAULT 0,
                    deadline_at TIMESTAMPTZ NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    terminal_at TIMESTAMPTZ NULL,
                    UNIQUE(caller_id, idempotency_key)
                )
            "#)).execute(&mut *tx).await?;
            sqlx::query(&format!("CREATE INDEX ix_tasks_active ON {tasks}(state, updated_at)")).execute(&mut *tx).await?;
            sqlx::query(&format!("CREATE INDEX ix_tasks_session ON {tasks}(session_id, updated_at)")).execute(&mut *tx).await?;
            sqlx::query(&format!(r#"
                CREATE TABLE {events} (
                    task_id UUID NOT NULL REFERENCES {tasks}(task_id) ON DELETE CASCADE,
                    seq BIGINT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY(task_id, seq)
                )
            "#)).execute(&mut *tx).await?;
            sqlx::query(&format!("CREATE INDEX ix_task_events_retention ON {events}(created_at)")).execute(&mut *tx).await?;
            sqlx::query(&format!(r#"
                CREATE TABLE {leases} (
                    task_id UUID PRIMARY KEY REFERENCES {tasks}(task_id) ON DELETE CASCADE,
                    owner_id TEXT NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL
                )
            "#)).execute(&mut *tx).await?;
            sqlx::query(&format!("INSERT INTO {migrations}(version, applied_at) VALUES(1, NOW())"))
                .execute(&mut *tx).await?;
            tx.commit().await?;
        }
        Ok(())
    }

    /// PostgreSQL unique constraint provides cross-instance idempotency.
    pub async fn create_or_get_idempotent(&self, task: NewTask) -> Result<CreateTaskResult, StoreError> {
        let tasks = self.table("tasks");
        let now = Utc::now();
        let inserted = sqlx::query(&format!(r#"
            INSERT INTO {tasks}(
              task_id, session_id, agent_id, caller_id, idempotency_key,
              state, revision, last_seq, deadline_at, created_at, updated_at
            ) VALUES($1, $2, $3, $4, $5, 'created', 0, 0, $6, $7, $7)
            ON CONFLICT(caller_id, idempotency_key) DO NOTHING
        "#))
            .bind(task.task_id).bind(task.session_id).bind(&task.agent_id).bind(&task.caller_id)
            .bind(&task.idempotency_key).bind(task.deadline_at).bind(now)
            .execute(&self.pool).await?;
        let row = sqlx::query(&format!("SELECT * FROM {tasks} WHERE caller_id = $1 AND idempotency_key = $2"))
            .bind(&task.caller_id).bind(&task.idempotency_key).fetch_one(&self.pool).await?;
        let snapshot = snapshot_from_row(&row)?;
        Ok(if inserted.rows_affected() == 1 { CreateTaskResult::Created(snapshot) } else { CreateTaskResult::Existing(snapshot) })
    }

    pub async fn get_snapshot(&self, task_id: TaskId) -> Result<Option<TaskSnapshot>, StoreError> {
        let tasks = self.table("tasks");
        let row = sqlx::query(&format!("SELECT * FROM {tasks} WHERE task_id = $1"))
            .bind(task_id).fetch_optional(&self.pool).await?;
        row.map(|row| snapshot_from_row(&row)).transpose()
    }

    /// Uses row lock + expected revision. The event is inserted and task state
    /// updated in one transaction, then committed before the core publishes it.
    pub async fn append_event_and_transition(&self, transition: Transition) -> Result<AppliedTransition, StoreError> {
        let mut tx = self.pool.begin().await?;
        let tasks = self.table("tasks");
        let events = self.table("task_events");
        let row = sqlx::query(&format!("SELECT * FROM {tasks} WHERE task_id = $1 FOR UPDATE"))
            .bind(transition.task_id).fetch_optional(&mut *tx).await?
            .ok_or(StoreError::NotFound(transition.task_id))?;
        let current = snapshot_from_row(&row)?;
        if current.revision != transition.expected_revision { return Err(StoreError::Conflict); }
        if !transition.allowed_states.contains(&current.state) {
            return Err(StoreError::InvalidTransition { current: current.state, allowed: transition.allowed_states });
        }
        let now = Utc::now();
        let next_seq = current.last_seq + 1;
        let next_revision = current.revision + 1;
        let terminal_at = transition.next_state.terminal().then_some(now);
        let payload = serde_json::to_value(&transition.event_payload)?;
        let affected = sqlx::query(&format!(r#"
            UPDATE {tasks}
            SET state = $1, revision = $2, last_seq = $3, updated_at = $4,
                terminal_at = COALESCE(terminal_at, $5)
            WHERE task_id = $6 AND revision = $7
        "#))
            .bind(transition.next_state.as_db()).bind(next_revision as i64).bind(next_seq as i64)
            .bind(now).bind(terminal_at).bind(transition.task_id).bind(current.revision as i64)
            .execute(&mut *tx).await?.rows_affected();
        if affected != 1 { return Err(StoreError::Conflict); }
        sqlx::query(&format!(r#"
            INSERT INTO {events}(task_id, seq, event_type, payload_json, created_at)
            VALUES($1, $2, $3, $4, $5)
        "#))
            .bind(transition.task_id).bind(next_seq as i64).bind(&transition.event_type).bind(payload).bind(now)
            .execute(&mut *tx).await?;
        let row = sqlx::query(&format!("SELECT * FROM {tasks} WHERE task_id = $1"))
            .bind(transition.task_id).fetch_one(&mut *tx).await?;
        let snapshot = snapshot_from_row(&row)?;
        let event = StoredEvent { task_id: transition.task_id, seq: next_seq, event_type: transition.event_type, payload: transition.event_payload, created_at: now };
        tx.commit().await?;
        Ok(AppliedTransition { snapshot, event })
    }

    pub async fn events_after(&self, task_id: TaskId, after_seq: EventSeq, limit: i64) -> Result<Vec<StoredEvent>, StoreError> {
        let events = self.table("task_events");
        let rows = sqlx::query(&format!(r#"
            SELECT task_id, seq, event_type, payload_json, created_at
            FROM {events} WHERE task_id = $1 AND seq > $2
            ORDER BY seq ASC LIMIT $3
        "#))
            .bind(task_id).bind(after_seq as i64).bind(limit.clamp(1, 10_000))
            .fetch_all(&self.pool).await?;
        rows.iter().map(event_from_row).collect()
    }

    /// Claim ownership if lease is absent/expired, or renew if same owner.
    /// This permits another adapter instance to recover abandoned work after a crash.
    pub async fn acquire_lease(&self, task_id: TaskId, owner_id: &str, ttl: Duration) -> Result<bool, StoreError> {
        let leases = self.table("task_leases");
        let ttl_seconds = i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX);
        let updated = sqlx::query(&format!(r#"
            INSERT INTO {leases}(task_id, owner_id, expires_at, updated_at)
            VALUES($1, $2, NOW() + ($3 * INTERVAL '1 second'), NOW())
            ON CONFLICT(task_id) DO UPDATE SET
              owner_id = EXCLUDED.owner_id,
              expires_at = EXCLUDED.expires_at,
              updated_at = NOW()
            WHERE {leases}.expires_at < NOW() OR {leases}.owner_id = EXCLUDED.owner_id
        "#))
            .bind(task_id).bind(owner_id).bind(ttl_seconds)
            .execute(&self.pool).await?.rows_affected();
        Ok(updated == 1)
    }

    pub async fn renew_lease(&self, task_id: TaskId, owner_id: &str, ttl: Duration) -> Result<bool, StoreError> {
        let leases = self.table("task_leases");
        let ttl_seconds = i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX);
        let updated = sqlx::query(&format!(r#"
            UPDATE {leases}
            SET expires_at = NOW() + ($1 * INTERVAL '1 second'), updated_at = NOW()
            WHERE task_id = $2 AND owner_id = $3 AND expires_at >= NOW()
        "#)).bind(ttl_seconds).bind(task_id).bind(owner_id)
            .execute(&self.pool).await?.rows_affected();
        Ok(updated == 1)
    }

    pub async fn cleanup(&self, policy: &RetentionPolicy) -> Result<CleanupReport, StoreError> {
        let _guard = self.maintenance_guard.lock().await;
        let mut tx = self.pool.begin().await?;
        let tasks = self.table("tasks");
        let events = self.table("task_events");
        let event_ttl_seconds = i64::try_from(policy.event_ttl.as_secs()).unwrap_or(i64::MAX);
        let task_ttl_seconds = i64::try_from(policy.task_ttl.as_secs()).unwrap_or(i64::MAX);
        let events_deleted = sqlx::query(&format!(r#"
            DELETE FROM {events} WHERE ctid IN (
                SELECT ctid FROM {events}
                WHERE created_at < NOW() - ($1 * INTERVAL '1 second')
                LIMIT $2
            )
        "#)).bind(event_ttl_seconds).bind(policy.cleanup_batch_size).execute(&mut *tx).await?.rows_affected();
        let tasks_deleted = sqlx::query(&format!(r#"
            DELETE FROM {tasks} WHERE ctid IN (
                SELECT ctid FROM {tasks}
                WHERE terminal_at IS NOT NULL
                  AND terminal_at < NOW() - ($1 * INTERVAL '1 second')
                LIMIT $2
            )
        "#)).bind(task_ttl_seconds).bind(policy.cleanup_batch_size).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(CleanupReport { tasks_deleted, events_deleted })
    }
}

fn validate_schema(schema: String) -> Result<String, StoreError> {
    if schema.is_empty() || !schema.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(StoreError::InvalidSchema(schema));
    }
    Ok(schema)
}

fn snapshot_from_row(row: &sqlx::postgres::PgRow) -> Result<TaskSnapshot, StoreError> {
    Ok(TaskSnapshot {
        task_id: row.try_get("task_id")?, session_id: row.try_get("session_id")?,
        agent_id: row.try_get("agent_id")?, caller_id: row.try_get("caller_id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        state: TaskState::from_db(&row.try_get::<String, _>("state")?)?,
        revision: row.try_get::<i64, _>("revision")? as u64,
        last_seq: row.try_get::<i64, _>("last_seq")? as u64,
        created_at: row.try_get("created_at")?, updated_at: row.try_get("updated_at")?,
        terminal_at: row.try_get("terminal_at")?,
    })
}

fn event_from_row(row: &sqlx::postgres::PgRow) -> Result<StoredEvent, StoreError> {
    Ok(StoredEvent {
        task_id: row.try_get("task_id")?, seq: row.try_get::<i64, _>("seq")? as u64,
        event_type: row.try_get("event_type")?, payload: row.try_get("payload_json")?,
        created_at: row.try_get("created_at")?,
    })
}
