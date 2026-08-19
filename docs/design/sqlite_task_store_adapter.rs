//! `storage-sqlite` integration with adapter-model + TaskStore.
//!
//! Replaces the duplicated DTO layer in the earlier `storage_sqlite.rs` draft.
//! This is the implementation AdapterCore v2 consumes through Arc<dyn TaskStore>.

use std::{path::Path, sync::Arc, time::Duration};

use adapter_model::{
    AppliedTransition, CoreEvent, CoreEventKind, CreateTaskResult, EventSeq,
    NewTask, TaskId, TaskSnapshot, TaskState, TaskTransition,
};
use adapter_store_contract::{CleanupReport, Lease, RetentionPolicy, StoreError, TaskStore};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous}, Row, SqlitePool};
use tokio::sync::Mutex;

pub struct SqliteTaskStore {
    pool: SqlitePool,
    write_guard: Arc<Mutex<()>>,
}

impl SqliteTaskStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new().filename(path).create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal).synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5)).foreign_keys(true);
        let pool = SqlitePoolOptions::new().max_connections(8).min_connections(1)
            .connect_with(options).await.map_err(db_error)?;
        let store = Self { pool, write_guard: Arc::new(Mutex::new(())) };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<(), StoreError> {
        let _guard = self.write_guard.lock().await;
        for sql in [
            "PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL", "PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=5000",
            "CREATE TABLE IF NOT EXISTS tasks (task_id TEXT PRIMARY KEY, session_id TEXT, agent_id TEXT NOT NULL, caller_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, state TEXT NOT NULL, revision INTEGER NOT NULL, last_seq INTEGER NOT NULL, deadline_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, terminal_at TEXT, UNIQUE(caller_id,idempotency_key))",
            "CREATE TABLE IF NOT EXISTS task_events (task_id TEXT NOT NULL, seq INTEGER NOT NULL, event_kind_json TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(task_id,seq), FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE)",
            "CREATE INDEX IF NOT EXISTS ix_task_events_replay ON task_events(task_id,seq)",
            "CREATE TABLE IF NOT EXISTS task_leases (task_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, expires_at TEXT NOT NULL, updated_at TEXT NOT NULL, FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE)",
        ] { sqlx::query(sql).execute(&self.pool).await.map_err(db_error)?; }
        Ok(())
    }

    pub async fn checkpoint_passive(&self) -> Result<(), StoreError> {
        sqlx::query("PRAGMA wal_checkpoint(PASSIVE)").execute(&self.pool).await.map_err(db_error)?;
        Ok(())
    }
}

#[async_trait]
impl TaskStore for SqliteTaskStore {
    async fn create_or_get_idempotent(&self, task: NewTask) -> Result<CreateTaskResult, StoreError> {
        let _guard = self.write_guard.lock().await;
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        if let Some(row) = sqlx::query("SELECT * FROM tasks WHERE caller_id=? AND idempotency_key=?")
            .bind(&task.caller_id.0).bind(&task.idempotency_key).fetch_optional(&mut *tx).await.map_err(db_error)? {
            let snapshot = snapshot(&row)?; tx.commit().await.map_err(db_error)?; return Ok(CreateTaskResult::Existing(snapshot));
        }
        let now = Utc::now();
        sqlx::query("INSERT INTO tasks(task_id,session_id,agent_id,caller_id,idempotency_key,state,revision,last_seq,deadline_at,created_at,updated_at) VALUES(?,?,?,?,?,'created',0,0,?,?,?)")
            .bind(task.task_id.to_string()).bind(task.session_id.map(|v| v.to_string())).bind(task.agent_id.0).bind(task.caller_id.0).bind(task.idempotency_key)
            .bind(task.deadline_at.map(|v| v.to_rfc3339())).bind(now.to_rfc3339()).bind(now.to_rfc3339())
            .execute(&mut *tx).await.map_err(db_error)?;
        let row = sqlx::query("SELECT * FROM tasks WHERE task_id=?").bind(task.task_id.to_string()).fetch_one(&mut *tx).await.map_err(db_error)?;
        let snapshot = snapshot(&row)?; tx.commit().await.map_err(db_error)?;
        Ok(CreateTaskResult::Created(snapshot))
    }

    async fn get_snapshot(&self, task_id: TaskId) -> Result<Option<TaskSnapshot>, StoreError> {
        sqlx::query("SELECT * FROM tasks WHERE task_id=?").bind(task_id.to_string()).fetch_optional(&self.pool).await.map_err(db_error)?
            .map(|row| snapshot(&row)).transpose()
    }

    async fn append_event_and_transition(&self, transition: TaskTransition) -> Result<AppliedTransition, StoreError> {
        let _guard = self.write_guard.lock().await;
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let row = sqlx::query("SELECT * FROM tasks WHERE task_id=?").bind(transition.task_id.to_string()).fetch_optional(&mut *tx).await.map_err(db_error)?
            .ok_or(StoreError::NotFound(transition.task_id))?;
        let current = snapshot(&row)?;
        if current.revision != transition.expected_revision { return Err(StoreError::Conflict); }
        if !transition.allowed_states.contains(&current.state) { return Err(StoreError::InvalidTransition(format!("current={:?}", current.state))); }
        let now = Utc::now(); let seq = current.last_seq + 1; let revision = current.revision + 1;
        let terminal = transition.next_state.terminal().then(|| now.to_rfc3339());
        let changed = sqlx::query("UPDATE tasks SET state=?,revision=?,last_seq=?,updated_at=?,terminal_at=COALESCE(terminal_at,?) WHERE task_id=? AND revision=?")
            .bind(state_db(transition.next_state)).bind(revision as i64).bind(seq as i64).bind(now.to_rfc3339()).bind(terminal)
            .bind(transition.task_id.to_string()).bind(current.revision as i64).execute(&mut *tx).await.map_err(db_error)?.rows_affected();
        if changed != 1 { return Err(StoreError::Conflict); }
        let kind_json = serde_json::to_string(&transition.event_kind).map_err(|e| StoreError::Internal(e.to_string()))?;
        sqlx::query("INSERT INTO task_events(task_id,seq,event_kind_json,created_at) VALUES(?,?,?,?)")
            .bind(transition.task_id.to_string()).bind(seq as i64).bind(kind_json).bind(now.to_rfc3339()).execute(&mut *tx).await.map_err(db_error)?;
        let row = sqlx::query("SELECT * FROM tasks WHERE task_id=?").bind(transition.task_id.to_string()).fetch_one(&mut *tx).await.map_err(db_error)?;
        let result = AppliedTransition { snapshot: snapshot(&row)?, event: CoreEvent { task_id: transition.task_id, seq, at: now, kind: transition.event_kind } };
        tx.commit().await.map_err(db_error)?;
        Ok(result)
    }

    async fn events_after(&self, task_id: TaskId, after_seq: EventSeq, limit: u32) -> Result<Vec<CoreEvent>, StoreError> {
        let rows = sqlx::query("SELECT seq,event_kind_json,created_at FROM task_events WHERE task_id=? AND seq>? ORDER BY seq LIMIT ?")
            .bind(task_id.to_string()).bind(after_seq as i64).bind(limit.min(10_000) as i64).fetch_all(&self.pool).await.map_err(db_error)?;
        rows.into_iter().map(|row| {
            let kind: CoreEventKind = serde_json::from_str(&row.try_get::<String,_>("event_kind_json").map_err(db_error)?).map_err(|e| StoreError::Corrupt(e.to_string()))?;
            Ok(CoreEvent { task_id, seq: row.try_get::<i64,_>("seq").map_err(db_error)? as u64, at: parse_time(&row.try_get::<String,_>("created_at").map_err(db_error)?)?, kind })
        }).collect()
    }

    async fn acquire_lease(&self, lease: Lease) -> Result<bool, StoreError> {
        let _guard = self.write_guard.lock().await; let now = Utc::now();
        let expires = now + chrono::Duration::from_std(lease.expires_in).map_err(|e| StoreError::Internal(e.to_string()))?;
        let changed = sqlx::query("INSERT INTO task_leases(task_id,owner_id,expires_at,updated_at) VALUES(?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET owner_id=excluded.owner_id,expires_at=excluded.expires_at,updated_at=excluded.updated_at WHERE task_leases.expires_at < ? OR task_leases.owner_id=excluded.owner_id")
            .bind(lease.task_id.to_string()).bind(lease.owner_id).bind(expires.to_rfc3339()).bind(now.to_rfc3339()).bind(now.to_rfc3339()).execute(&self.pool).await.map_err(db_error)?.rows_affected();
        Ok(changed == 1)
    }

    async fn renew_lease(&self, lease: Lease) -> Result<bool, StoreError> {
        let _guard = self.write_guard.lock().await; let now = Utc::now();
        let expires = now + chrono::Duration::from_std(lease.expires_in).map_err(|e| StoreError::Internal(e.to_string()))?;
        let changed = sqlx::query("UPDATE task_leases SET expires_at=?,updated_at=? WHERE task_id=? AND owner_id=? AND expires_at>=?")
            .bind(expires.to_rfc3339()).bind(now.to_rfc3339()).bind(lease.task_id.to_string()).bind(lease.owner_id).bind(now.to_rfc3339()).execute(&self.pool).await.map_err(db_error)?.rows_affected();
        Ok(changed == 1)
    }

    async fn cleanup(&self, policy: &RetentionPolicy) -> Result<CleanupReport, StoreError> {
        let _guard = self.write_guard.lock().await; let now = Utc::now();
        let task_cutoff = now - chrono::Duration::from_std(policy.task_ttl).map_err(|e| StoreError::Internal(e.to_string()))?;
        let event_cutoff = now - chrono::Duration::from_std(policy.event_ttl).map_err(|e| StoreError::Internal(e.to_string()))?;
        let events = sqlx::query("DELETE FROM task_events WHERE rowid IN (SELECT rowid FROM task_events WHERE created_at<? LIMIT ?)").bind(event_cutoff.to_rfc3339()).bind(policy.cleanup_batch_size as i64).execute(&self.pool).await.map_err(db_error)?.rows_affected();
        let tasks = sqlx::query("DELETE FROM tasks WHERE task_id IN (SELECT task_id FROM tasks WHERE terminal_at IS NOT NULL AND terminal_at<? LIMIT ?)").bind(task_cutoff.to_rfc3339()).bind(policy.cleanup_batch_size as i64).execute(&self.pool).await.map_err(db_error)?.rows_affected();
        Ok(CleanupReport { tasks_deleted: tasks, events_deleted: events, idempotency_deleted: tasks })
    }
}

fn db_error(error: sqlx::Error) -> StoreError { StoreError::Unavailable(error.to_string()) }
fn parse_time(value: &str) -> Result<DateTime<Utc>, StoreError> { value.parse().map_err(|e| StoreError::Corrupt(format!("time: {e}"))) }
fn state_db(state: TaskState) -> &'static str { match state { TaskState::Created=>"created",TaskState::Accepted=>"accepted",TaskState::Running=>"running",TaskState::WaitingForInput=>"waiting_for_input",TaskState::CancelRequested=>"cancel_requested",TaskState::Completed=>"completed",TaskState::Failed=>"failed",TaskState::Cancelled=>"cancelled" } }
fn state(value: &str) -> Result<TaskState, StoreError> { match value { "created"=>Ok(TaskState::Created),"accepted"=>Ok(TaskState::Accepted),"running"=>Ok(TaskState::Running),"waiting_for_input"=>Ok(TaskState::WaitingForInput),"cancel_requested"=>Ok(TaskState::CancelRequested),"completed"=>Ok(TaskState::Completed),"failed"=>Ok(TaskState::Failed),"cancelled"=>Ok(TaskState::Cancelled),_=>Err(StoreError::Corrupt(format!("state={value}"))) } }
fn snapshot(row: &sqlx::sqlite::SqliteRow) -> Result<TaskSnapshot, StoreError> {
    let id: String=row.try_get("task_id").map_err(db_error)?; let session:Option<String>=row.try_get("session_id").map_err(db_error)?; let terminal:Option<String>=row.try_get("terminal_at").map_err(db_error)?;
    Ok(TaskSnapshot { task_id:id.parse().map_err(|e|StoreError::Corrupt(format!("id:{e}")))?, session_id:session.map(|v|v.parse().map_err(|e|StoreError::Corrupt(format!("session:{e}")))).transpose()?, agent_id:adapter_model::AgentId(row.try_get("agent_id").map_err(db_error)?), caller_id:adapter_model::CallerId(row.try_get("caller_id").map_err(db_error)?), state:state(&row.try_get::<String,_>("state").map_err(db_error)?)?, revision:row.try_get::<i64,_>("revision").map_err(db_error)? as u64, last_seq:row.try_get::<i64,_>("last_seq").map_err(db_error)? as u64, created_at:parse_time(&row.try_get::<String,_>("created_at").map_err(db_error)?)?, updated_at:parse_time(&row.try_get::<String,_>("updated_at").map_err(db_error)?)?, terminal_at:terminal.map(|v|parse_time(&v)).transpose()? })
}
