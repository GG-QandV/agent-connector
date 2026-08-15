//! `storage-postgres` integration with adapter-model + TaskStore.
//!
//! Multi-instance/HA backend. Runtime does not create Postgres: installer or
//! operator supplies DSN and schema. Each adapter installation uses a dedicated
//! database or a validated dedicated schema, never `public` by default.

use adapter_model::{
    AppliedTransition, CoreEvent, CoreEventKind, CreateTaskResult, EventSeq, NewTask, TaskId,
    TaskSnapshot, TaskState, TaskTransition,
};
use adapter_store_contract::{CleanupReport, Lease, RetentionPolicy, StoreError, TaskStore};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct PostgresTaskStore {
    pool: PgPool,
    schema: String,
    migration_guard: Arc<Mutex<()>>,
}

impl PostgresTaskStore {
    pub async fn connect(
        dsn: &str,
        schema: impl Into<String>,
        max_connections: u32,
    ) -> Result<Self, StoreError> {
        let schema = validate_schema(schema.into())?;
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(dsn)
            .await
            .map_err(db_error)?;
        let store = Self {
            pool,
            schema,
            migration_guard: Arc::new(Mutex::new(())),
        };
        store.migrate().await?;
        Ok(store)
    }
    fn table(&self, name: &str) -> String {
        format!("\"{}\".\"{}\"", self.schema, name)
    }
    async fn migrate(&self) -> Result<(), StoreError> {
        let _guard = self.migration_guard.lock().await;
        let s = format!("\"{}\"", self.schema);
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {s}"))
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        let tasks = self.table("tasks");
        let events = self.table("task_events");
        let leases = self.table("task_leases");
        for sql in [
            format!("CREATE TABLE IF NOT EXISTS {tasks} (task_id UUID PRIMARY KEY,session_id UUID,agent_id TEXT NOT NULL,caller_id TEXT NOT NULL,idempotency_key TEXT NOT NULL,state TEXT NOT NULL,revision BIGINT NOT NULL,last_seq BIGINT NOT NULL,deadline_at TIMESTAMPTZ,created_at TIMESTAMPTZ NOT NULL,updated_at TIMESTAMPTZ NOT NULL,terminal_at TIMESTAMPTZ,UNIQUE(caller_id,idempotency_key))"),
            format!("CREATE TABLE IF NOT EXISTS {events} (task_id UUID NOT NULL REFERENCES {tasks}(task_id) ON DELETE CASCADE,seq BIGINT NOT NULL,event_kind_json JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL,PRIMARY KEY(task_id,seq))"),
            format!("CREATE INDEX IF NOT EXISTS ix_task_events_replay ON {events}(task_id,seq)"),
            format!("CREATE TABLE IF NOT EXISTS {leases} (task_id UUID PRIMARY KEY REFERENCES {tasks}(task_id) ON DELETE CASCADE,owner_id TEXT NOT NULL,expires_at TIMESTAMPTZ NOT NULL,updated_at TIMESTAMPTZ NOT NULL)"),
        ] { sqlx::query(&sql).execute(&self.pool).await.map_err(db_error)?; }
        Ok(())
    }
}

#[async_trait]
impl TaskStore for PostgresTaskStore {
    async fn ping(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        Ok(())
    }

    async fn create_or_get_idempotent(
        &self,
        task: NewTask,
    ) -> Result<CreateTaskResult, StoreError> {
        let tasks = self.table("tasks");
        let now = Utc::now();
        let inserted=sqlx::query(&format!("INSERT INTO {tasks}(task_id,session_id,agent_id,caller_id,idempotency_key,state,revision,last_seq,deadline_at,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'created',0,0,$6,$7,$7) ON CONFLICT(caller_id,idempotency_key) DO NOTHING"))
            .bind(task.task_id).bind(task.session_id).bind(&task.agent_id.0).bind(&task.caller_id.0).bind(&task.idempotency_key).bind(task.deadline_at).bind(now).execute(&self.pool).await.map_err(db_error)?;
        let row = sqlx::query(&format!(
            "SELECT * FROM {tasks} WHERE caller_id=$1 AND idempotency_key=$2"
        ))
        .bind(&task.caller_id.0)
        .bind(&task.idempotency_key)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let snapshot = snapshot(&row)?;
        Ok(if inserted.rows_affected() == 1 {
            CreateTaskResult::Created(snapshot)
        } else {
            CreateTaskResult::Existing(snapshot)
        })
    }
    async fn get_snapshot(&self, task_id: TaskId) -> Result<Option<TaskSnapshot>, StoreError> {
        let row = sqlx::query(&format!(
            "SELECT * FROM {} WHERE task_id=$1",
            self.table("tasks")
        ))
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        row.map(|r| snapshot(&r)).transpose()
    }
    async fn append_event_and_transition(
        &self,
        t: TaskTransition,
    ) -> Result<AppliedTransition, StoreError> {
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let tasks = self.table("tasks");
        let events = self.table("task_events");
        let row = sqlx::query(&format!(
            "SELECT * FROM {tasks} WHERE task_id=$1 FOR UPDATE"
        ))
        .bind(t.task_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db_error)?
        .ok_or(StoreError::NotFound(t.task_id))?;
        let current = snapshot(&row)?;
        if current.revision != t.expected_revision {
            return Err(StoreError::Conflict);
        }
        if !t.allowed_states.contains(&current.state) {
            return Err(StoreError::InvalidTransition(format!(
                "current={:?}",
                current.state
            )));
        }
        let now = Utc::now();
        let seq = current.last_seq + 1;
        let revision = current.revision + 1;
        let terminal = t.next_state.terminal().then_some(now);
        let updated=sqlx::query(&format!("UPDATE {tasks} SET state=$1,revision=$2,last_seq=$3,updated_at=$4,terminal_at=COALESCE(terminal_at,$5) WHERE task_id=$6 AND revision=$7"))
            .bind(db_state(t.next_state)).bind(revision as i64).bind(seq as i64).bind(now).bind(terminal).bind(t.task_id).bind(current.revision as i64).execute(&mut *tx).await.map_err(db_error)?.rows_affected();
        if updated != 1 {
            return Err(StoreError::Conflict);
        }
        let payload =
            serde_json::to_value(&t.event_kind).map_err(|e| StoreError::Internal(e.to_string()))?;
        sqlx::query(&format!(
            "INSERT INTO {events}(task_id,seq,event_kind_json,created_at) VALUES($1,$2,$3,$4)"
        ))
        .bind(t.task_id)
        .bind(seq as i64)
        .bind(payload)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        let row = sqlx::query(&format!("SELECT * FROM {tasks} WHERE task_id=$1"))
            .bind(t.task_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(db_error)?;
        let result = AppliedTransition {
            snapshot: snapshot(&row)?,
            event: CoreEvent {
                task_id: t.task_id,
                seq,
                at: now,
                kind: t.event_kind,
            },
        };
        tx.commit().await.map_err(db_error)?;
        Ok(result)
    }
    async fn events_after(
        &self,
        task_id: TaskId,
        after_seq: EventSeq,
        limit: u32,
    ) -> Result<Vec<CoreEvent>, StoreError> {
        let rows=sqlx::query(&format!("SELECT seq,event_kind_json,created_at FROM {} WHERE task_id=$1 AND seq>$2 ORDER BY seq LIMIT $3",self.table("task_events"))).bind(task_id).bind(after_seq as i64).bind(limit.min(10_000) as i64).fetch_all(&self.pool).await.map_err(db_error)?;
        rows.into_iter()
            .map(|row| {
                let kind: CoreEventKind =
                    serde_json::from_value(row.try_get("event_kind_json").map_err(db_error)?)
                        .map_err(|e| StoreError::Corrupt(e.to_string()))?;
                Ok(CoreEvent {
                    task_id,
                    seq: row.try_get::<i64, _>("seq").map_err(db_error)? as u64,
                    at: row.try_get("created_at").map_err(db_error)?,
                    kind,
                })
            })
            .collect()
    }
    async fn acquire_lease(&self, lease: Lease) -> Result<bool, StoreError> {
        let seconds = lease.expires_in.as_secs() as i64;
        let leases = self.table("task_leases");
        let changed=sqlx::query(&format!("INSERT INTO {leases}(task_id,owner_id,expires_at,updated_at) VALUES($1,$2,NOW()+($3*INTERVAL '1 second'),NOW()) ON CONFLICT(task_id) DO UPDATE SET owner_id=EXCLUDED.owner_id,expires_at=EXCLUDED.expires_at,updated_at=NOW() WHERE {leases}.expires_at<NOW() OR {leases}.owner_id=EXCLUDED.owner_id"))
            .bind(lease.task_id).bind(lease.owner_id).bind(seconds).execute(&self.pool).await.map_err(db_error)?.rows_affected();
        Ok(changed == 1)
    }
    async fn renew_lease(&self, lease: Lease) -> Result<bool, StoreError> {
        let changed=sqlx::query(&format!("UPDATE {} SET expires_at=NOW()+($1*INTERVAL '1 second'),updated_at=NOW() WHERE task_id=$2 AND owner_id=$3 AND expires_at>=NOW()",self.table("task_leases"))).bind(lease.expires_in.as_secs() as i64).bind(lease.task_id).bind(lease.owner_id).execute(&self.pool).await.map_err(db_error)?.rows_affected();
        Ok(changed == 1)
    }
    async fn cleanup(&self, policy: &RetentionPolicy) -> Result<CleanupReport, StoreError> {
        let events = self.table("task_events");
        let tasks = self.table("tasks");
        let et = policy.event_ttl.as_secs() as i64;
        let tt = policy.task_ttl.as_secs() as i64;
        let limit = policy.cleanup_batch_size as i64;
        let events_deleted=sqlx::query(&format!("DELETE FROM {events} WHERE ctid IN (SELECT ctid FROM {events} WHERE created_at<NOW()-($1*INTERVAL '1 second') LIMIT $2)")).bind(et).bind(limit).execute(&self.pool).await.map_err(db_error)?.rows_affected();
        let tasks_deleted=sqlx::query(&format!("DELETE FROM {tasks} WHERE ctid IN (SELECT ctid FROM {tasks} WHERE terminal_at IS NOT NULL AND terminal_at<NOW()-($1*INTERVAL '1 second') LIMIT $2)")).bind(tt).bind(limit).execute(&self.pool).await.map_err(db_error)?.rows_affected();
        Ok(CleanupReport {
            tasks_deleted,
            events_deleted,
            idempotency_deleted: tasks_deleted,
        })
    }
}

fn validate_schema(s: String) -> Result<String, StoreError> {
    if s.is_empty() || !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Err(StoreError::Internal(
            "schema must contain only [A-Za-z0-9_]".into(),
        ))
    } else {
        Ok(s)
    }
}
fn db_error(e: sqlx::Error) -> StoreError {
    StoreError::Unavailable(e.to_string())
}
fn db_state(s: TaskState) -> &'static str {
    match s {
        TaskState::Created => "created",
        TaskState::Accepted => "accepted",
        TaskState::Running => "running",
        TaskState::WaitingForInput => "waiting_for_input",
        TaskState::CancelRequested => "cancel_requested",
        TaskState::Completed => "completed",
        TaskState::Failed => "failed",
        TaskState::Cancelled => "cancelled",
    }
}
fn state(s: &str) -> Result<TaskState, StoreError> {
    match s {
        "created" => Ok(TaskState::Created),
        "accepted" => Ok(TaskState::Accepted),
        "running" => Ok(TaskState::Running),
        "waiting_for_input" => Ok(TaskState::WaitingForInput),
        "cancel_requested" => Ok(TaskState::CancelRequested),
        "completed" => Ok(TaskState::Completed),
        "failed" => Ok(TaskState::Failed),
        "cancelled" => Ok(TaskState::Cancelled),
        _ => Err(StoreError::Corrupt(format!("unknown state: {s}"))),
    }
}
fn snapshot(row: &sqlx::postgres::PgRow) -> Result<TaskSnapshot, StoreError> {
    Ok(TaskSnapshot {
        task_id: row.try_get("task_id").map_err(db_error)?,
        session_id: row.try_get("session_id").map_err(db_error)?,
        agent_id: adapter_model::AgentId(row.try_get("agent_id").map_err(db_error)?),
        caller_id: adapter_model::CallerId(row.try_get("caller_id").map_err(db_error)?),
        state: state(&row.try_get::<String, _>("state").map_err(db_error)?)?,
        revision: row.try_get::<i64, _>("revision").map_err(db_error)? as u64,
        last_seq: row.try_get::<i64, _>("last_seq").map_err(db_error)? as u64,
        created_at: row.try_get("created_at").map_err(db_error)?,
        updated_at: row.try_get("updated_at").map_err(db_error)?,
        terminal_at: row.try_get("terminal_at").map_err(db_error)?,
    })
}
