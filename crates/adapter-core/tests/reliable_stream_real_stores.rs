//! E2E storage integration tests for reliable streaming.
//!
//! Full path: `AdapterCore → real TaskStore → subscribe/history →
//! ReliableTaskStream`. SQLite tests run in the normal suite; PostgreSQL
//! tests are `#[ignore]` because they require a live Postgres (Docker).
//! `TEST_DATABASE_URL` must be set to run them.
//!
//! A `ScriptedDriver` lets each test control the exact event sequence, so
//! ordering / no-loss / resume / lag recovery are asserted against the real
//! store, not a fake.

use std::sync::Arc;

use adapter_core::{
    AdapterCore, AgentDriver, AgentLimits, AgentRegistry, AllowAllPolicy, Caller, CallerId,
    CoreCommand, CoreError, DispatchResult, DriverCapabilities, DriverEvent, InvokeRequest, Part,
    RegisteredAgent, ReliableTaskStream, TaskId, TaskState,
};
use adapter_model::{AgentId, TaskSnapshot};
use adapter_store_contract::TaskStore;
use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::{sleep, Duration};

/// A driver whose events the test controls via an external sender.
struct ScriptedDriver {
    tx: Arc<Mutex<Option<mpsc::Sender<DriverEvent>>>>,
    ready: Arc<Notify>,
}

impl ScriptedDriver {
    fn new() -> (Arc<Self>, Arc<Notify>) {
        let ready = Arc::new(Notify::new());
        let driver = Arc::new(Self {
            tx: Arc::new(Mutex::new(None)),
            ready: ready.clone(),
        });
        (driver, ready)
    }

    /// Waits until the worker called `invoke` and the sender is installed.
    async fn sender(&self, ready: &Notify) -> mpsc::Sender<DriverEvent> {
        ready.notified().await;
        self.tx
            .lock()
            .await
            .take()
            .expect("invoke must have been called")
    }
}

#[async_trait]
impl AgentDriver for ScriptedDriver {
    fn id(&self) -> &str {
        "scripted"
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
        let (tx, rx) = mpsc::channel(512);
        *self.tx.lock().await = Some(tx);
        self.ready.notify_one();
        Ok(rx)
    }
    async fn cancel(&self, _task_id: TaskId) -> Result<(), CoreError> {
        Ok(())
    }
    async fn provide_input(&self, _task_id: TaskId, _input: Vec<Part>) -> Result<(), CoreError> {
        Ok(())
    }
}

fn snapshot_state(snapshot: &TaskSnapshot) -> TaskState {
    snapshot.state
}

/// Registers the scripted driver and returns an AdapterCore over `store`.
fn build_core_with_driver(
    store: Arc<dyn TaskStore>,
    driver: Arc<ScriptedDriver>,
) -> Arc<AdapterCore> {
    let registry = Arc::new(AgentRegistry::new());
    registry.register(RegisteredAgent::new(
        AgentId("scripted".into()),
        vec!["skill".into()],
        driver,
        AgentLimits {
            max_concurrent_tasks: 16,
            max_queued_tasks: 64,
            max_input_bytes: 1024 * 1024,
            max_event_bytes: 256 * 1024,
            default_timeout: Duration::from_secs(60),
        },
    ));
    Arc::new(AdapterCore::new(
        store,
        registry,
        Arc::new(AllowAllPolicy),
        16,
    ))
}

/// Invokes a task on `core` and returns the task id.
async fn invoke_task(core: &AdapterCore, key: &str) -> TaskId {
    match core
        .dispatch(
            Caller {
                id: CallerId("e2e".into()),
                scopes: vec![],
            },
            CoreCommand::Invoke(InvokeRequest {
                task_id: None,
                agent_id: None,
                skill_id: None,
                idempotency_key: key.into(),
                session_id: None,
                input: vec![],
                context: serde_json::json!({}),
                deadline: None,
            }),
        )
        .await
        .expect("invoke should succeed")
    {
        DispatchResult::Created(snapshot) => snapshot.task_id,
        DispatchResult::Existing(snapshot) => snapshot.task_id,
        other => panic!("unexpected dispatch result: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Assertions shared by SQLite and Postgres.
// ---------------------------------------------------------------------------

async fn run_full_path(store: Arc<dyn TaskStore>) {
    let (driver, ready) = ScriptedDriver::new();
    let core = build_core_with_driver(store.clone(), driver.clone());
    let task_id = invoke_task(&core, "e2e-1").await;
    let tx = driver.sender(&ready).await;

    // history -> live ordering: send progress events, then terminal.
    tx.send(DriverEvent::Progress {
        message: "step 1".into(),
        percent: Some(10),
    })
    .await
    .unwrap();
    tx.send(DriverEvent::Progress {
        message: "step 2".into(),
        percent: Some(40),
    })
    .await
    .unwrap();
    // Wait so the first two are durably in the store before we subscribe.
    sleep(Duration::from_millis(30)).await;
    let mut stream = ReliableTaskStream::subscribe(&core, task_id, 0)
        .await
        .expect("subscribe");
    tx.send(DriverEvent::Completed(vec![])).await.unwrap();

    // Drain: must see seq 1..4 in strict order (Accepted=1, two Progress,
    // Completed=4), then terminal None.
    let mut seen: Vec<u64> = vec![];
    while let Some(event) = stream.next().await.expect("no errors") {
        seen.push(event.seq);
        if stream.is_terminal() {
            break;
        }
    }
    assert_eq!(seen, vec![1, 2, 3, 4], "strict ordering history->live");
    assert!(stream.is_terminal(), "terminal after Completed");

    // after_seq resume: a new stream from seq=2 must deliver only seq>2.
    let mut resumed = ReliableTaskStream::subscribe(&core, task_id, 2)
        .await
        .expect("resume subscribe");
    let mut resumed_seen: Vec<u64> = vec![];
    while let Some(event) = resumed.next().await.expect("resume no errors") {
        resumed_seen.push(event.seq);
        if resumed.is_terminal() {
            break;
        }
    }
    assert_eq!(
        resumed_seen,
        vec![3, 4],
        "after_seq=2 resume delivers seq>2 only"
    );

    // Duplicate suppression: resume from seq=4 (last delivered) yields nothing.
    let mut after_last = ReliableTaskStream::subscribe(&core, task_id, 4)
        .await
        .expect("after-last subscribe");
    assert!(
        after_last.next().await.expect("after-last").is_none(),
        "no events with seq <= after_seq are redelivered"
    );

    // Direct TaskStore::events_after pagination sanity.
    let history = store
        .events_after(task_id, 1, 500)
        .await
        .expect("events_after");
    let seqs: Vec<u64> = history.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, vec![2, 3, 4], "events_after(1) returns seq>1");

    // Terminal snapshot.
    let snap = store
        .get_snapshot(task_id)
        .await
        .expect("snapshot")
        .expect("exists");
    assert!(snapshot_state(&snap).terminal(), "terminal state persisted");
}

async fn run_lag_recovery(store: Arc<dyn TaskStore>) {
    let (driver, ready) = ScriptedDriver::new();
    let core = build_core_with_driver(store, driver.clone());
    let task_id = invoke_task(&core, "e2e-lag").await;
    let tx = driver.sender(&ready).await;

    // Overwhelm the broadcast channel (>256 events) without the consumer
    // reading: the receiver must Lag, and ReliableTaskStream must recover via
    // durable catch-up from the store.
    for i in 0..300u64 {
        tx.send(DriverEvent::Progress {
            message: format!("bulk {i}"),
            percent: None,
        })
        .await
        .unwrap();
    }
    tx.send(DriverEvent::Completed(vec![])).await.unwrap();

    // Subscribe late: history contains all 302 events (Accepted + 300 + Completed);
    // live receiver may be lagged/closed by now.
    let mut stream = ReliableTaskStream::subscribe(&core, task_id, 0)
        .await
        .expect("subscribe after burst");

    let mut seen: Vec<u64> = vec![];
    while let Some(event) = stream.next().await.expect("catch-up must not error") {
        seen.push(event.seq);
        if stream.is_terminal() {
            break;
        }
    }
    assert_eq!(seen.len(), 302, "no event lost despite broadcast overflow");
    assert_eq!(seen.first(), Some(&1), "starts at Accepted seq=1");
    assert_eq!(seen.last(), Some(&302), "ends at Completed seq=302");
    assert!(stream.is_terminal());
}

async fn run_terminal_and_closed(store: Arc<dyn TaskStore>) {
    let (driver, ready) = ScriptedDriver::new();
    let core = build_core_with_driver(store, driver.clone());
    let task_id = invoke_task(&core, "e2e-term").await;
    let tx = driver.sender(&ready).await;

    tx.send(DriverEvent::Progress {
        message: "p".into(),
        percent: None,
    })
    .await
    .unwrap();
    sleep(Duration::from_millis(30)).await;
    let mut stream = ReliableTaskStream::subscribe(&core, task_id, 0)
        .await
        .expect("subscribe");

    tx.send(DriverEvent::Completed(vec![])).await.unwrap();
    let mut seen: Vec<u64> = vec![];
    while let Some(e) = stream.next().await.expect("ok") {
        seen.push(e.seq);
        if stream.is_terminal() {
            break;
        }
    }
    assert_eq!(seen, vec![1, 2, 3]);
    assert!(stream.is_terminal());

    // The broadcast sender is closed after the worker removes the active
    // entry; a fresh subscription still replays durable history.
    let mut replay = ReliableTaskStream::subscribe(&core, task_id, 0)
        .await
        .expect("replay");
    let mut replayed: Vec<u64> = vec![];
    while let Some(e) = replay.next().await.expect("replay ok") {
        replayed.push(e.seq);
        if replay.is_terminal() {
            break;
        }
    }
    assert_eq!(
        replayed,
        vec![1, 2, 3],
        "closed sender still replays durable history"
    );
}

async fn run_retention_empty_history(store: Arc<dyn TaskStore>) {
    let (driver, ready) = ScriptedDriver::new();
    let core = build_core_with_driver(store, driver.clone());
    let task_id = invoke_task(&core, "e2e-ret").await;
    let tx = driver.sender(&ready).await;
    tx.send(DriverEvent::Completed(vec![])).await.unwrap();
    sleep(Duration::from_millis(30)).await;

    // A cursor past the end yields an empty history and terminal None.
    let mut stream = ReliableTaskStream::subscribe(&core, task_id, 10_000)
        .await
        .expect("subscribe with far cursor");
    assert!(
        stream.next().await.expect("no events expected").is_none(),
        "cursor past end returns empty history"
    );
}

// ---------------------------------------------------------------------------
// SQLite — run in the normal suite.
// ---------------------------------------------------------------------------

async fn sqlite_store() -> Arc<dyn TaskStore> {
    // tempdir must outlive the pool: use a unique path under the OS temp dir.
    let db_path = std::env::temp_dir().join(format!("anp-e2e-{}.sqlite", uuid::Uuid::new_v4()));
    let _ = std::fs::remove_file(&db_path);
    let store = sqlite_task_store_adapter::SqliteTaskStore::open(&db_path)
        .await
        .expect("open sqlite");
    // Keep the file alive for the store's lifetime.
    Box::leak(Box::new(db_path));
    Arc::new(store)
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_full_path_ordering_and_resume() {
    run_full_path(sqlite_store().await).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_lag_recovery_via_durable_catch_up() {
    run_lag_recovery(sqlite_store().await).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_terminal_event_and_closed_sender() {
    run_terminal_and_closed(sqlite_store().await).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_retention_and_empty_history() {
    run_retention_empty_history(sqlite_store().await).await;
}

// ---------------------------------------------------------------------------
// PostgreSQL — same E2E path, requires a live database (#[ignore]).
// Set TEST_DATABASE_URL (e.g. postgres://postgres:postgres@localhost:5432/agent_connector_test).
// ---------------------------------------------------------------------------

async fn postgres_store() -> Option<Arc<dyn TaskStore>> {
    let dsn = std::env::var("TEST_DATABASE_URL").ok()?;
    Some(Arc::new(
        postgres_task_store_adapter::PostgresTaskStore::connect(&dsn, "anp_e2e", 4)
            .await
            .expect("connect postgres"),
    ))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL (TEST_DATABASE_URL)"]
async fn postgres_full_path_ordering_and_resume() {
    let store = postgres_store()
        .await
        .expect("TEST_DATABASE_URL must be set");
    run_full_path(store).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL (TEST_DATABASE_URL)"]
async fn postgres_lag_recovery_via_durable_catch_up() {
    let store = postgres_store()
        .await
        .expect("TEST_DATABASE_URL must be set");
    run_lag_recovery(store).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL (TEST_DATABASE_URL)"]
async fn postgres_terminal_event_and_closed_sender() {
    let store = postgres_store()
        .await
        .expect("TEST_DATABASE_URL must be set");
    run_terminal_and_closed(store).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live PostgreSQL (TEST_DATABASE_URL)"]
async fn postgres_retention_and_empty_history() {
    let store = postgres_store()
        .await
        .expect("TEST_DATABASE_URL must be set");
    run_retention_empty_history(store).await;
}
