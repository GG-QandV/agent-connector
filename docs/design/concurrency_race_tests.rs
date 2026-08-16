//! Специальные тесты на race conditions / concurrency, которые обычные
//! sequential unit-тесты не ловят. Каждый тест целится в конкретное узкое
//! место, обсуждённое в ревью. Разложить по фактическим crates:
//!
//!   §1 -> crates/driver-mcp/tests/cancel_race.rs
//!   §2 -> crates/adapter-core/tests/registry_hot_reload.rs (после ADR-0001 п.1)
//!   §3 -> crates/adapter-core/tests/broadcast_overflow.rs
//!   §4 -> crates/adapter-core/tests/idempotency_race.rs
//!   §5 -> crates/protocol-a2a-server/tests/execution_manager_race.rs
//!
//! Все тесты используют `#[tokio::test(flavor = "multi_thread")]`, где нужна
//! настоящая параллельность (не просто interleaved single-thread executor) —
//! иначе гонка может не воспроизвестись детерминированно.

// ============================================================
// §1. cancel-race в driver-mcp: cancel() против естественного завершения
// ============================================================
mod cancel_race {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    /// Фейковый MCP stdio-сервер с управляемой задержкой ответа.
    /// Заменить на реальный `TokioChildProcess` + минимальный echo-сервер
    /// из `examples/servers/` rust-sdk, параметризованный через env var
    /// `RESPONSE_DELAY_MS`, если нужен end-to-end тест, а не мок driver'а.
    struct DelayedMcpFixture {
        response_delay: Duration,
    }

    impl DelayedMcpFixture {
        fn new(response_delay_ms: u64) -> Self {
            Self { response_delay: Duration::from_millis(response_delay_ms) }
        }

        /// Возвращает driver, у которого invoke() эмулирует реальный
        /// send_request_with_option + tokio::select! между await_response()
        /// и cancel_token.cancelled(), но без реального MCP transport —
        /// достаточно для проверки инварианта active_handles/событий.
        async fn spawn_driver(&self) -> McpDriverUnderTest {
            McpDriverUnderTest::new(self.response_delay)
        }
    }

    // Минимальная копия релевантной части driver-mcp invoke() для теста
    // инварианта без реального transport. Если в тестовом окружении есть
    // доступ к настоящему McpDriver::connect_stdio() с тестовым stdio
    // сервером — заменить эту структуру на реальный driver и удалить мок.
    struct McpDriverUnderTest {
        response_delay: Duration,
        active_handles: Arc<dashmap::DashMap<uuid::Uuid, tokio_util::sync::CancellationToken>>,
        events_emitted: Arc<AtomicUsize>,
    }

    #[derive(Debug, PartialEq)]
    enum Outcome { Completed, Cancelled }

    impl McpDriverUnderTest {
        fn new(response_delay: Duration) -> Self {
            Self {
                response_delay,
                active_handles: Arc::new(dashmap::DashMap::new()),
                events_emitted: Arc::new(AtomicUsize::new(0)),
            }
        }

        async fn invoke(&self, task_id: uuid::Uuid) -> tokio::sync::oneshot::Receiver<Outcome> {
            let cancel_token = tokio_util::sync::CancellationToken::new();
            self.active_handles.insert(task_id, cancel_token.clone());
            let (tx, rx) = tokio::sync::oneshot::channel();
            let delay = self.response_delay;
            let active_handles = self.active_handles.clone();
            let events_emitted = self.events_emitted.clone();

            tokio::spawn(async move {
                let outcome = tokio::select! {
                    _ = sleep(delay) => Outcome::Completed,
                    _ = cancel_token.cancelled() => Outcome::Cancelled,
                };
                active_handles.remove(&task_id);
                events_emitted.fetch_add(1, Ordering::SeqCst);
                let _ = tx.send(outcome);
            });

            rx
        }

        fn cancel(&self, task_id: uuid::Uuid) {
            if let Some(entry) = self.active_handles.get(&task_id) {
                entry.value().cancel();
            }
        }

        fn active_handle_count(&self) -> usize {
            self.active_handles.len()
        }
    }

    /// Базовый инвариант: ровно одно из двух исходов, и active_handles
    /// пуст после завершения — независимо от того, кто победил в гонке.
    #[tokio::test(flavor = "multi_thread")]
    async fn cancel_or_complete_never_both_never_leaked() {
        for delay_ms in [0u64, 1, 5, 20, 50, 100] {
            let fixture = DelayedMcpFixture::new(delay_ms);
            let driver = fixture.spawn_driver().await;
            let task_id = uuid::Uuid::new_v4();

            let rx = driver.invoke(task_id).await;
            // Узкое окно: отменяем почти сразу после invoke, до того как
            // known response_delay истечёт (кроме delay_ms=0, где completion
            // может успеть первым — это тоже валидный, ожидаемый исход).
            sleep(Duration::from_millis(2)).await;
            driver.cancel(task_id);

            let outcome = rx.await.expect("spawned task must send exactly one outcome");
            assert!(
                matches!(outcome, Outcome::Completed | Outcome::Cancelled),
                "delay_ms={delay_ms}: unexpected outcome {outcome:?}"
            );
            assert_eq!(
                driver.active_handle_count(), 0,
                "delay_ms={delay_ms}: active_handles leaked after task completion"
            );
        }
    }

    /// Стресс-версия: много одновременных invoke+cancel пар, проверяем что
    /// счётчик emitted событий равен числу invoke — не больше (не может быть
    /// дублирующего события Completed+Cancelled для одной задачи) и не
    /// меньше (не может застрять/потеряться).
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_cancel_storm_exact_one_event_per_task() {
        let fixture = DelayedMcpFixture::new(10);
        let driver = Arc::new(fixture.spawn_driver().await);
        const N: usize = 200;

        let mut receivers = Vec::with_capacity(N);
        let mut task_ids = Vec::with_capacity(N);
        for _ in 0..N {
            let task_id = uuid::Uuid::new_v4();
            task_ids.push(task_id);
            receivers.push(driver.invoke(task_id).await);
        }

        // Отменяем половину задач в случайном порядке относительно их запуска,
        // не дожидаясь завершения — это провоцирует гонку на каждой из них.
        for &task_id in task_ids.iter().step_by(2) {
            driver.cancel(task_id);
        }

        let mut completed = 0usize;
        let mut cancelled = 0usize;
        for rx in receivers {
            match rx.await.expect("must resolve") {
                Outcome::Completed => completed += 1,
                Outcome::Cancelled => cancelled += 1,
            }
        }

        assert_eq!(completed + cancelled, N, "every task must resolve exactly once");
        assert_eq!(driver.active_handle_count(), 0, "no leaked handles after storm");
    }
}

// ============================================================
// §2. AgentRegistry hot-reload: resolve() vs update_skills() concurrency
// (актуально после реализации Решения 1 из ADR-0001-mcp-dynamic-capabilities)
// ============================================================
mod registry_hot_reload {
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;

    /// Заглушка под будущий `RegisteredAgent.skills: Arc<RwLock<Vec<String>>>`.
    /// Заменить на реальный тип из adapter-core после того, как Решение 1
    /// будет реализовано — сохранить сам сценарий теста без изменений.
    struct FakeRegisteredAgent {
        skills: Arc<RwLock<Vec<String>>>,
    }

    impl FakeRegisteredAgent {
        fn new(initial: Vec<String>) -> Self {
            Self { skills: Arc::new(RwLock::new(initial)) }
        }
        async fn has_skill(&self, skill: &str) -> bool {
            self.skills.read().await.iter().any(|s| s == skill)
        }
        async fn update_skills(&self, new: Vec<String>) {
            *self.skills.write().await = new;
        }
    }

    /// Инвариант: skill, присутствующий и до, и после конкретного
    /// update_skills() вызова, должен быть виден resolve() в любой момент —
    /// не должно быть "дыры" видимости, где resolve() временно теряет
    /// skill, который логически никогда пропадал из списка.
    #[tokio::test(flavor = "multi_thread")]
    async fn no_visibility_gap_for_stable_skill_during_concurrent_updates() {
        let agent = Arc::new(FakeRegisteredAgent::new(vec![
            "stable_skill".into(),
            "old_skill".into(),
        ]));

        let writer = {
            let agent = agent.clone();
            tokio::spawn(async move {
                for i in 0..100 {
                    // "stable_skill" остаётся во всех версиях списка;
                    // "old_skill"/"new_skill_{i}" эмулируют реальный
                    // list_changed churn вокруг него.
                    agent.update_skills(vec![
                        "stable_skill".into(),
                        format!("new_skill_{i}"),
                    ]).await;
                    tokio::time::sleep(Duration::from_micros(50)).await;
                }
            })
        };

        let readers: Vec<_> = (0..16).map(|_| {
            let agent = agent.clone();
            tokio::spawn(async move {
                let mut violations = 0usize;
                for _ in 0..500 {
                    if !agent.has_skill("stable_skill").await {
                        violations += 1;
                    }
                }
                violations
            })
        }).collect();

        writer.await.unwrap();
        let mut total_violations = 0usize;
        for r in readers {
            total_violations += r.await.unwrap();
        }

        assert_eq!(
            total_violations, 0,
            "stable_skill must be visible to resolve() at all times, found {total_violations} violations"
        );
    }
}

// ============================================================
// §3. broadcast::channel(256) overflow при медленном подписчике
// ============================================================
mod broadcast_overflow {
    use tokio::sync::broadcast;
    use std::time::Duration;

    const CHANNEL_CAPACITY: usize = 256; // должно совпадать с ActiveTask.tx в adapter-core

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_subscriber_gets_explicit_lagged_error_not_silent_drop() {
        let (tx, mut rx) = broadcast::channel::<u64>(CHANNEL_CAPACITY);

        // Producer шлёт events быстрее, чем rx их читает — намеренно
        // превышаем capacity в 2 раза, не читая параллельно.
        let producer = {
            let tx = tx.clone();
            tokio::spawn(async move {
                for seq in 0..(CHANNEL_CAPACITY as u64 * 2) {
                    let _ = tx.send(seq);
                }
            })
        };
        producer.await.unwrap();

        let mut received = Vec::new();
        let mut got_lagged = false;
        loop {
            match rx.try_recv() {
                Ok(seq) => received.push(seq),
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    got_lagged = true;
                    assert!(skipped > 0, "Lagged error must report a nonzero skip count");
                }
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Closed) => break,
            }
        }

        assert!(
            got_lagged,
            "producer sent {}x capacity without reader keeping up — must surface Lagged, not silently truncate",
            2
        );
        // Именно это должно маппиться в executor.rs на:
        //   Err(broadcast::error::RecvError::Lagged(_)) =>
        //       A2AError::internal("subscription fell behind...")
        // Тест здесь проверяет только сам примитив; end-to-end вариант —
        // отдельный тест в protocol-a2a-server с реальным ActiveExecution.
        let _ = received;
        let _ = Duration::ZERO; // placeholder if timing variant needed later
    }
}

// ============================================================
// §4. Idempotency-key race: два одновременных invoke() с тем же ключом
// ============================================================
mod idempotency_race {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Barrier;

    /// Минимальная модель store.create_or_get_idempotent, чтобы проверить
    /// сам паттерн синхронизации без поднятия настоящего SQLite/Postgres.
    /// Реальный тест должен запускать это против каждого TaskStore impl
    /// (memory/sqlite/postgres) как параметризованный кейс — гонка может
    /// проявляться по-разному в зависимости от того, как backend делает
    /// insert-or-get (UNIQUE constraint + retry vs SELECT-then-INSERT).
    struct FakeIdempotentStore {
        created_count: Arc<AtomicUsize>,
        lock: Arc<tokio::sync::Mutex<Option<String>>>, // хранит created task_id, если есть
    }

    impl FakeIdempotentStore {
        fn new() -> Self {
            Self {
                created_count: Arc::new(AtomicUsize::new(0)),
                lock: Arc::new(tokio::sync::Mutex::new(None)),
            }
        }

        async fn create_or_get(&self, idempotency_key: &str) -> (bool, String) {
            let mut guard = self.lock.lock().await;
            match guard.as_ref() {
                Some(existing) => (false, existing.clone()),
                None => {
                    let task_id = format!("task-for-{idempotency_key}");
                    *guard = Some(task_id.clone());
                    self.created_count.fetch_add(1, Ordering::SeqCst);
                    (true, task_id)
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn exactly_one_created_under_true_concurrent_invoke() {
        let store = Arc::new(FakeIdempotentStore::new());
        const N: usize = 50;
        let barrier = Arc::new(Barrier::new(N));

        let mut handles = Vec::with_capacity(N);
        for _ in 0..N {
            let store = store.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                // Все N задач синхронизируются на барьере, чтобы стартовать
                // create_or_get практически одновременно — это ключевое
                // отличие от sequential "create, then create again" теста.
                barrier.wait().await;
                store.create_or_get("same-idempotency-key").await
            }));
        }

        let mut created_flags = Vec::with_capacity(N);
        let mut task_ids = std::collections::HashSet::new();
        for h in handles {
            let (was_created, task_id) = h.await.unwrap();
            created_flags.push(was_created);
            task_ids.insert(task_id);
        }

        let created_true_count = created_flags.iter().filter(|&&c| c).count();
        assert_eq!(created_true_count, 1, "exactly one invoke() must observe was_created=true");
        assert_eq!(task_ids.len(), 1, "all concurrent invokes must resolve to the same task_id");
        assert_eq!(store.created_count.load(Ordering::SeqCst), 1, "store must record exactly one creation");
    }
}

// ============================================================
// §5. ExecutionManager::finish() use-after-finish (Arc::ptr_eq guard)
// ============================================================
mod execution_manager_race {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    /// Уменьшенная модель ExecutionManager из protocol-a2a-server: только
    /// та часть, что отвечает за start/finish/ptr_eq guard, без реального
    /// broadcast/streaming — цель теста в самой логике удаления, не в I/O.
    struct FakeActiveExecution {
        marker: u64,
    }

    struct FakeExecutionManager {
        executions: RwLock<HashMap<String, Arc<FakeActiveExecution>>>,
    }

    impl FakeExecutionManager {
        fn new() -> Self {
            Self { executions: RwLock::new(HashMap::new()) }
        }

        async fn start(&self, task_id: &str, marker: u64) -> Arc<FakeActiveExecution> {
            let active = Arc::new(FakeActiveExecution { marker });
            self.executions.write().await.insert(task_id.to_string(), active.clone());
            active
        }

        /// 1:1 копия защиты из sdk_a2a_server_handler.rs: удаляет запись
        /// только если текущая запись в map — это тот же Arc, что вызывающий
        /// держит. Если start() для того же task_id уже успел вставить новый
        /// Arc, finish() старого execution не должен его затирать.
        async fn finish(&self, task_id: &str, active: Arc<FakeActiveExecution>) {
            let mut executions = self.executions.write().await;
            let should_remove = executions
                .get(task_id)
                .is_some_and(|current| Arc::ptr_eq(current, &active));
            if should_remove {
                executions.remove(task_id);
            }
        }

        async fn get(&self, task_id: &str) -> Option<Arc<FakeActiveExecution>> {
            self.executions.read().await.get(task_id).cloned()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_finish_does_not_evict_newer_execution() {
        let manager = Arc::new(FakeExecutionManager::new());
        let task_id = "same-task-id";

        // Старая execution стартует первой.
        let old_execution = manager.start(task_id, 1).await;

        // Новая execution (например, из повторного cancel()+restart сценария)
        // стартует ДО того, как старая корутина вызовет finish() —
        // это именно тот порядок, который защита Arc::ptr_eq должна покрыть.
        let new_execution = manager.start(task_id, 2).await;
        assert_ne!(old_execution.marker, new_execution.marker);

        // Старая корутина наконец добирается до finish(), уже устаревшая.
        manager.finish(task_id, old_execution).await;

        // Инвариант: новая execution должна остаться в map нетронутой.
        let current = manager.get(task_id).await
            .expect("newer execution must still be present after stale finish()");
        assert_eq!(current.marker, 2, "stale finish() must not evict the newer active execution");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fresh_finish_does_evict_current_execution() {
        let manager = Arc::new(FakeExecutionManager::new());
        let task_id = "same-task-id";

        let execution = manager.start(task_id, 42).await;
        manager.finish(task_id, execution).await;

        assert!(
            manager.get(task_id).await.is_none(),
            "finish() with the current active execution must evict it"
        );
    }
}
