//! §4 из docs/design/concurrency_race_tests.rs: idempotency-key race.
//!
//! В отличие от мока в docs/design — тестируется РЕАЛЬНЫЙ
//! `MemoryTaskStore::create_or_get_idempotent`. Barrier заставляет все N
//! задач стартовать insert-or-get практически одновременно — это ключевое
//! отличие от sequential "create, then create again" теста, который гонку
//! никогда не поймает.
//!
//! Инвариант: из N одновременных create_or_get с одним ключом ровно одна
//! задача наблюдает `Created`, остальные — `Existing`, и все получают один
//! и тот же task_id.

use std::sync::Arc;

use adapter_model::{AgentId, CallerId, CreateTaskResult, NewTask, TaskId};
use adapter_store_contract::TaskStore;
use memory_task_store::MemoryTaskStore;
use tokio::sync::Barrier;
use uuid::Uuid;

fn new_task(task_id: TaskId, idempotency_key: &str) -> NewTask {
    NewTask {
        task_id,
        session_id: None,
        agent_id: AgentId("race-agent".into()),
        caller_id: CallerId("race-caller".into()),
        idempotency_key: idempotency_key.to_string(),
        deadline_at: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn exactly_one_created_under_true_concurrent_invoke() {
    let store = Arc::new(MemoryTaskStore::new());
    const N: usize = 50;
    let barrier = Arc::new(Barrier::new(N));
    let key = "same-idempotency-key";
    let fixed_task_id = Uuid::new_v4();

    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .create_or_get_idempotent(new_task(fixed_task_id, key))
                .await
        }));
    }

    let mut created_count = 0usize;
    let mut existing_count = 0usize;
    let mut task_ids = std::collections::HashSet::new();
    for handle in handles {
        match handle.await.expect("task panicked") {
            Ok(CreateTaskResult::Created(snapshot)) => {
                created_count += 1;
                task_ids.insert(snapshot.task_id);
            }
            Ok(CreateTaskResult::Existing(snapshot)) => {
                existing_count += 1;
                task_ids.insert(snapshot.task_id);
            }
            Err(error) => panic!("store error: {error:?}"),
        }
    }

    assert_eq!(
        created_count, 1,
        "exactly one invoke() must observe Created, got {created_count}"
    );
    assert_eq!(
        existing_count,
        N - 1,
        "all other invokes must observe Existing"
    );
    assert_eq!(
        task_ids.len(),
        1,
        "all concurrent invokes must resolve to the same task_id"
    );
}
