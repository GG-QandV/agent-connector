//! §1 из docs/design/concurrency_race_tests.rs: cancel-race в driver-mcp.
//!
//! В отличие от исходного мока в docs/design — здесь тестируется РЕАЛЬНЫЙ
//! `McpDriver` против реального MCP stdio-сервера (`mcp_test_counter`, bin
//! этого crate), с tool'ом `delayed(delay_ms)`, дающим управляемую задержку
//! ответа. Именно так гонка между `cancel()` и естественным завершением
//! воспроизводится на настоящем коде, а не на копии его логики.
//!
//! Инварианты:
//! - на каждую задачу эмитируется ровно ОДНО terminal-событие
//!   (Completed XOR Failed XOR Cancelled), никогда не два;
//! - после terminal-события канал закрывается (spawn завершился) — это
//!   observable-эквивалент "active_handles не течёт".

use std::{path::PathBuf, time::Duration};

use adapter_core::{AgentDriver, DriverEvent};
use adapter_model::{InvokeRequest, Part};
use driver_mcp::{McpDriver, McpStdioConfig};
use tokio::sync::mpsc;

fn counter_config() -> McpStdioConfig {
    let bin = PathBuf::from(env!("CARGO_BIN_EXE_mcp_test_counter"));
    McpStdioConfig {
        command: bin,
        args: Vec::new(),
        env: std::collections::HashMap::new(),
    }
}

fn delayed_invoke(delay_ms: u64) -> InvokeRequest {
    InvokeRequest {
        task_id: None,
        agent_id: None,
        skill_id: Some("delayed".to_string()),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        session_id: None,
        input: vec![Part::Json {
            value: serde_json::json!({ "delay_ms": delay_ms }),
        }],
        context: serde_json::Value::Null,
        deadline: None,
    }
}

/// Читает события до terminal-события, затем проверяет, что канал закрыт
/// (spawn завершился). Возвращает terminal-событие.
async fn await_terminal_and_closed(mut rx: mpsc::Receiver<DriverEvent>) -> DriverEvent {
    let terminal = loop {
        let event = rx
            .recv()
            .await
            .expect("driver channel closed before terminal event");
        if matches!(
            event,
            DriverEvent::Completed(_) | DriverEvent::Failed(_) | DriverEvent::Cancelled
        ) {
            break event;
        }
    };
    // Terminal отправлен -> spawn должен завершиться и дропнуть sender ->
    // канал обязан быть закрыт. Это observable-эквивалент отсутствия утечки
    // active_handles (remove() выполнен до завершения задачи).
    assert!(
        rx.recv().await.is_none(),
        "channel must close after terminal event (spawn finished, no leak)"
    );
    terminal
}

fn is_terminal(event: &DriverEvent) -> bool {
    matches!(
        event,
        DriverEvent::Completed(_) | DriverEvent::Failed(_) | DriverEvent::Cancelled
    )
}

/// Гонка cancel() vs завершение для одного task: при любых таймингах ровно
/// одно terminal-событие, никогда Completed+Cancelled вместе.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_or_complete_never_both_and_channel_closes() {
    let driver = McpDriver::connect_stdio(
        "race-counter",
        counter_config(),
        vec!["delayed".to_string()],
        Duration::from_secs(30),
        adapter_model::AgentId("test-counter".into()),
        std::sync::Weak::new(),
    )
    .await
    .expect("connect to counter server");

    // (а) cancel приходит ДО завершения — должен выиграть Cancelled.
    for delay_ms in [10u64, 50, 100] {
        let task_id = uuid::Uuid::new_v4();
        let rx = driver
            .invoke(task_id, delayed_invoke(delay_ms))
            .await
            .expect("invoke delayed");
        tokio::time::sleep(Duration::from_millis(2)).await;
        driver.cancel(task_id).await.expect("cancel");
        let terminal = await_terminal_and_closed(rx).await;
        assert!(
            matches!(terminal, DriverEvent::Cancelled),
            "delay_ms={delay_ms}: expected Cancelled, got {terminal:?}"
        );
    }

    // (б) завершение приходит раньше cancel — должен выиграть Completed.
    {
        let task_id = uuid::Uuid::new_v4();
        let rx = driver
            .invoke(task_id, delayed_invoke(1))
            .await
            .expect("invoke delayed");
        tokio::time::sleep(Duration::from_millis(50)).await;
        driver
            .cancel(task_id)
            .await
            .expect("cancel after completion");
        let terminal = await_terminal_and_closed(rx).await;
        assert!(
            matches!(terminal, DriverEvent::Completed(_)),
            "expected Completed, got {terminal:?}"
        );
    }
}

/// Стресс: много одновременных invoke+cancel пар. Суммарно эмитировано ровно
/// N terminal-событий — ни одного дублирующего, ни одного потерянного.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cancel_storm_exact_one_event_per_task() {
    let driver = std::sync::Arc::new(
        McpDriver::connect_stdio(
            "race-counter",
            counter_config(),
            vec!["delayed".to_string()],
            Duration::from_secs(30),
            adapter_model::AgentId("test-counter".into()),
            std::sync::Weak::new(),
        )
        .await
        .expect("connect to counter server"),
    );

    const N: usize = 64;
    let mut receivers = Vec::with_capacity(N);
    let mut task_ids = Vec::with_capacity(N);
    for _ in 0..N {
        let task_id = uuid::Uuid::new_v4();
        task_ids.push(task_id);
        let rx = driver
            .invoke(task_id, delayed_invoke(5_000))
            .await
            .expect("invoke delayed");
        receivers.push(rx);
    }

    // Отменяем каждую вторую задачу в параллельных тасках — провоцируем
    // гонку на каждой из них одновременно.
    let mut cancels = Vec::new();
    for &task_id in task_ids.iter().step_by(2) {
        let driver = driver.clone();
        cancels.push(tokio::spawn(async move {
            driver.cancel(task_id).await.expect("cancel");
        }));
    }
    for handle in cancels {
        handle.await.expect("cancel task");
    }

    let mut terminal_count = 0usize;
    for mut rx in receivers {
        let mut seen_terminal = false;
        while let Some(event) = rx.recv().await {
            if is_terminal(&event) {
                assert!(!seen_terminal, "duplicate terminal event for one task");
                seen_terminal = true;
                terminal_count += 1;
            }
        }
        assert!(seen_terminal, "task resolved without a terminal event");
    }

    assert_eq!(terminal_count, N, "exactly one terminal event per task");
}
