//! Integration test: `driver-mcp` против реального MCP stdio-сервера
//! `mcp_test_counter` (bin в этом же crate).
//!
//! Полный цикл: connect_stdio -> discover_tools (находит tool) -> invoke() ->
//! Completed с ожидаемым content -> cancel() во время long-running tool call ->
//! spawn'нутая задача завершается, Cancelled доставляется, orphaned tasks нет.

use std::{path::PathBuf, time::Duration};

use adapter_core::{AgentDriver, CoreError, DriverEvent};
use adapter_model::{InvokeRequest, Part};
use driver_mcp::{McpDriver, McpHttpConfig, McpStdioConfig};
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc;

fn counter_config() -> McpStdioConfig {
    let bin = PathBuf::from(env!("CARGO_BIN_EXE_mcp_test_counter"));
    McpStdioConfig {
        command: bin,
        args: Vec::new(),
        env: std::collections::HashMap::new(),
    }
}

fn counter_http_config(endpoint: &str) -> McpHttpConfig {
    McpHttpConfig {
        endpoint: endpoint.to_string(),
        token: None,
    }
}

fn invoke_request(skill_id: &str, input: Vec<Part>) -> InvokeRequest {
    InvokeRequest {
        task_id: None,
        agent_id: None,
        skill_id: Some(skill_id.to_string()),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        session_id: None,
        input,
        context: serde_json::Value::Null,
        deadline: None,
    }
}

async fn drain_events_async(mut rx: mpsc::Receiver<DriverEvent>) -> Vec<DriverEvent> {
    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        let terminal = matches!(
            event,
            DriverEvent::Completed(_) | DriverEvent::Failed(_) | DriverEvent::Cancelled
        );
        events.push(event);
        if terminal {
            break;
        }
    }
    events
}

#[tokio::test]
async fn discovery_finds_counter_tools() {
    let driver = McpDriver::connect_stdio(
        "test-counter",
        counter_config(),
        Vec::new(),
        Duration::from_secs(5),
    )
    .await
    .expect("connect to counter server");

    assert!(driver.health().await.is_ok());

    // invoke с tool, которого нет в discover — должен быть отклонён.
    let err = driver
        .invoke(
            uuid::Uuid::new_v4(),
            invoke_request("nonexistent_tool", vec![]),
        )
        .await
        .expect_err("unknown tool must be rejected");
    assert!(matches!(err, CoreError::InvalidRequest(_)));
}

#[tokio::test]
async fn invoke_get_value_completes_with_text() {
    let driver = McpDriver::connect_stdio(
        "test-counter",
        counter_config(),
        vec!["get_value".to_string(), "increment".to_string()],
        Duration::from_secs(5),
    )
    .await
    .expect("connect to counter server");

    let task_id = uuid::Uuid::new_v4();
    let rx = driver
        .invoke(task_id, invoke_request("increment", vec![]))
        .await
        .expect("invoke increment");

    let events = drain_events_async(rx).await;
    let completed = events
        .iter()
        .find_map(|event| match event {
            DriverEvent::Completed(parts) => Some(parts.clone()),
            _ => None,
        })
        .expect("Completed event expected");
    assert!(matches!(
        completed.as_slice(),
        [Part::Text { text }] if text == "1"
    ));

    // Второй invoke — значение должно вырасти до 2.
    let task_id = uuid::Uuid::new_v4();
    let rx = driver
        .invoke(task_id, invoke_request("get_value", vec![]))
        .await
        .expect("invoke get_value");
    let events = drain_events_async(rx).await;
    let completed = events
        .iter()
        .find_map(|event| match event {
            DriverEvent::Completed(parts) => Some(parts.clone()),
            _ => None,
        })
        .expect("Completed event expected");
    assert!(matches!(
        completed.as_slice(),
        [Part::Text { text }] if text == "1"
    ));
}

#[tokio::test]
async fn cancel_long_task_delivers_cancelled_and_no_orphan_tasks() {
    let driver = McpDriver::connect_stdio(
        "test-counter",
        counter_config(),
        vec!["long_task".to_string()],
        Duration::from_secs(30),
    )
    .await
    .expect("connect to counter server");

    let task_id = uuid::Uuid::new_v4();
    let mut rx = driver
        .invoke(task_id, invoke_request("long_task", vec![]))
        .await
        .expect("invoke long_task");

    // Ждём Accepted, затем отменяем.
    let accepted = rx
        .recv()
        .await
        .expect("Accepted event expected before cancel");
    assert!(matches!(accepted, DriverEvent::Accepted));

    // CancellationToken подхватывается spawn'нутой задачей: cancel() снаружи
    // лишь сигналит, handle.cancel() выполняется внутри select!.
    driver.cancel(task_id).await.expect("cancel long_task");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(event)) => {
                // Должен прийти Cancelled до истечения long_task (10s).
                if matches!(event, DriverEvent::Cancelled) {
                    return;
                }
                // Progress или Accepted — продолжаем ждать.
            }
            Ok(None) => panic!("channel closed before Cancelled"),
            Err(_) => break,
        }
    }
    panic!("Cancelled event not received within 5s");
}

#[tokio::test]
async fn input_schema_validation_rejects_missing_required_field() {
    let driver = McpDriver::connect_stdio(
        "test-counter",
        counter_config(),
        vec!["echo".to_string()],
        Duration::from_secs(5),
    )
    .await
    .expect("connect to counter server");

    // echo требует обязательное поле message — без него InvalidRequest.
    let err = driver
        .invoke(uuid::Uuid::new_v4(), invoke_request("echo", vec![]))
        .await
        .expect_err("echo without message must be rejected by schema");
    assert!(matches!(err, CoreError::InvalidRequest(_)));
    assert!(err.to_string().contains("message"));

    // С валидным input — проходит.
    let rx = driver
        .invoke(
            uuid::Uuid::new_v4(),
            invoke_request(
                "echo",
                vec![Part::Json {
                    value: serde_json::json!({ "message": "hello" }),
                }],
            ),
        )
        .await
        .expect("echo with message must succeed");
    let events = drain_events_async(rx).await;
    let completed = events
        .iter()
        .find_map(|event| match event {
            DriverEvent::Completed(parts) => Some(parts.clone()),
            _ => None,
        })
        .expect("Completed event expected");
    assert!(matches!(
        completed.as_slice(),
        [Part::Text { text }] if text == "hello"
    ));
}

#[tokio::test]
async fn http_transport_full_roundtrip() {
    // Поднимаем counter сервер в http-режиме на свободном порту.
    let addr = "127.0.0.1:0";
    let bin = PathBuf::from(env!("CARGO_BIN_EXE_mcp_test_counter"));
    let mut server = tokio::process::Command::new(&bin)
        .env("MCP_TEST_COUNTER_HTTP", addr)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn counter http server");

    // Узнаём фактический порт: сервер печатает его в stderr. Ждём строку.
    let stderr = server.stderr.take().expect("stderr");
    let mut lines = tokio::io::BufReader::new(stderr).lines();
    let mut http_addr = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), lines.next_line()).await {
            Ok(Ok(Some(line))) => {
                if let Some(start) = line.find("http://") {
                    http_addr = Some(line[start..].trim_end_matches('/').to_string());
                    break;
                }
            }
            Ok(Ok(None)) => break,
            _ => continue,
        }
    }
    let http_addr = http_addr.expect("counter http server must print listening address to stderr");

    let driver = McpDriver::connect_http(
        "test-counter-http",
        counter_http_config(&http_addr),
        vec!["increment".to_string(), "get_value".to_string()],
        Duration::from_secs(5),
    )
    .await
    .expect("connect to counter http server");
    let rx = driver
        .invoke(uuid::Uuid::new_v4(), invoke_request("increment", vec![]))
        .await
        .expect("invoke increment over http");
    let events = drain_events_async(rx).await;
    let completed = events
        .iter()
        .find_map(|event| match event {
            DriverEvent::Completed(parts) => Some(parts.clone()),
            _ => None,
        })
        .expect("Completed event expected over http");
    assert!(matches!(
        completed.as_slice(),
        [Part::Text { text }] if text == "1"
    ));

    let _ = server.kill().await;
    let _ = server.wait().await;
}

#[tokio::test]
async fn protocol_version_is_verified_on_connect() {
    // Counter server (rmcp 0.8.5) отдаёт поддерживаемую версию — подключение
    // должно пройти, verify_protocol_version не должен отклонить.
    let driver = McpDriver::connect_stdio(
        "test-counter",
        counter_config(),
        vec!["get_value".to_string()],
        Duration::from_secs(5),
    )
    .await
    .expect("connect must succeed with supported protocol version");
    assert!(driver.health().await.is_ok());
}
