//! crates/driver-acp-client/src/lib.rs — ИСПРАВЛЕННАЯ версия, три фикса:
//!   1. Cargo.toml: tokio features дополнены process/io-util/io-std (см.
//!      diff Cargo.toml ниже отдельным блоком в конце файла).
//!   2. Убрана лишняя '}' — весь файл теперь один непрерывный module scope,
//!      impl AgentDriver и все свободные функции видны друг другу.
//!   3. acp_block_to_part принимает &Value (не Value), вызывается через
//!      .iter() без .cloned() — меньше аллокаций, тип совпадает с filter_map.

use adapter_core::{AgentDriver, CoreError, DriverCapabilities, DriverEvent};
use adapter_model::{InputRequest, InvokeRequest, Part, PublicError, TaskId};
use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot, Mutex};

#[derive(Clone, Debug)]
pub struct AcpClientConfig {
    pub command: PathBuf,
    pub args: Vec<String>,
    pub working_dir: Option<PathBuf>,
}

#[derive(thiserror::Error, Debug)]
pub enum AcpClientError {
    #[error("failed to spawn ACP agent process: {0}")]
    Spawn(String),
    #[error("ACP process stdio unavailable")]
    StdioUnavailable,
    #[error("JSON-RPC request failed: {0}")]
    Rpc(String),
    #[error("ACP process exited unexpectedly")]
    ProcessExited,
}

type PendingRequests = Arc<DashMap<u64, oneshot::Sender<Value>>>;

struct ActiveAcpTask {
    event_tx: mpsc::Sender<DriverEvent>,
    #[allow(dead_code)]
    // зарезервировано для будущего использования session_id в provide_input/cancel-разграничении по сессии
    session_id: Option<String>,
}

pub struct AcpClientDriver {
    id: String,
    child_stdin: Arc<Mutex<tokio::process::ChildStdin>>,
    pending: PendingRequests,
    active_tasks: Arc<DashMap<TaskId, ActiveAcpTask>>,
    next_request_id: Arc<AtomicU64>,
    _child: Arc<Mutex<Child>>,
    reader_task: tokio::task::JoinHandle<()>,
}

impl AcpClientDriver {
    pub async fn spawn(
        id: impl Into<String>,
        config: AcpClientConfig,
    ) -> Result<Self, AcpClientError> {
        let mut command = Command::new(&config.command);
        command
            .args(&config.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(dir) = &config.working_dir {
            command.current_dir(dir);
        }

        let mut child = command
            .spawn()
            .map_err(|e| AcpClientError::Spawn(e.to_string()))?;
        let stdin = child.stdin.take().ok_or(AcpClientError::StdioUnavailable)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(AcpClientError::StdioUnavailable)?;

        let pending: PendingRequests = Arc::new(DashMap::new());
        let active_tasks: Arc<DashMap<TaskId, ActiveAcpTask>> = Arc::new(DashMap::new());

        let reader_task = spawn_reader_loop(stdout, pending.clone(), active_tasks.clone());

        let driver = Self {
            id: id.into(),
            child_stdin: Arc::new(Mutex::new(stdin)),
            pending,
            active_tasks,
            next_request_id: Arc::new(AtomicU64::new(1)),
            _child: Arc::new(Mutex::new(child)),
            reader_task,
        };

        driver.call("initialize", json!({})).await?;
        Ok(driver)
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, AcpClientError> {
        let id = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);

        let request = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let mut line =
            serde_json::to_string(&request).map_err(|e| AcpClientError::Rpc(e.to_string()))?;
        line.push('\n');

        {
            let mut stdin = self.child_stdin.lock().await;
            stdin
                .write_all(line.as_bytes())
                .await
                .map_err(|e| AcpClientError::Rpc(e.to_string()))?;
            stdin
                .flush()
                .await
                .map_err(|e| AcpClientError::Rpc(e.to_string()))?;
        }

        rx.await.map_err(|_| AcpClientError::ProcessExited)
    }

    fn parts_to_acp_blocks(parts: &[Part]) -> Vec<Value> {
        parts
            .iter()
            .map(|part| match part {
                Part::Text { text } => json!({ "kind": "text", "text": text }),
                Part::Json { value } => json!({ "kind": "data", "json": value }),
                Part::FileRef { uri, mime_type } => json!({
                    "kind": "resource", "uri": uri, "mimeType": mime_type,
                }),
            })
            .collect()
    }
} // <-- ЕДИНСТВЕННАЯ закрывающая скобка impl AcpClientDriver, ничего не дублируется

impl Drop for AcpClientDriver {
    fn drop(&mut self) {
        self.reader_task.abort();
    }
} // <-- ЕДИНСТВЕННАЯ закрывающая скобка impl Drop — раньше здесь была лишняя '}', убрана

// ============================================================
// Свободные функции — ФИКС 2: раньше преждевременный '}' выше закрывал
// модуль, из-за чего всё это оказывалось невидимым для impl AgentDriver
// ниже. Теперь все они в одном module scope с impl AgentDriver.
// ============================================================

fn spawn_reader_loop(
    stdout: tokio::process::ChildStdout,
    pending: PendingRequests,
    active_tasks: Arc<DashMap<TaskId, ActiveAcpTask>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let Ok(value) = serde_json::from_str::<Value>(&line) else {
                        tracing::warn!(line = %line, "ACP client: unparseable line from child stdout");
                        continue;
                    };

                    if let Some(id) = value.get("id").and_then(|v| v.as_u64()) {
                        if let Some((_, tx)) = pending.remove(&id) {
                            let result = value.get("result").cloned().unwrap_or(Value::Null);
                            let _ = tx.send(result);
                        }
                    } else if let Some(method) = value.get("method").and_then(|v| v.as_str()) {
                        tracing::debug!(
                            method,
                            "received unhandled ACP push notification (v1 uses polling instead)"
                        );
                    }
                }
                Ok(None) => {
                    tracing::warn!("ACP client: child stdout closed (process likely exited)");
                    break;
                }
                Err(e) => {
                    tracing::error!(error = %e, "ACP client: stdout read error");
                    break;
                }
            }
        }

        for entry in active_tasks.iter() {
            let _ = entry
                .value()
                .event_tx
                .send(DriverEvent::Failed(PublicError {
                    code: "acp_process_exited".into(),
                    message: "the ACP agent child process terminated unexpectedly".into(),
                    retryable: false,
                }))
                .await;
        }
    })
}

/// ФИКС 3: сигнатура `&Value -> Option<Part>`, вызывается через `.iter()`
/// без `.cloned()` в filter_map — типы совпадают, без лишних аллокаций.
///
/// БЫЛО (баг): `.filter_map(acp_block_to_part)` где acp_block_to_part был
/// `fn(&Value)`, но итератор давал owned `Value` из `.cloned()` перед этим —
/// типы не совпадали (fn(&Value) не реализует FnMut(Value)).
///
/// СТАЛО: итерируем `&Value` напрямую (`events.iter()`, не
/// `events.iter().cloned()`), acp_block_to_part принимает `&Value`.
fn acp_block_to_part(block: &Value) -> Option<Part> {
    let kind = block.get("kind")?.as_str()?;
    match kind {
        "text" => {
            let text = block.get("text")?.as_str()?.to_string();
            Some(Part::Text { text })
        }
        "resource" => {
            let uri = block.get("uri")?.as_str()?.to_string();
            let mime_type = block
                .get("mimeType")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            Some(Part::FileRef { uri, mime_type })
        }
        "data" => {
            let value = block.get("json")?.clone();
            Some(Part::Json { value })
        }
        _ => None,
    }
}

/// Извлекает output parts из события Completed session/update ответа —
/// использует acp_block_to_part через .iter(), не .into_iter().cloned(),
/// это то место, где раньше был баг #3.
fn extract_output_parts(event: &Value) -> Vec<Part> {
    event
        .get("output")
        .and_then(|v| v.as_array())
        .map(|blocks| blocks.iter().filter_map(acp_block_to_part).collect())
        .unwrap_or_default()
}

#[async_trait]
impl AgentDriver for AcpClientDriver {
    fn id(&self) -> &str {
        &self.id
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            cancellation: true,
            provide_input: true,
        }
    }

    async fn health(&self) -> Result<(), CoreError> {
        self.call("initialize", json!({}))
            .await
            .map(|_| ())
            .map_err(|e| CoreError::Driver(format!("ACP health check failed: {e}")))
    }

    async fn invoke(
        &self,
        task_id: TaskId,
        request: InvokeRequest,
    ) -> Result<mpsc::Receiver<DriverEvent>, CoreError> {
        let session_response = self
            .call("session/new", json!({}))
            .await
            .map_err(|e| CoreError::Driver(format!("session/new failed: {e}")))?;
        let session_id = session_response
            .get("sessionId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let (tx, rx) = mpsc::channel(32);
        self.active_tasks.insert(
            task_id,
            ActiveAcpTask {
                event_tx: tx.clone(),
                session_id: session_id.clone(),
            },
        );

        let prompt_params = json!({
            "sessionId": session_id.clone().unwrap_or_default(),
            "requestId": task_id.to_string(),
            "prompt": Self::parts_to_acp_blocks(&request.input),
            "metadata": request.context,
        });

        let call_result = self.call("session/prompt", prompt_params).await;

        let poll_task_id = task_id;
        let poll_pending = self.pending.clone();
        let poll_child_stdin = self.child_stdin.clone();
        let poll_next_id = self.next_request_id.clone();
        let poll_active_tasks = self.active_tasks.clone();

        tokio::spawn(async move {
            match call_result {
                Ok(_) => {
                    let _ = tx.send(DriverEvent::Accepted).await;
                }
                Err(e) => {
                    let _ = tx
                        .send(DriverEvent::Failed(PublicError {
                            code: "acp_prompt_failed".into(),
                            message: e.to_string(),
                            retryable: true,
                        }))
                        .await;
                    poll_active_tasks.remove(&poll_task_id);
                    return;
                }
            }

            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            let mut last_seq: u64 = 0;
            loop {
                interval.tick().await;

                let id = poll_next_id.fetch_add(1, Ordering::SeqCst);
                let (rtx, rrx) = oneshot::channel();
                poll_pending.insert(id, rtx);
                let request = json!({
                    "jsonrpc": "2.0", "id": id, "method": "session/update",
                    "params": { "taskId": poll_task_id.to_string() },
                });
                let mut line = serde_json::to_string(&request).unwrap_or_default();
                line.push('\n');
                {
                    let mut stdin = poll_child_stdin.lock().await;
                    if stdin.write_all(line.as_bytes()).await.is_err() {
                        break;
                    }
                    let _ = stdin.flush().await;
                }

                let Ok(response) =
                    tokio::time::timeout(std::time::Duration::from_secs(5), rrx).await
                else {
                    continue;
                };
                let Ok(response) = response else { break };
                let Some(events) = response.get("events").and_then(|v| v.as_array()) else {
                    continue;
                };

                for event in events {
                    let seq = event.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
                    if seq <= last_seq {
                        continue;
                    }
                    last_seq = seq;

                    let kind = event.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                    let driver_event = match kind {
                        "Progress" => Some(DriverEvent::Progress {
                            message: event
                                .get("message")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            percent: event
                                .get("percent")
                                .and_then(|v| v.as_u64())
                                .map(|p| p as u8),
                        }),
                        "InputRequired" => Some(DriverEvent::InputRequired(InputRequest {
                            question: event
                                .get("question")
                                .and_then(|v| v.as_str())
                                .unwrap_or("input required")
                                .to_string(),
                            schema: None,
                        })),
                        // ФИКС 3 применён здесь: extract_output_parts использует
                        // acp_block_to_part(&Value) через .iter(), не .cloned().
                        "Completed" => Some(DriverEvent::Completed(extract_output_parts(event))),
                        "Failed" => Some(DriverEvent::Failed(PublicError {
                            code: "acp_task_failed".into(),
                            message: event
                                .get("error")
                                .and_then(|v| v.get("message"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("remote ACP task failed")
                                .to_string(),
                            retryable: false,
                        })),
                        "Cancelled" => Some(DriverEvent::Cancelled),
                        _ => None,
                    };

                    let is_terminal = matches!(kind, "Completed" | "Failed" | "Cancelled");
                    if let Some(de) = driver_event {
                        let _ = tx.send(de).await;
                    }
                    if is_terminal {
                        poll_active_tasks.remove(&poll_task_id);
                        return;
                    }
                }
            }

            poll_active_tasks.remove(&poll_task_id);
        });

        Ok(rx)
    }

    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError> {
        self.call("session/cancel", json!({ "taskId": task_id.to_string() }))
            .await
            .map(|_| ())
            .map_err(|e| CoreError::Driver(format!("session/cancel failed: {e}")))
    }

    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<(), CoreError> {
        self.call(
            "session/input",
            json!({
                "taskId": task_id.to_string(),
                "prompt": Self::parts_to_acp_blocks(&input),
            }),
        )
        .await
        .map(|_| ())
        .map_err(|e| CoreError::Driver(format!("session/input failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parts_to_acp_blocks_maps_text() {
        let parts = vec![Part::Text { text: "hi".into() }];
        let blocks = AcpClientDriver::parts_to_acp_blocks(&parts);
        assert_eq!(blocks[0]["kind"], "text");
    }

    #[test]
    fn acp_block_to_part_roundtrips_text() {
        let block = json!({ "kind": "text", "text": "hello" });
        let part = acp_block_to_part(&block).unwrap();
        match part {
            Part::Text { text } => assert_eq!(text, "hello"),
            _ => panic!("expected Text"),
        }
    }

    #[test]
    fn acp_block_to_part_roundtrips_resource() {
        let block = json!({ "kind": "resource", "uri": "file:///a", "mimeType": "text/plain" });
        let part = acp_block_to_part(&block).unwrap();
        match part {
            Part::FileRef { uri, mime_type } => {
                assert_eq!(uri, "file:///a");
                assert_eq!(mime_type.as_deref(), Some("text/plain"));
            }
            _ => panic!("expected FileRef"),
        }
    }

    #[test]
    fn acp_block_to_part_returns_none_for_unknown_kind() {
        let block = json!({ "kind": "unknown-future-kind" });
        assert!(acp_block_to_part(&block).is_none());
    }

    #[test]
    fn extract_output_parts_uses_filter_map_without_type_mismatch() {
        // Регрессионный тест ИМЕННО на фикс #3: если сигнатура снова
        // разойдётся (например, кто-то поменяет acp_block_to_part на
        // принимающий owned Value), этот тест не скомпилируется — что и
        // требуется, filter_map(acp_block_to_part) должно типизироваться
        // само по себе без явного closure-обёртки.
        let event = json!({
            "output": [
                { "kind": "text", "text": "result" },
                { "kind": "unknown" },
                { "kind": "resource", "uri": "file:///out.txt" },
            ]
        });
        let parts = extract_output_parts(&event);
        assert_eq!(
            parts.len(),
            2,
            "unknown kind must be filtered out, not error"
        );
    }
}

// ============================================================
// ФИКС 1 — Cargo.toml diff (crates/driver-acp-client/Cargo.toml)
// ============================================================
//
// БЫЛО (не хватало features, отсюда unresolved imports):
//   tokio = { workspace = true }
//
// СТАЛО:
//   [dependencies]
//   tokio = { workspace = true, features = ["process", "io-util", "io-std"] }
//
// Пояснение: workspace-level tokio.workspace даёт [macros, rt-multi-thread,
// sync, time] по умолчанию (то, что нужно большинству крейтов). process
// (для tokio::process::Command/Child), io-util (AsyncReadExt/AsyncWriteExt,
// write_all/flush) и io-std (BufReader работающий с std-совместимыми
// потоками/lines()) — специфичны для этого крейта, который единственный
// в workspace spawns child-процессы и парсит их stdio построчно. Cargo
// объединяет workspace-inherited features с локально добавленными, не
// требует повторения уже унаследованных.
