//! crates/driver-a2a-client/src/lib.rs
//!
//! Точка входа драйвера. Диспетчеризация по wire_format происходит один
//! раз в new() — дальше вся логика invoke/get/cancel работает только с
//! NormalizedTask и не знает, какой диалект JSON-RPC используется.
//!
//! Этот файл = чистовой клиент (docs/design/lib_driver.rs) + обёртка
//! `AgentDriver` (адаптер к `adapter_core`): чистовой клиент оперирует
//! NormalizedTask, а `AgentDriver` транслирует его в DriverEvent.

pub mod error;
pub mod wire;

use adapter_core::{AgentDriver, CoreError, DriverCapabilities, DriverEvent};
use adapter_model::{InvokeRequest, Part, PublicError, TaskId};
use async_trait::async_trait;
use error::{from_jsonrpc_error, A2aClientError};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wire::{
    sdk::A2aSdkWire, spec::A2aSpecWire, A2aOperation, A2aWire, NormalizedPart, NormalizedState,
    NormalizedTask,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum A2aWireFormat {
    #[default]
    Sdk,
    Spec,
}

#[derive(Clone, Debug)]
pub struct A2aClientConfig {
    pub endpoint: String,
    pub token: Option<String>,
    pub wire_format: A2aWireFormat,
    pub timeout_secs: u64,
}

impl Default for A2aClientConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            token: None,
            wire_format: A2aWireFormat::default(),
            timeout_secs: 30,
        }
    }
}

/// Активные запросы: TaskId -> CancellationToken (для локальной отмены).
type ActiveHandles = Arc<dashmap::DashMap<TaskId, CancellationToken>>;

/// Чистовой A2A-клиент: wire-формат-нейтрален, оперирует NormalizedTask.
pub struct A2aClientDriver {
    config: A2aClientConfig,
    client: reqwest::Client,
    wire: Arc<dyn A2aWire>,
    active: ActiveHandles,
}

impl A2aClientDriver {
    pub fn new(config: A2aClientConfig) -> Result<Self, A2aClientError> {
        let wire: Arc<dyn A2aWire> = match config.wire_format {
            A2aWireFormat::Sdk => Arc::new(A2aSdkWire),
            A2aWireFormat::Spec => Arc::new(A2aSpecWire),
        };

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| A2aClientError::Http(e.to_string()))?;

        Ok(Self {
            config,
            client,
            wire,
            active: Arc::new(dashmap::DashMap::new()),
        })
    }

    /// Отправляет новое сообщение (SendMessage / message/send в зависимости
    /// от wire_format). task_id передаётся, если это продолжение диалога.
    pub async fn invoke(
        &self,
        text: &str,
        context_id: Option<&str>,
        task_id: Option<&str>,
    ) -> Result<NormalizedTask, A2aClientError> {
        let parts = vec![NormalizedPart::text(text)];
        let op = A2aOperation::SendMessage {
            parts: &parts,
            context_id,
            task_id,
        };
        self.execute(op).await
    }

    /// Опрашивает статус существующей задачи (GetTask / tasks/get) — нужен
    /// для continuation после InputRequired без повторной отправки message.
    pub async fn get_task(&self, task_id: &str) -> Result<NormalizedTask, A2aClientError> {
        self.execute(A2aOperation::GetTask { task_id }).await
    }

    /// Отменяет задачу (CancelTask / tasks/cancel).
    pub async fn cancel_task(&self, task_id: &str) -> Result<NormalizedTask, A2aClientError> {
        self.execute(A2aOperation::CancelTask { task_id }).await
    }

    async fn execute(&self, op: A2aOperation<'_>) -> Result<NormalizedTask, A2aClientError> {
        let method = self.wire.jsonrpc_method(&op);
        let params = self.wire.build_params(&op);

        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let mut req = self.client.post(&self.config.endpoint).json(&payload);
        if let Some(token) = &self.config.token {
            req = req.bearer_auth(token);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| A2aClientError::Http(e.to_string()))?;

        let body: Value = resp
            .json()
            .await
            .map_err(|e| A2aClientError::Http(e.to_string()))?;

        if let Some(err) = body.get("error") {
            let code = err.get("code").and_then(Value::as_i64).unwrap_or(-32000);
            let message = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            return Err(from_jsonrpc_error(code, message, method, self.wire.name()));
        }

        let result = body.get("result").ok_or_else(|| {
            A2aClientError::ProtocolError("missing 'result' in JSON-RPC response".into())
        })?;

        self.wire.parse_task(result)
    }

    /// Нормализованный Part -> adapter_model::Part (текст/uri; raw пропускаем).
    fn normalized_to_part(p: &NormalizedPart) -> Part {
        if let Some(text) = &p.text {
            Part::Text { text: text.clone() }
        } else if let Some(uri) = &p.uri {
            Part::FileRef {
                uri: uri.clone(),
                mime_type: p.mime_type.clone(),
            }
        } else {
            Part::Text {
                text: String::new(),
            }
        }
    }

    /// NormalizedTask -> DriverEvent (терминальное событие).
    fn task_to_terminal(task: &NormalizedTask) -> DriverEvent {
        match task.state {
            NormalizedState::Completed => {
                let parts = task
                    .output_parts
                    .iter()
                    .map(Self::normalized_to_part)
                    .collect();
                DriverEvent::Completed(parts)
            }
            NormalizedState::Failed | NormalizedState::Rejected => {
                DriverEvent::Failed(PublicError {
                    code: "a2a_task_failed".into(),
                    message: task
                        .status_message
                        .clone()
                        .unwrap_or_else(|| "A2A task failed".into()),
                    retryable: false,
                })
            }
            NormalizedState::Canceled => DriverEvent::Cancelled,
            NormalizedState::InputRequired | NormalizedState::AuthRequired => {
                DriverEvent::InputRequired(adapter_model::InputRequest {
                    question: task
                        .status_message
                        .clone()
                        .unwrap_or_else(|| "input required".into()),
                    schema: None,
                })
            }
            _ => DriverEvent::Progress {
                message: task.status_message.clone().unwrap_or_default(),
                percent: None,
            },
        }
    }
}

// ============================================================
// AgentDriver — обёртка над чистовым клиентом
// ============================================================

#[async_trait]
impl AgentDriver for A2aClientDriver {
    fn id(&self) -> &str {
        // Для A2A-клиента id не входит в чистовой клиент (там только config).
        // Используем endpoint как человекочитаемый id.
        &self.config.endpoint
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            cancellation: true,
            provide_input: true,
        }
    }

    async fn health(&self) -> Result<(), CoreError> {
        // Пустое сообщение — сервер вернёт task (или понятную ошибку).
        self.invoke("", None, None)
            .await
            .map(|_| ())
            .map_err(|e| CoreError::Driver(format!("A2A health check failed: {e}")))
    }

    async fn invoke(
        &self,
        task_id: TaskId,
        request: InvokeRequest,
    ) -> Result<mpsc::Receiver<DriverEvent>, CoreError> {
        let cancel_token = CancellationToken::new();
        self.active.insert(task_id, cancel_token.clone());

        let (tx, rx) = mpsc::channel(32);
        // Клиент не Clone (reqwest Client и wire — Arc, но struct не Clone).
        // Строим локального вызовчика из полей.
        let driver = self.clone_state();

        tokio::spawn(async move {
            let _ = tx.send(DriverEvent::Accepted).await;

            let text = text_of_request(&request.input);
            let session = request.session_id.map(|s| s.to_string());
            let task_id_str = task_id.to_string();
            let outcome = tokio::select! {
                result = driver.invoke(&text, session.as_deref(), Some(&task_id_str)) => {
                    match result {
                        Ok(task) => {
                            let event = A2aClientDriver::task_to_terminal(&task);
                            let _ = tx.send(event).await;
                        }
                        Err(e) => {
                            let _ = tx.send(DriverEvent::Failed(PublicError {
                                code: "a2a_call_failed".into(),
                                message: e.to_string(),
                                retryable: false,
                            })).await;
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    let _ = tx.send(DriverEvent::Cancelled).await;
                }
            };
            let _ = outcome;
            driver.active.remove(&task_id);
        });

        Ok(rx)
    }

    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError> {
        // Локальная отмена приоритетна; HTTP-отмена — best-effort.
        if let Some(entry) = self.active.get(&task_id) {
            entry.value().cancel();
        }
        let _ = self.cancel_task(&task_id.to_string()).await;
        Ok(())
    }

    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<(), CoreError> {
        // A2A multi-turn: повторный message/send с тем же task_id.
        let text = text_of_request(&input);
        self.invoke(&text, None, Some(&task_id.to_string()))
            .await
            .map(|_| ())
            .map_err(|e| CoreError::Driver(format!("A2A provide_input failed: {e}")))
    }
}

impl A2aClientDriver {
    fn clone_state(&self) -> Self {
        Self {
            config: self.config.clone(),
            client: self.client.clone(),
            wire: self.wire.clone(),
            active: self.active.clone(),
        }
    }
}

/// Собирает текст из входных Part (для SendMessage драйвер работает с
/// простым текстом; Part::Json/FileRef в этой версии не передаются как
/// аргументы tool-вызова).
fn text_of_request(input: &[Part]) -> String {
    input
        .iter()
        .filter_map(|part| match part {
            Part::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}
