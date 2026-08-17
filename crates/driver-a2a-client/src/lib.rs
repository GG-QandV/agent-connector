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
use dashmap::DashMap;
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

/// Маппинг core TaskId -> удалённый (remote) task id, который сервер вернул
/// в ответ на первый SendMessage. Нужен cancel/provide_input, чтобы они
/// обращались к правильному remote-заданию, а не к core id драйвера.
type RemoteTaskIds = Arc<DashMap<TaskId, String>>;
/// Локальные сигналы отмены: core TaskId -> CancellationToken.
type CancellationTokens = Arc<DashMap<TaskId, CancellationToken>>;

/// Чистовой A2A-клиент: wire-формат-нейтрален, оперирует NormalizedTask.
pub struct A2aClientDriver {
    config: A2aClientConfig,
    client: reqwest::Client,
    wire: Arc<dyn A2aWire>,
    remote_task_ids: RemoteTaskIds,
    cancellation_tokens: CancellationTokens,
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
            remote_task_ids: Arc::new(DashMap::new()),
            cancellation_tokens: Arc::new(DashMap::new()),
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
        self.send_parts(&parts, context_id, task_id).await
    }

    /// Отправляет части (включая Json/FileRef) как сообщение. Внутренний
    /// примитив, на котором держится invoke(text) и provide_input.
    async fn send_parts(
        &self,
        parts: &[NormalizedPart],
        context_id: Option<&str>,
        task_id: Option<&str>,
    ) -> Result<NormalizedTask, A2aClientError> {
        let op = A2aOperation::SendMessage {
            parts,
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

    /// Core-части (Text/Json/FileRef) -> NormalizedPart. Json сериализуется
    /// в текст, FileRef сохраняет uri/mime_type — ничего не теряется.
    fn core_parts_to_normalized(parts: &[Part]) -> Vec<NormalizedPart> {
        parts
            .iter()
            .map(|p| match p {
                Part::Text { text } => NormalizedPart::text(text.clone()),
                Part::Json { value } => NormalizedPart::text(value.to_string()),
                Part::FileRef { uri, mime_type } => NormalizedPart {
                    text: None,
                    raw: None,
                    uri: Some(uri.clone()),
                    mime_type: mime_type.clone(),
                },
            })
            .collect()
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

    fn is_terminal(state: &NormalizedState) -> bool {
        matches!(
            state,
            NormalizedState::Completed
                | NormalizedState::Failed
                | NormalizedState::Canceled
                | NormalizedState::Rejected
        )
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
        self.cancellation_tokens
            .insert(task_id, cancel_token.clone());

        let (tx, rx) = mpsc::channel(32);
        // Клиент не Clone (reqwest Client и wire — Arc, но struct не Clone).
        // Строим локального вызовчика из полей.
        let driver = self.clone_state();
        let remote_task_ids = self.remote_task_ids.clone();
        let cancellation_tokens = self.cancellation_tokens.clone();

        tokio::spawn(async move {
            let _ = tx.send(DriverEvent::Accepted).await;

            let normalized_parts = A2aClientDriver::core_parts_to_normalized(&request.input);
            let session = request.session_id.map(|s| s.to_string());

            // Первый SendMessage идёт без remote task_id: сервер создаёт
            // задание и возвращает его id в ответе.
            let outcome = tokio::select! {
                result = driver.send_parts(&normalized_parts, session.as_deref(), None) => {
                    match result {
                        Ok(task) => {
                            let remote_id = task.id.clone();
                            remote_task_ids.insert(task_id, remote_id);
                            let terminal = A2aClientDriver::is_terminal(&task.state);
                            let event = A2aClientDriver::task_to_terminal(&task);
                            let _ = tx.send(event).await;
                            if terminal {
                                remote_task_ids.remove(&task_id);
                                cancellation_tokens.remove(&task_id);
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(DriverEvent::Failed(PublicError {
                                code: "a2a_call_failed".into(),
                                message: e.to_string(),
                                retryable: false,
                            })).await;
                            cancellation_tokens.remove(&task_id);
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    let _ = tx.send(DriverEvent::Cancelled).await;
                    remote_task_ids.remove(&task_id);
                    cancellation_tokens.remove(&task_id);
                }
            };
            let _ = outcome;
        });

        Ok(rx)
    }

    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError> {
        // Локальная отмена приоритетна; HTTP-отмена — best-effort.
        let remote_id = self
            .remote_task_ids
            .get(&task_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| {
                CoreError::Driver(format!("no active A2A task for core task_id={task_id}"))
            })?;

        if let Some(token_entry) = self.cancellation_tokens.get(&task_id) {
            token_entry.value().cancel();
        }

        let result = self
            .cancel_task(&remote_id)
            .await
            .map(|_| ())
            .map_err(|e| CoreError::Driver(e.to_string()));
        self.remote_task_ids.remove(&task_id);
        self.cancellation_tokens.remove(&task_id);
        result
    }

    async fn provide_input(&self, task_id: TaskId, input: Vec<Part>) -> Result<(), CoreError> {
        // A2A multi-turn: повторный message/send с тем же remote task_id.
        let remote_id = self
            .remote_task_ids
            .get(&task_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| {
                CoreError::Driver(format!("no active A2A task for core task_id={task_id}"))
            })?;

        let normalized_parts = Self::core_parts_to_normalized(&input);
        self.send_parts(&normalized_parts, None, Some(&remote_id))
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
            remote_task_ids: self.remote_task_ids.clone(),
            cancellation_tokens: self.cancellation_tokens.clone(),
        }
    }
}
