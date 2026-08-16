//! `driver-mcp` — MCP client backend для `adapter_core::AgentDriver`.
//!
//! Перенесено из `docs/design/driver_mcp_TRULY_FINAL.rs` (API-verified против
//! github.com/modelcontextprotocol/rust-sdk, commit f713ebd1a6feab492fb730a8bc13026be114d82f,
//! см. `docs/design/rmcp-api-verification.md`).
//!
//! Доработка относительно референса — закрытие единственного ownership-пробела
//! в `cancel()` (вариант (б) из комментария в референсе):
//! `RequestHandle` целиком живёт внутри spawn'нутой задачи, снаружи через
//! `active_handles: DashMap<TaskId, CancellationToken>` передаётся только сигнал
//! отмены. `cancel()` снаружи вызывает `cancel_token.cancel()`, а внутри задачи
//! `tokio::select!` реагирует на это и вызывает `handle.cancel(reason)` там, где
//! handle реально доступен. Moved-value конфликт устранён без unsafe.

use adapter_core::{AgentDriver, CoreError, DriverCapabilities, DriverEvent};
use adapter_model::{InvokeRequest, Part, PublicError, TaskId};
use async_trait::async_trait;
use dashmap::DashMap;
use futures_util::StreamExt;
use rmcp::{
    handler::client::{progress::ProgressDispatcher, ClientHandler},
    model::{
        CallToolRequest, CallToolRequestParam, ClientRequest, Content, PaginatedRequestParam,
        ProgressNotificationParam, RawContent, ServerResult,
    },
    service::{NotificationContext, PeerRequestOptions, RequestHandle, RoleClient},
    transport::TokioChildProcess,
    ServiceError, ServiceExt,
};
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{process::Command, sync::mpsc};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct McpStdioConfig {
    pub command: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}

#[derive(thiserror::Error, Debug)]
pub enum McpDriverError {
    #[error("failed to spawn MCP stdio transport: {0}")]
    Spawn(String),
    #[error("MCP initialize/connect failed: {0}")]
    Connect(String),
    #[error("MCP tools/list failed: {0}")]
    ToolsList(String),
}

/// `ClientHandler` — тонкая обёртка над встроенным SDK `ProgressDispatcher`.
/// Никакого самодельного `DashMap<String, Sender>` больше не требуется:
/// SDK сам маршрутизирует notification к нужному подписчику по токену.
#[derive(Clone, Default)]
struct McpClientHandler {
    progress: ProgressDispatcher,
}

impl ClientHandler for McpClientHandler {
    async fn on_progress(
        &self,
        params: ProgressNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) {
        self.progress.handle_notification(params).await;
    }
}

type McpSession = rmcp::service::RunningService<RoleClient, McpClientHandler>;

pub struct McpDriver {
    id: String,
    session: Arc<McpSession>,
    handler: McpClientHandler,
    tool_names: Arc<tokio::sync::RwLock<Vec<String>>>,
    allowed_tools: Vec<String>,
    default_timeout: Duration,
    /// Активные запросы: TaskId -> CancellationToken. RequestHandle целиком
    /// живёт внутри spawn'нутой задачи; снаружи только токен отмены, чтобы
    /// `cancel()` не трогал moved-значение.
    active_handles: Arc<DashMap<TaskId, CancellationToken>>,
}

impl McpDriver {
    pub async fn connect_stdio(
        id: impl Into<String>,
        config: McpStdioConfig,
        allowed_tools: Vec<String>,
        default_timeout: Duration,
    ) -> Result<Self, McpDriverError> {
        let mut command = Command::new(&config.command);
        command.args(&config.args).envs(&config.env);
        let transport =
            TokioChildProcess::new(command).map_err(|e| McpDriverError::Spawn(e.to_string()))?;

        let handler = McpClientHandler::default();

        // Подтверждённая форма (progress_client.rs): serve() на handler'е.
        // ClientHandler требует Clone в этой реализации (McpClientHandler
        // выше derive(Clone)), поэтому клонируем перед передачей в serve(),
        // сохраняя оригинал у себя для последующего subscribe().
        let session = handler
            .clone()
            .serve(transport)
            .await
            .map_err(|e| McpDriverError::Connect(e.to_string()))?;

        let driver = Self {
            id: id.into(),
            session: Arc::new(session),
            handler,
            tool_names: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            allowed_tools,
            default_timeout,
            active_handles: Arc::new(DashMap::new()),
        };

        driver.discover_tools().await?;
        Ok(driver)
    }

    async fn discover_tools(&self) -> Result<(), McpDriverError> {
        let mut cursor: Option<PaginatedRequestParam> = None;
        let mut discovered = Vec::new();
        loop {
            let page = self
                .session
                .list_tools(cursor.take())
                .await
                .map_err(|e: ServiceError| McpDriverError::ToolsList(e.to_string()))?;
            for tool in page.tools {
                let name = tool.name.to_string();
                if self.allowed_tools.is_empty() || self.allowed_tools.contains(&name) {
                    discovered.push(name);
                }
            }
            match page.next_cursor {
                Some(next) => cursor = Some(PaginatedRequestParam { cursor: Some(next) }),
                None => break,
            }
        }
        *self.tool_names.write().await = discovered;
        Ok(())
    }

    fn part_to_arguments(&self, input: &[Part]) -> serde_json::Map<String, serde_json::Value> {
        let mut merged = serde_json::Map::new();
        for part in input {
            match part {
                Part::Text { text } => {
                    merged.insert("input".to_string(), serde_json::Value::String(text.clone()));
                }
                Part::Json { value } => {
                    if let serde_json::Value::Object(map) = value {
                        merged.extend(map.clone());
                    } else {
                        merged.insert("value".to_string(), value.clone());
                    }
                }
                Part::FileRef { uri, mime_type } => {
                    merged.insert(
                        "resource".to_string(),
                        serde_json::json!({ "uri": uri, "mimeType": mime_type }),
                    );
                }
            }
        }
        merged
    }
}

fn content_blocks_to_parts(blocks: Vec<Content>) -> Vec<Part> {
    blocks
        .into_iter()
        .map(|block| match block.raw {
            RawContent::Text(text_content) => Part::Text {
                text: text_content.text,
            },
            RawContent::Image(image_content) => Part::Json {
                value: serde_json::json!({
                    "mimeType": image_content.mime_type,
                    "data": image_content.data,
                }),
            },
            other => Part::Json {
                value: serde_json::to_value(&other).unwrap_or(serde_json::Value::Null),
            },
        })
        .collect()
}

#[async_trait]
impl AgentDriver for McpDriver {
    fn id(&self) -> &str {
        &self.id
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            cancellation: true,
            provide_input: false,
        }
    }

    async fn health(&self) -> Result<(), CoreError> {
        self.session
            .list_tools(None)
            .await
            .map(|_| ())
            .map_err(|e| CoreError::Driver(format!("MCP health check failed: {e}")))
    }

    async fn invoke(
        &self,
        task_id: TaskId,
        request: InvokeRequest,
    ) -> Result<mpsc::Receiver<DriverEvent>, CoreError> {
        let skill_id = request
            .skill_id
            .clone()
            .ok_or_else(|| CoreError::InvalidRequest("skill_id required for MCP driver".into()))?;

        {
            let names = self.tool_names.read().await;
            if !names.iter().any(|n| n == &skill_id) {
                return Err(CoreError::InvalidRequest(format!(
                    "unknown or disallowed MCP tool: {skill_id}"
                )));
            }
        }

        let arguments = self.part_to_arguments(&request.input);
        let params = CallToolRequestParam {
            name: skill_id.clone().into(),
            arguments: Some(arguments),
        };

        // Подтверждённый путь с progress: peer.send_request_with_option(...)
        // -> RequestHandle. SDK сам генерирует progress_token — подписка на
        // handle.progress_token ПОСЛЕ отправки, не заранее.
        let options = PeerRequestOptions::default();
        let handle: RequestHandle<RoleClient> = self
            .session
            .peer()
            .send_request_with_option(
                ClientRequest::CallToolRequest(CallToolRequest::new(params)),
                options,
            )
            .await
            .map_err(|e: ServiceError| CoreError::Driver(e.to_string()))?;

        // Вариант (б): снаружи храним только сигнал отмены, RequestHandle
        // остаётся внутри spawn'нутой задачи.
        let cancel_token = CancellationToken::new();
        self.active_handles.insert(task_id, cancel_token.clone());

        let mut progress_subscriber = self
            .handler
            .progress
            .subscribe(handle.progress_token.clone())
            .await;

        let (tx, rx) = mpsc::channel(32);
        let timeout = self.default_timeout;
        let active_handles = self.active_handles.clone();

        // handle.cancel(self, reason) потребляет RequestHandle, а его нужно
        // отдать в await_response() в основной ветке select!. Поэтому
        // клонируем peer (Clone) и id до этого и в cancel-ветке отправляем
        // CancelledNotification вручную — 1:1 код handle.cancel() из SDK
        // (service.rs:655 / 0.8.5 service.rs:281).
        let cancel_peer = handle.peer.clone();
        let cancel_request_id = handle.id.clone();

        tokio::spawn(async move {
            let _ = tx.send(DriverEvent::Accepted).await;

            // Слушаем progress events параллельно с ожиданием финального
            // ответа. ProgressSubscriber — Stream, Drop = автоотписка, так
            // что просто выходим из scope в конце — не нужен явный unsubscribe.
            let progress_task = {
                let tx = tx.clone();
                tokio::spawn(async move {
                    while let Some(update) = progress_subscriber.next().await {
                        let percent = match (
                            update.progress,
                            /* total, если есть в модели */ None::<f64>,
                        ) {
                            (p, Some(total)) if total > 0.0 => Some(((p / total) * 100.0) as u8),
                            _ => None,
                        };
                        let message = format!("progress: {}", update.progress);
                        let _ = tx.send(DriverEvent::Progress { message, percent }).await;
                    }
                })
            };

            // RequestHandle доступен здесь — именно тут вызываем handle.cancel()
            // при внешнем сигнале отмены через cancel_token.
            let outcome = tokio::select! {
                result = tokio::time::timeout(timeout, handle.await_response()) => {
                    match result {
                        Ok(Ok(ServerResult::CallToolResult(call_result))) => {
                            if call_result.is_error.unwrap_or(false) {
                                let message = call_result
                                    .content
                                    .iter()
                                    .find_map(|block| block.as_text().map(|t| t.text.clone()))
                                    .unwrap_or_else(|| "MCP tool returned an error".to_string());
                                DriverEvent::Failed(PublicError {
                                    code: "mcp_tool_error".into(),
                                    message,
                                    retryable: false,
                                })
                            } else {
                                let parts = content_blocks_to_parts(call_result.content);
                                DriverEvent::Completed(parts)
                            }
                        }
                        Ok(Ok(other)) => DriverEvent::Failed(PublicError {
                            code: "mcp_unexpected_response".into(),
                            message: format!("unexpected MCP response: {other:?}"),
                            retryable: false,
                        }),
                        Ok(Err(service_error)) => DriverEvent::Failed(PublicError {
                            code: "mcp_call_failed".into(),
                            message: service_error.to_string(),
                            retryable: false,
                        }),
                        Err(_elapsed) => DriverEvent::Failed(PublicError {
                            code: "timeout".into(),
                            message: "MCP tool call timed out".into(),
                            retryable: false,
                        }),
                    }
                }
                _ = cancel_token.cancelled() => {
                    let notification = rmcp::model::CancelledNotification::new(
                        rmcp::model::CancelledNotificationParam {
                            request_id: cancel_request_id.clone(),
                            reason: Some("cancelled by adapter-core".to_string()),
                        },
                    );
                    let _ = cancel_peer.send_notification(notification.into()).await;
                    DriverEvent::Cancelled
                }
            };

            progress_task.abort();
            active_handles.remove(&task_id);
            let _ = tx.send(outcome).await;
        });

        Ok(rx)
    }

    async fn cancel(&self, task_id: TaskId) -> Result<(), CoreError> {
        // Снаружи — только сигнал отмены. Сам handle.cancel(reason) выполнит
        // spawn'нутая задача в ветке `_ = cancel_token.cancelled()`, где
        // RequestHandle доступен. handle.cancel сам шлёт notifications/cancelled
        // с корректным request_id — отдельный notify_cancelled не нужен.
        if let Some(entry) = self.active_handles.get(&task_id) {
            entry.value().cancel();
        }
        Ok(())
    }

    async fn provide_input(&self, _task_id: TaskId, _input: Vec<Part>) -> Result<(), CoreError> {
        Err(CoreError::InvalidRequest(
            "MCP driver does not support mid-call input in this version".into(),
        ))
    }
}
