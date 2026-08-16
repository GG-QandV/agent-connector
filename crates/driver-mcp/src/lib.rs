//! `driver-mcp` — MCP client backend для `adapter_core::AgentDriver`.
//!
//! Реализует `AgentDriver` поверх MCP client-соединения к внешнему MCP-серверу.
//! Поддерживаются два транспорта: stdio (child-процесс) и Streamable HTTP
//! (rmcp 0.8.5, reqwest). Progress транслируется в `DriverEvent::Progress`,
//! отмена — через `CancellationToken`, который `tokio::select!` внутри
//! spawn'нутой задачи превращает в `notifications/cancelled`.
//!
//! Дополнительно к базовому циклу invoke/complete/cancel:
//! - проверка версии MCP-протокола сервера после `initialize`;
//! - валидация `request.input` против сохранённой `inputSchema` до `tools/call`
//!   (безопасность, docs/driver-mcp-spec.md §8).

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
    transport::{StreamableHttpClientTransport, TokioChildProcess},
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

#[derive(Clone, Debug)]
pub struct McpHttpConfig {
    pub endpoint: String,
    pub token: Option<String>,
}

/// Версии MCP-протокола, которые `driver-mcp` принимает от сервера.
/// Поддержка одной версии назад от текущей стабильной (2025-03-26).
const SUPPORTED_PROTOCOL_VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];

#[derive(thiserror::Error, Debug)]
pub enum McpDriverError {
    #[error("failed to spawn MCP stdio transport: {0}")]
    Spawn(String),
    #[error("MCP initialize/connect failed: {0}")]
    Connect(String),
    #[error("MCP tools/list failed: {0}")]
    ToolsList(String),
    #[error("unsupported MCP protocol version: {0}")]
    UnsupportedProtocolVersion(String),
}

/// `ClientHandler` — тонкая обёртка над встроенным SDK `ProgressDispatcher`.
/// SDK сам маршрутизирует notification к нужному подписчику по токену.
/// Дополнительно ретранслирует `notifications/tools/list_changed` через
/// mpsc-канал: `McpClientHandler` создаётся ДО `McpDriver` (тот же порядок,
/// что для progress), поэтому обратная ссылка на driver невозможна — канал
/// решает конструирование без цикла: Sender создаётся раньше, Receiver
/// слушается background-задачей уже после появления `Arc<McpDriver>`.
#[derive(Clone)]
struct McpClientHandler {
    progress: ProgressDispatcher,
    /// capacity 1 + try_send: если сигнал уже в очереди, второй не нужен —
    /// background-задача всё равно сделает полный re-discovery.
    list_changed_tx: tokio::sync::mpsc::Sender<()>,
}

impl McpClientHandler {
    fn new(list_changed_tx: tokio::sync::mpsc::Sender<()>) -> Self {
        Self {
            progress: ProgressDispatcher::default(),
            list_changed_tx,
        }
    }
}

impl ClientHandler for McpClientHandler {
    async fn on_progress(
        &self,
        params: ProgressNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) {
        self.progress.handle_notification(params).await;
    }

    // Типизированный метод SDK (rmcp 0.8.5 handler/client.rs): диспатчится
    // автоматически из ServerNotification::ToolListChangedNotification.
    // try_send, не send().await — не блокироваться и не паниковать, если
    // сигнал уже в очереди (re-discovery всё равно покроет всё).
    async fn on_tool_list_changed(&self, _context: NotificationContext<RoleClient>) {
        if self.list_changed_tx.try_send(()).is_err() {
            tracing::debug!(
                "list_changed signal already pending, background watcher will still re-discover"
            );
        }
    }
}

type McpSession = rmcp::service::RunningService<RoleClient, McpClientHandler>;

pub struct McpDriver {
    id: String,
    /// id агента в `AgentRegistry` — нужен background-задаче list_changed
    /// для поиска `RegisteredAgent` при hot-update skills.
    agent_id: adapter_model::AgentId,
    /// Weak (не Arc) — не создавать цикл владения Registry -> Agent -> Driver
    /// -> Registry; upgrade() вернёт None после shutdown registry.
    registry: std::sync::Weak<adapter_core::AgentRegistry>,
    session: Arc<McpSession>,
    handler: McpClientHandler,
    /// Tool name -> input_schema (JSON Schema) для валидации input до вызова.
    tool_schemas: Arc<tokio::sync::RwLock<HashMap<String, serde_json::Value>>>,
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
        agent_id: adapter_model::AgentId,
        registry: std::sync::Weak<adapter_core::AgentRegistry>,
    ) -> Result<Arc<Self>, McpDriverError> {
        let mut command = Command::new(&config.command);
        command.args(&config.args).envs(&config.env);
        let transport =
            TokioChildProcess::new(command).map_err(|e| McpDriverError::Spawn(e.to_string()))?;

        let (list_changed_tx, list_changed_rx) = tokio::sync::mpsc::channel::<()>(1);
        let handler = McpClientHandler::new(list_changed_tx);
        let session = handler
            .clone()
            .serve(transport)
            .await
            .map_err(|e| McpDriverError::Connect(e.to_string()))?;

        Self::from_session(
            id,
            agent_id,
            registry,
            session,
            handler,
            list_changed_rx,
            allowed_tools,
            default_timeout,
        )
        .await
    }

    /// Streamable HTTP транспорт (MCP 2025-03-26 spec). Токен кладётся в
    /// Authorization: Bearer header; TLS-проверка — ответственность конфига
    /// (`allow_http_development` в adapterd-config, см. main.rs).
    pub async fn connect_http(
        id: impl Into<String>,
        config: McpHttpConfig,
        allowed_tools: Vec<String>,
        default_timeout: Duration,
        agent_id: adapter_model::AgentId,
        registry: std::sync::Weak<adapter_core::AgentRegistry>,
    ) -> Result<Arc<Self>, McpDriverError> {
        let client = reqwest::Client::new();
        let mut transport_config =
            rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig::with_uri(
                config.endpoint,
            );
        if let Some(token) = config.token {
            transport_config = transport_config.auth_header(format!("Bearer {token}"));
        }
        let transport = StreamableHttpClientTransport::with_client(client, transport_config);

        let (list_changed_tx, list_changed_rx) = tokio::sync::mpsc::channel::<()>(1);
        let handler = McpClientHandler::new(list_changed_tx);
        let session = handler
            .clone()
            .serve(transport)
            .await
            .map_err(|e| McpDriverError::Connect(e.to_string()))?;

        Self::from_session(
            id,
            agent_id,
            registry,
            session,
            handler,
            list_changed_rx,
            allowed_tools,
            default_timeout,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn from_session(
        id: impl Into<String>,
        agent_id: adapter_model::AgentId,
        registry: std::sync::Weak<adapter_core::AgentRegistry>,
        session: McpSession,
        handler: McpClientHandler,
        list_changed_rx: tokio::sync::mpsc::Receiver<()>,
        allowed_tools: Vec<String>,
        default_timeout: Duration,
    ) -> Result<Arc<Self>, McpDriverError> {
        let driver = Arc::new(Self {
            id: id.into(),
            agent_id,
            registry,
            session: Arc::new(session),
            handler,
            tool_schemas: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            allowed_tools,
            default_timeout,
            active_handles: Arc::new(DashMap::new()),
        });

        driver.verify_protocol_version()?;
        driver.discover_tools().await?;

        // Background-задача: реагирует на notifications/tools/list_changed.
        // Держит Weak<McpDriver> (не Arc — не мешать shutdown) + Weak<AgentRegistry>
        // (не создавать цикл). Канал решает порядок конструирования: Sender
        // передан handler'у ДО того, как эта задача начала слушать Receiver.
        driver.spawn_list_changed_watcher(list_changed_rx);

        Ok(driver)
    }

    /// Background-задача hot-update skills при tools/list_changed (ADR-0001
    /// Решение 1). При сигнале: полный re-discovery, затем обновление
    /// `RegisteredAgent.skills` через `update_skills()`. Если registry или
    /// сам агент уже сброшены — задача завершается.
    fn spawn_list_changed_watcher(self: &Arc<Self>, mut rx: tokio::sync::mpsc::Receiver<()>) {
        let driver = Arc::downgrade(self);
        let registry = self.registry.clone();
        let agent_id = self.agent_id.clone();
        tokio::spawn(async move {
            while rx.recv().await.is_some() {
                let Some(driver) = driver.upgrade() else {
                    tracing::debug!("driver dropped, stopping list_changed watcher");
                    break;
                };
                if let Err(e) = driver.discover_tools().await {
                    tracing::warn!(
                        agent_id = %agent_id.0,
                        error = %e,
                        "re-discovery after tools/list_changed failed, keeping stale skill list"
                    );
                    continue;
                }
                let Some(registry) = registry.upgrade() else {
                    tracing::debug!("registry dropped, stopping list_changed watcher");
                    break;
                };
                let Some(agent) = registry.get(&agent_id) else {
                    tracing::warn!(agent_id = %agent_id.0, "agent no longer in registry, stopping watcher");
                    break;
                };
                // В skills идут только имена tools (ключи tool_schemas);
                // сами схемы (input_schema) остаются в driver.tool_schemas —
                // не дублируются в AgentRegistry.
                let new_skills: Vec<String> =
                    driver.tool_schemas.read().await.keys().cloned().collect();
                agent.update_skills(new_skills);
                tracing::info!(agent_id = %agent_id.0, "skills hot-updated after tools/list_changed");
            }
        });
    }

    /// Проверка версии MCP-протокола сервера после initialize (см. spec §7).
    fn verify_protocol_version(&self) -> Result<(), McpDriverError> {
        let Some(info) = self.session.peer().peer_info() else {
            return Err(McpDriverError::UnsupportedProtocolVersion(
                "no server info available after initialize".into(),
            ));
        };
        let version = info.protocol_version.to_string();
        if SUPPORTED_PROTOCOL_VERSIONS.contains(&version.as_str()) {
            Ok(())
        } else {
            Err(McpDriverError::UnsupportedProtocolVersion(format!(
                "{version} (supported: {})",
                SUPPORTED_PROTOCOL_VERSIONS.join(", ")
            )))
        }
    }

    async fn discover_tools(&self) -> Result<(), McpDriverError> {
        let mut cursor: Option<PaginatedRequestParam> = None;
        let mut discovered = HashMap::new();
        loop {
            let page = self
                .session
                .list_tools(cursor.take())
                .await
                .map_err(|e: ServiceError| McpDriverError::ToolsList(e.to_string()))?;
            for tool in page.tools {
                let name = tool.name.to_string();
                if self.allowed_tools.is_empty() || self.allowed_tools.contains(&name) {
                    let schema = serde_json::to_value(&tool.input_schema)
                        .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
                    discovered.insert(name, schema);
                }
            }
            match page.next_cursor {
                Some(next) => cursor = Some(PaginatedRequestParam { cursor: Some(next) }),
                None => break,
            }
        }
        *self.tool_schemas.write().await = discovered;
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

    /// Валидация сформированных arguments против сохранённой inputSchema
    /// (spec §8): быстрый понятный `InvalidRequest` до отправки tools/call,
    /// а не непрозрачная MCP-ошибка.
    async fn validate_input(
        &self,
        skill_id: &str,
        arguments: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), CoreError> {
        let schema = self.tool_schemas.read().await.get(skill_id).cloned();
        let Some(schema) = schema else {
            return Err(CoreError::InvalidRequest(format!(
                "unknown or disallowed MCP tool: {skill_id}"
            )));
        };
        let Ok(validator) = jsonschema::validator_for(&schema) else {
            // Сервер отдал невалидную схему — не можем проверить, пропускаем.
            // Это безопаснее, чем блокировать все вызовы из-за кривого сервера.
            tracing::warn!(
                skill_id,
                "invalid inputSchema from server, skipping validation"
            );
            return Ok(());
        };
        let value = serde_json::Value::Object(arguments.clone());
        let errors: Vec<String> = validator
            .iter_errors(&value)
            .map(|error| error.to_string())
            .take(3)
            .collect();
        if !errors.is_empty() {
            return Err(CoreError::InvalidRequest(format!(
                "MCP tool {skill_id} input validation failed: {}",
                errors.join("; ")
            )));
        }
        Ok(())
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

        let arguments = self.part_to_arguments(&request.input);
        self.validate_input(&skill_id, &arguments).await?;

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
        // (0.8.5 service.rs:281).
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

            // RequestHandle доступен здесь — именно тут выполняем handle.cancel()
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
